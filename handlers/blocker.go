package handlers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/coinbase/memcachedbetween/pool"
	"github.com/d2army/redisbetween/messenger"
	"github.com/d2army/redisbetween/redis"
	"go.uber.org/zap"
)

// This currently only implements support for BRPOPLPUSH

type blockingCommand struct {
	local   *connection
	command []*redis.Message
	source  string
	timeout time.Duration
	start   time.Time
}

type blocker struct {
	sync.Mutex
	sync.Once

	ctx       context.Context
	upstream  pool.ConnectionWrapper
	messenger messenger.Messenger
	queue     []*blockingCommand
	parent    *Reservations

	quit     chan bool        // local close
	shutdown chan interface{} // global shutdown
	kill     chan interface{} // global kill

	source       string
	log          *zap.Logger
	readTimeout  time.Duration
	writeTimeout time.Duration

	working bool
	closed  bool
}

var errUpstreamDisconnected = errors.New("upstream disconnected")

func handleBlocker(c *connection, wm []*redis.Message) error {
	c.reservations.Lock()
	defer c.reservations.Unlock()

	if c.reservations.closed {
		return fmt.Errorf("reservations are closed for blocking")
	}

	bc, err := parseCommand(c, wm)
	if err != nil {
		c.log.Warn("Cannot parse blocking command", zap.Error(err))

		return err
	}

	log := c.log.With(zap.String("source", bc.source))

	blocker, err := findBlocker(c.reservations, bc.source)
	if err != nil {
		log.Warn("Cannot find blocker", zap.Error(err))

		return err
	}

	if blocker == nil {
		blocker, err = newBlocker(c, bc.source, log)
		if err != nil {
			return err
		}
	}

	blocker.Lock()
	defer blocker.Unlock()

	if err := blocker.enqueue(c, bc); err != nil {
		log.Warn("Could not enqueue blocking command", zap.Error(err))
		if blocker.isEmpty() {
			blocker.close()
		}
		return err
	}

	return nil
}

func findBlocker(reservations *Reservations, source string) (*blocker, error) {
	if r := reservations.get("b", source); r != nil {
		blocker, ok := r.(*blocker)
		if !ok {
			return nil, fmt.Errorf("Reservation is not a blocker type")
		}

		return blocker, nil
	}

	return nil, nil
}

func newBlocker(c *connection, source string, log *zap.Logger) (*blocker, error) {
	var err error

	blocker := &blocker{
		ctx:          c.ctx,
		quit:         make(chan bool, 1),
		kill:         c.kill,
		shutdown:     c.quit,
		readTimeout:  c.readTimeout,
		writeTimeout: c.writeTimeout,
		source:       source,
		parent:       c.reservations,
		messenger:    c.messenger,
	}

	if err := blocker.parent.checkMax("b"); err != nil {
		return nil, err
	}

	if blocker.upstream, err = c.checkoutConnection(); err != nil {
		return nil, err
	}

	blocker.log = log.With(
		zap.Uint64("upstream_id", blocker.upstream.ID()),
	)

	blocker.log.Debug("Connection checked out for blocker")

	if err = blocker.parent.add("b", source, blocker); err != nil {
		blocker.close()

		return nil, err
	}

	c.reservations.monitor.incrementUpstreamBlockers(
		"reservation_event.upstream_block",
		[]string{fmt.Sprintf("source:%s", blocker.source)})

	return blocker, nil
}

func (b *blocker) enqueue(c *connection, bc *blockingCommand) error {
	// This will block enqueuing of new commands in the event that the upstream
	// is no longer alive and will facilitate draining of the active queue (all
	// of which should error out).
	if !b.upstream.Alive() {
		return fmt.Errorf("blocker upstream is disconnected")
	}

	if b.closed {
		return fmt.Errorf("blocker reservation is closed")
	}

	b.log.Debug("Enqueuing blocking command")
	b.queue = append(b.queue, bc)

	if !b.working {
		b.working = true
		go b.dequeue()
	}

	c.reservations.monitor.incrementLocalBlockers(
		"reservation_event.local_block",
		[]string{fmt.Sprintf("source:%s", bc.source)})

	return nil
}

func (b *blocker) dequeue() {
	b.log.Debug("Starting blocking dequeuer")

	for loop := true; loop; {
		select {
		case <-b.shutdown:
			b.log.Debug("received shutdown signal")
			loop = false
		case <-b.quit:
			b.log.Debug("received local quit signal")
			loop = false
		case <-b.kill:
			b.log.Debug("received kill signal")
			loop = false
		default:
		}

		if !loop {
			break
		}

		cmd := b.peek()

		if cmd.local.Closed() {
			b.log.Debug("Local connection was closed")
		} else if cmd.adjust() {
			b.log.Debug("blocking command timeout exceeded")
			if err := b.localWrite(cmd.local, []*redis.Message{redis.NewArray(nil)}); err != nil {
				b.log.Error("local write error", zap.Error(err))
			}
		} else if err := b.roundTrip(cmd); err != nil {
			if errors.Is(err, errUpstreamDisconnected) {
				// When we see the upstream disconnected we want to break out of the loop and close the blocker +
				// all local connections to simulate what the client would see if they were talking directly
				// to the upstream.
				b.log.Error("Upstream disconnected", zap.Error(err))
				break
			} else {
				b.log.Warn("Could not roundtrip blocking command", zap.Error(err))
			}
		}

		if done := b.pop(); done {
			break
		}
	}

	b.log.Debug("Ending blocking dequeuer")
	b.parent.Lock()
	defer b.parent.Unlock()

	b.Lock()
	defer b.Unlock()
	b.close()
}

// adjust will check the timeout of the blocking command and adjust any
// non-zero timeout according to the time already spent in the queue. returns
// true if the adjustment would bring the timeout below or equal to zero.
func (bc *blockingCommand) adjust() bool {
	if bc.timeout == 0 {
		return false
	}

	elapsed := time.Since(bc.start)
	adjusted := bc.timeout - elapsed

	if adjusted.Seconds() < 0.1 {
		return true
	}

	bc.timeout = adjusted
	bc.command[0].Array[3].Value = []byte(fmt.Sprintf("%f", adjusted.Seconds()))

	return false
}

func (b *blocker) isEmpty() bool {
	return len(b.queue) == 0
}

func (b *blocker) peek() (cmd *blockingCommand) {
	b.Lock()
	defer b.Unlock()

	return b.queue[0]
}

func (b *blocker) pop() bool {
	b.parent.Lock()
	defer b.parent.Unlock()

	b.Lock()
	defer b.Unlock()

	// empty out the first value in the slice prior to popping in order to
	// ensure GC
	b.queue[0] = nil
	b.queue = b.queue[1:]

	b.parent.monitor.decrementLocalBlockers(
		"reservation_event.local_unblock",
		[]string{fmt.Sprintf("source:%s", b.source)})

	if b.closed || b.isEmpty() {
		return true
	}

	return false
}

func (b *blocker) close() {
	// Only close once, otherwise the closing b.quit won't panic.
	b.Do(func() {
		b.closed = true

		close(b.quit)
		if err := b.upstream.Return(); err != nil {
			b.log.Warn("Could not upstream connection to pool (from Blocker)", zap.Error(err))
			return
		}

		b.log.Debug("Connection returned to pool (from Blocker)")

		if err := b.parent.delete("b", b.source); err != nil {
			b.log.Warn("Could not remove blocker from reservation list", zap.Error(err))
		}

		for i := range b.queue {
			local := b.queue[i].local
			if err := local.Close(); err != nil {
				b.log.Warn("Could not close local connection", zap.Error(err))
			}

			b.parent.monitor.decrementLocalBlockers(
				"reservation_event.local_unblock",
				[]string{fmt.Sprintf("source:%s", b.source)})
		}

		b.parent.monitor.decrementUpstreamBlockers(
			"reservation_event.upstream_unblock",
			[]string{fmt.Sprintf("source:%s", b.source)})
	})
}

func (b *blocker) roundTrip(cmd *blockingCommand) error {
	if err := b.upstreamWrite(cmd.command); err != nil {
		return fmt.Errorf("upstream write: %s", err)
	}

	res, err := b.upstreamRead(b.readTimeout)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return errUpstreamDisconnected
		}

		return fmt.Errorf("upstream read: %w", err)
	}

	if isClusterResponse(res) {
		err = fmt.Errorf("blocking commands aren't supported for Redis cluster")
		mm := []*redis.Message{redis.NewError([]byte(fmt.Sprintf("redisbetween: %v", err.Error())))}
		if err := b.localWrite(cmd.local, mm); err != nil {
			b.log.Error("local write error", zap.Error(err))
		}

		return err
	}

	if cmd.local.Closed() {
		return fmt.Errorf("local connection was closed")
	}

	if err := b.localWrite(cmd.local, res); err != nil {
		return fmt.Errorf("local write: %w", err)
	}

	return nil
}

// Convenience wrapper for WriteWireMessage for reservation upstreams
func (b *blocker) upstreamWrite(wm []*redis.Message) error {
	err := b.messenger.Write(
		b.ctx, b.log, wm, b.upstream.Conn(), b.upstream.Address().String(),
		b.upstream.ID(), b.writeTimeout, false, b.upstream.Close,
	)

	return err
}

// Convenience wrapper for ReadWireMessage for reservation upstreams
func (b *blocker) upstreamRead(timeout time.Duration) ([]*redis.Message, error) {
	msg, err := b.messenger.Read(
		b.ctx, b.log, b.upstream.Conn(), b.upstream.Address().String(),
		b.upstream.ID(), b.readTimeout, 1, false, b.upstream.Close,
	)

	return msg, err
}

func (b *blocker) localWrite(c *connection, wm []*redis.Message) error {
	return c.Write(wm)
}

func parseCommand(c *connection, wm []*redis.Message) (*blockingCommand, error) {
	var parts [][]byte

	for _, part := range wm[0].Array {
		parts = append(parts, part.Value)
	}

	if len(parts) < 4 {
		return nil, fmt.Errorf("invalid command: %v", wm)
	}

	timeout, err := time.ParseDuration(fmt.Sprintf("%ss", parts[3]))
	if err != nil {
		return nil, err
	}

	cmd := &blockingCommand{
		local:   c,
		command: wm,
		source:  string(parts[1]),
		timeout: timeout,
		start:   time.Now(),
	}

	return cmd, nil
}

// Checks if command is a) singular and b) is a *blocking command
func isBlockingCommand(commands []string) bool {
	if len(commands) == 1 && strings.Contains(commands[0], "BRPOPLPUSH") {
		return true
	}

	return false
}

func isClusterResponse(res []*redis.Message) bool {
	for _, m := range res {
		if m.IsError() {
			msg := string(m.Value)

			if strings.HasPrefix(msg, "CROSSSLOT") || strings.HasPrefix(msg, "MOVED") || strings.HasPrefix(msg, "ASK") {
				return true
			}
		}
	}

	return false
}
