package handlers

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/coinbase/memcachedbetween/pool"
	"github.com/d2army/redisbetween/messenger"
	"github.com/d2army/redisbetween/redis"
	"go.uber.org/zap"
)

type subscription struct {
	sync.Mutex
	sync.Once

	ctx       context.Context
	upstream  pool.ConnectionWrapper
	messenger messenger.Messenger
	locals    map[uint64]*connection
	parent    *Reservations

	quit     chan bool        // local close
	shutdown chan interface{} // global shutdown
	kill     chan interface{} // global kill

	channels     string
	init         []*redis.Message
	log          *zap.Logger
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func handleSubscription(c *connection, wm []*redis.Message) error {
	var err error

	c.reservations.Lock()
	defer c.reservations.Unlock()

	if c.reservations.closed {
		c.log.Warn("reservations are closed for subscriptions")
		return nil
	}

	cmd := strings.ToUpper(string(wm[0].Array[0].Value))
	channels := parseChannels(wm[0])

	log := c.log.With(zap.String("channels", channels))

	switch cmd {
	case "SUBSCRIBE", "PSUBSCRIBE":
		if len(strings.Split(channels, " ")) > 1 {
			err = fmt.Errorf("cannot subscribe to multiple channels")
			log.Warn("Invalid subscription", zap.Error(err))
			return err
		}

		sub, err := findSubscription(c.reservations, channels)
		if err != nil {
			log.Warn("Cannot find subscription", zap.Error(err))
			return err
		}

		if sub == nil {
			sub, err = newSubscription(c, channels, log)
			if err != nil {
				log.Warn("Cannot create subscription", zap.Error(err))
				return err
			}
		}

		err = sub.subscribe(c, wm)
		if err != nil {
			log.Error("Cannot subscribe", zap.Error(err))
			return err
		}
	case "UNSUBSCRIBE", "PUNSUBSCRIBE":
		sub, err := findSubscription(c.reservations, channels)
		if err != nil {
			return err
		}

		if sub == nil {
			err = fmt.Errorf("existing subscription not found")
			log.Warn("Cannot unsubscribe", zap.Error(err))
			return err
		}

		err = sub.unsubscribe(c, wm)
		if err != nil {
			log.Error("Cannot unsubscribe", zap.Error(err))
			return err
		}
	default:
		return fmt.Errorf("cannot process '%s' as a sub or unsub command", cmd)
	}

	return err
}

func findSubscription(reservations *Reservations, channels string) (*subscription, error) {
	if r := reservations.get("c", channels); r != nil {
		sub, ok := r.(*subscription)
		if !ok {
			return nil, fmt.Errorf("Reservation is not a subscription type")
		}

		return sub, nil
	}

	return nil, nil
}

func newSubscription(c *connection, channels string, log *zap.Logger) (*subscription, error) {
	var err error

	sub := &subscription{
		ctx:          c.ctx,
		quit:         make(chan bool, 1),
		kill:         c.kill,
		shutdown:     c.quit,
		locals:       make(map[uint64]*connection),
		messenger:    c.messenger,
		readTimeout:  c.readTimeout,
		writeTimeout: c.writeTimeout,
		channels:     channels,
		parent:       c.reservations,
	}

	if err := sub.parent.checkMax("c"); err != nil {
		return nil, err
	}

	if sub.upstream, err = c.checkoutConnection(); err != nil {
		return nil, err
	}

	sub.log = log.With(
		zap.Uint64("upstream_id", sub.upstream.ID()),
	)

	sub.log.Debug("Connection checked out for subscription")
	c.reservations.monitor.incrementUpstreamSubscriptions(
		"reservation_event.upstream_subscribe",
		[]string{fmt.Sprintf("channel:%s", sub.channels)})

	if err := sub.parent.add("c", channels, sub); err != nil {
		return nil, err
	}

	return sub, nil
}

func (s *subscription) close() {
	s.Do(func() {
		close(s.quit)
		if err := s.upstream.Return(); err != nil {
			s.log.Warn("Could not upstream connection to pool (from Subscription)", zap.Error(err))
			return
		}

		s.log.Debug("Connection returned to pool (from Subscription)")

		if err := s.parent.delete("c", s.channels); err != nil {
			s.log.Warn("Could not remove subscription from reservation list", zap.Error(err))
		}

		s.parent.monitor.decrementUpstreamSubscriptions(
			"reservation_event.upstream_unsubscribe",
			[]string{fmt.Sprintf("channel:%s", s.channels)})

		for _, local := range s.locals {
			if err := local.Close(); err != nil {
				s.log.Warn("Could not close local connection", zap.Error(err))
			}

			s.parent.monitor.decrementLocalSubscriptions(
				"reservation_event.local_unsubscribe",
				[]string{fmt.Sprintf("channel:%s", s.channels)})
		}
	})
}

func (s *subscription) subscribe(c *connection, wm []*redis.Message) error {
	var err error

	// add the local connection to the list first to avoid a race condition
	// on broadcasting published messages to locals as soon as we send the
	// subscribe command
	s.Lock()
	defer s.Unlock()
	s.locals[c.id] = c
	s.log.Debug("Added client connection to subscription", zap.Uint64("client_id", c.id))

	// If this is the first local connection added to the subscription,
	// we need to initialize the subscription by:
	//     a) actually sending the subscribe command to the upstream and
	//     b) storing the response in `init` to subsequent local subscribers and
	//     c) starting to broadcast publish messages to subscribed connections
	if len(s.init) == 0 {
		if err = s.upstreamWrite(wm); err != nil {
			return err
		}

		s.init, err = s.upstreamRead(s.readTimeout)
		if err != nil {
			return err
		}

		// As soon as the subscription is created, begin broadcasting
		go s.broadcast()
	}

	if err = s.localWrite(c, s.init); err != nil {
		return err
	}

	c.reservations.monitor.incrementLocalSubscriptions(
		"reservation_event.local_subscribe",
		[]string{fmt.Sprintf("channel:%s", s.channels)})

	return nil
}

func (s *subscription) broadcast() {
	for loop := true; loop; {
		select {
		case <-s.quit:
			s.log.Debug("received local quit signal")
			loop = false
		case <-s.kill:
			s.log.Debug("received kill signal")
			loop = false
		case <-s.shutdown:
			s.log.Debug("recieved shutdown signal")
			loop = false
		default:
		}

		if !loop {
			break
		}

		wm, err := s.upstreamRead(1 * time.Second)
		if err != nil {
			if err, ok := err.(net.Error); ok && err.Timeout() {
				// We expect to timeout every second if there is nothing published on
				// the channel(s)
				continue
			} else {
				s.log.Error("Could not read upstream", zap.Error(err))
				break
			}
		}

		if len(wm) > 0 {
			s.Lock()
			s.log.Debug(
				"Writing message to subscribers",
				zap.String("message", wm[0].String()),
				zap.Int("count", len(s.locals)),
			)

			if len(s.locals) > 0 {
				for id, local := range s.locals {
					if err = s.localWrite(local, wm); err != nil {
						s.log.Error(
							"Failed to broadcast published message to subscriber",
							zap.Uint64("client_id", id),
							zap.String("message", wm[0].String()),
							zap.Error(err),
						)
						continue
					}
				}
			}
			s.Unlock()
		}
	}
	s.log.Debug("Ending subscription broadcast")
	s.close()
}

func (s *subscription) unsubscribe(c *connection, wm []*redis.Message) error {
	s.Lock()
	defer s.Unlock()

	if err := s.localWrite(c, unsubscribeMessage(s.channels)); err != nil {
		return err
	}

	delete(s.locals, c.id)
	s.log.Debug("Removed local connection from subscription", zap.Uint64("client_id", c.id))
	c.reservations.monitor.decrementLocalSubscriptions(
		"reservation_event.local_unsubscribe",
		[]string{fmt.Sprintf("channel:%s", s.channels)})

	if len(s.locals) == 0 {
		if err := s.upstreamWrite(wm); err != nil {
			s.log.Error("Cannot unsubscribe upstream for empty subscription", zap.Error(err))
		}

		s.close()
	}

	return nil
}

// Convenience wrapper for WriteWireMessage for reservation upstreams
func (s *subscription) upstreamWrite(wm []*redis.Message) error {
	err := s.messenger.Write(
		s.ctx, s.log, wm, s.upstream.Conn(), s.upstream.Address().String(),
		s.upstream.ID(), s.writeTimeout, false, s.upstream.Close,
	)

	return err
}

// Convenience wrapper for ReadWireMessage for reservation upstreams
func (s *subscription) upstreamRead(timeout time.Duration) ([]*redis.Message, error) {
	msg, err := s.messenger.Read(
		s.ctx, s.log, s.upstream.Conn(), s.upstream.Address().String(),
		s.upstream.ID(), timeout, 1, false, s.upstream.Close,
	)

	return msg, err
}

func (s *subscription) localWrite(c *connection, wm []*redis.Message) error {
	return c.Write(wm)
}

func parseChannels(msg *redis.Message) string {
	parts := msg.Array[1:]

	var channels []string

	for _, part := range parts {
		val := strings.Split(part.String(), "\\r\\n")
		channels = append(channels, strings.TrimSpace(val[len(val)-2]))
	}

	return strings.Join(channels, " ")
}

func unsubscribeMessage(channels string) []*redis.Message {
	var msgs []*redis.Message
	msgs = append(msgs, &redis.Message{
		Type:  redis.TypeBulkBytes,
		Value: []byte("unsubscribe"),
	})

	msgs = append(msgs, &redis.Message{
		Type:  redis.TypeBulkBytes,
		Value: []byte(channels),
	})

	msgs = append(msgs, &redis.Message{
		Type:  redis.TypeInt,
		Value: []byte("1"),
	})

	return msgs
}

// Checks if command is a) singular and b) is a *subscribe command
func isSubscriptionCommand(commands []string) bool {
	if len(commands) == 1 && strings.Contains(commands[0], "SUBSCRIBE") {
		return true
	}

	return false
}
