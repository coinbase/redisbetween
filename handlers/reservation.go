package handlers

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/coinbase/mongobetween/util"
	"github.com/d2army/redisbetween/redis"
)

type reservation interface {
	close()
	upstreamWrite(wm []*redis.Message) error
	upstreamRead(timeout time.Duration) ([]*redis.Message, error)
	localWrite(c *connection, wm []*redis.Message) error
}

type reservationsMonitor struct {
	incrementUpstreamSubscriptions util.StatsdBackgroundGaugeCallback
	decrementUpstreamSubscriptions util.StatsdBackgroundGaugeCallback
	incrementLocalSubscriptions    util.StatsdBackgroundGaugeCallback
	decrementLocalSubscriptions    util.StatsdBackgroundGaugeCallback
	incrementUpstreamBlockers      util.StatsdBackgroundGaugeCallback
	decrementUpstreamBlockers      util.StatsdBackgroundGaugeCallback
	incrementLocalBlockers         util.StatsdBackgroundGaugeCallback
	decrementLocalBlockers         util.StatsdBackgroundGaugeCallback
}

type Reservations struct {
	sync.Mutex
	list    map[string]reservation
	maxSub  int
	maxBlk  int
	monitor *reservationsMonitor
	closed  bool
}

type reservationError struct {
	key string
}

func (e reservationError) Error() string {
	return fmt.Sprintf("reservation with key '%s' already exists", e.key)
}

func NewReservations(maxSub, maxBlk int, sd *statsd.Client) *Reservations {
	return &Reservations{
		list:    make(map[string]reservation),
		maxSub:  maxSub,
		maxBlk:  maxBlk,
		monitor: newReservationsMonitor(sd),
	}
}

func newReservationsMonitor(sd *statsd.Client) *reservationsMonitor {
	incrementUpstreamSubs, decrementUpstreamSubs := util.StatsdBackgroundGauge(sd, "reservations.upstream_subscriptions", []string{})
	incrementLocalSubs, decrementLocalSubs := util.StatsdBackgroundGauge(sd, "reservations.local_subscriptions", []string{})
	incrementUpstreamBlockers, decrementUpstreamBlockers := util.StatsdBackgroundGauge(sd, "reservations.upstream_blockers", []string{})
	incrementLocalBlockers, decrementLocalBlockers := util.StatsdBackgroundGauge(sd, "reservations.local_blockers", []string{})

	return &reservationsMonitor{
		incrementUpstreamSubscriptions: incrementUpstreamSubs,
		decrementUpstreamSubscriptions: decrementUpstreamSubs,
		incrementLocalSubscriptions:    incrementLocalSubs,
		decrementLocalSubscriptions:    decrementLocalSubs,
		incrementUpstreamBlockers:      incrementUpstreamBlockers,
		decrementUpstreamBlockers:      decrementUpstreamBlockers,
		incrementLocalBlockers:         incrementLocalBlockers,
		decrementLocalBlockers:         decrementLocalBlockers,
	}
}

func (r *Reservations) add(prefix string, source string, res reservation) error {
	key := fmt.Sprintf("%s:%s", prefix, source)

	if r.closed {
		return fmt.Errorf("reservations are closed: %s", key)
	}

	_, ok := r.list[key]
	if ok {
		return reservationError{key: key}
	}

	r.list[key] = res
	return nil
}

func (r *Reservations) get(prefix string, source string) reservation {
	if r.closed {
		return nil
	}

	key := fmt.Sprintf("%s:%s", prefix, source)

	return r.list[key]
}

func (r *Reservations) delete(prefix string, source string) error {
	key := fmt.Sprintf("%s:%s", prefix, source)

	_, ok := r.list[key]
	if !ok {
		return fmt.Errorf("reservation with key '%s' does not exist", key)
	}

	delete(r.list, key)
	return nil
}

func (r *Reservations) checkMax(prefix string) error {
	var count int

	for key := range r.list {
		if strings.HasPrefix(key, prefix) {
			count++
		}
	}

	switch prefix {
	case "c":
		if count >= r.maxSub {
			return fmt.Errorf("new reservation would exceed configured max subscriptions (%d)", r.maxSub)
		}
	case "b":
		if count >= r.maxBlk {
			return fmt.Errorf("new reservation would exceed configured max blockers (%d)", r.maxBlk)
		}
	}

	return nil
}

func (r *Reservations) Close() {
	r.Lock()
	defer r.Unlock()

	r.closed = true
}
