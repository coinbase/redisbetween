package utils

import (
	"github.com/DataDog/datadog-go/statsd"
	"time"
)

func StatsdWithTags(client statsd.ClientInterface, tags []string) (statsd.ClientInterface, error) {
	if client, ok := client.(*statsd.Client); ok {
		tags = append(client.Tags, tags...)
		return statsd.CloneWithExtraOptions(client, statsd.WithTags(tags))
	}
	return client, nil
}

type StatsdBackgroundGaugeCallback func(name string, tags []string)

func StatsdBackgroundGauge(client statsd.ClientInterface, name string, tags []string) (increment, decrement StatsdBackgroundGaugeCallback) {
	inc := make(chan bool)
	increment = func(name string, tags []string) {
		_ = client.Incr(name, tags, 1)
		inc <- true
	}

	dec := make(chan bool)
	decrement = func(name string, tags []string) {
		_ = client.Incr(name, tags, 1)
		dec <- true
	}

	go func() {
		count := 0
		for {
			select {
			case <-inc:
				count++
			case <-dec:
				count--
			case <-time.After(1 * time.Second):
			}
			_ = client.Gauge(name, float64(count), tags, 1)
		}
	}()

	return
}
