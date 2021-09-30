package rjet

import (
	"time"

	"github.com/nats-io/nats.go"
)

type o struct {
	conf *nats.StreamConfig
	opts []nats.JSOpt
}

type option func(*o)

func WithDefaultStream(subjects ...string) option {
	return func(o *o) {
		o.conf = &nats.StreamConfig{
			Description: "Reflex managed jetstream",
			Subjects:    subjects,
			Retention:   nats.LimitsPolicy,
			MaxAge:      time.Hour * 24 * 30, // 30 days
			Discard:     nats.DiscardOld,
			Storage:     nats.FileStorage,
			Duplicates:  time.Minute * 5,
		}
	}
}

func WithStream(conf *nats.StreamConfig, opts ...nats.JSOpt) option {
	return func(o *o) {
		o.conf = conf
		o.opts = append(o.opts, opts...)
	}
}
