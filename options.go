package rjet

import (
	"time"

	"github.com/nats-io/nats.go"
)

type o struct {
	conf *nats.StreamConfig
	opts []nats.JSOpt
	subj string
}

type option func(*o)

// WithDefaultStream returns an option to configure a default rjet stream for the provided subjects.
// See details for recommended config if defining the steam yourself.
func WithDefaultStream(subjects ...string) option {
	return func(o *o) {
		o.conf = &nats.StreamConfig{
			Description: "Reflex managed stream",
			Subjects:    subjects,
			Retention:   nats.LimitsPolicy,
			MaxAge:      time.Hour * 24 * 30 * 6, // 6 months
			Discard:     nats.DiscardOld,
			Storage:     nats.FileStorage,
			Duplicates:  time.Minute * 5,
		}
	}
}

// WithStream returns an option to configure the provided stream.
func WithStream(conf *nats.StreamConfig, opts ...nats.JSOpt) option {
	return func(o *o) {
		o.conf = conf
		o.opts = append(o.opts, opts...)
	}
}

// WithSubjectFilter returns an option to specify a subject filter when consuming from a stream.
// If not provided, events for all subjects of the stream will be consumed.
func WithSubjectFilter(subj string) option {
	return func(o *o) {
		o.subj = subj
	}
}
