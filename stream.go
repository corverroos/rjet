package rjet

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
	"github.com/nats-io/nats.go"
)

// NewStream returns a reflex stream capable of streaming (reading) from the provided jetstream.
// Use WithDefaultStream to automatically configure a suitable stream.
func NewStream(js nats.JetStreamContext, name string, opts ...option) (*Stream, error) {
	var o o
	for _, opt := range opts {
		opt(&o)
	}

	if o.conf != nil {
		// Configure the stream
		if o.conf.Name == "" {
			o.conf.Name = name
		} else if o.conf.Name != name {
			return nil, errors.New("mismatching stream names")
		}

		_, err := js.AddStream(o.conf, o.opts...)
		if err != nil {
			return nil, errors.Wrap(err, "add stream")
		}
	}

	return &Stream{
		name: name,
		js:   js,
	}, nil
}

// Stream represents a redis stream.
type Stream struct {
	// name of the nats jet stream.
	name string

	// js is the nats jetstream client.
	js nats.JetStreamContext
}

// Stream implements reflex.StreamFunc and returns a StreamClient that
// streams events from the redis stream after the provided cursor (redis stream ID).
// Stream is safe to call from multiple goroutines, but the returned
// StreamClient is only safe for a single goroutine to use.
//
// Note that the stream sequence number after references must already exist.
// If it hasn't been created yet, it is equivalent to StreamFromHead,
func (s *Stream) Stream(ctx context.Context, after string,
	opts ...reflex.StreamOption) (reflex.StreamClient, error) {

	sopts := new(reflex.StreamOptions)
	for _, opt := range opts {
		opt(sopts)
	}

	subopts := []nats.SubOpt{
		nats.BindStream(s.name),
		nats.ReplayInstant(),
	}

	if sopts.Lag > 0 {
		return nil, errors.New("lag option not supported")
	} else if sopts.StreamFromHead {
		subopts = append(subopts, nats.DeliverNew())
	} else if after == "" {
		subopts = append(subopts, nats.DeliverAll())
	} else if after != "" {
		seq, err := strconv.ParseUint(after, 10, 64)
		if err != nil {
			return nil, errors.Wrap(err, "failed parsing redis stream entry id: "+after)
		}
		subopts = append(subopts, nats.StartSequence(seq+1))
	}

	// Since reflex manages its own cursors, we create an ephemeral consumers
	sub, err := s.js.SubscribeSync("", subopts...)
	if err != nil {
		return nil, err
	}

	return streamclient{
		ctx:    ctx,
		toHead: sopts.StreamToHead,
		sub:    sub,
	}, nil
}

type streamclient struct {
	ctx    context.Context
	sub    *nats.Subscription
	toHead bool
}

func (s streamclient) Recv() (*reflex.Event, error) {
	msg, err := s.sub.NextMsgWithContext(s.ctx)
	if err != nil {
		return nil, errors.Wrap(err, "jetstream next")
	}

	return parseEvent(msg)
}

// parseEvent parses a reflex event from a jetstream msg.
// Get metadata from reply subject: $JS.ACK.<stream>.<consumer>.<delivered count>.<stream sequence>.<consumer sequence>.<timestamp>.<pending messages>
// See for reference: https://docs.nats.io/jetstream/nats_api_reference#acknowledging-messages
// Also see: https://github.com/jgaskins/nats/blob/3ebac6a59ab1d0482c1ef5c42ee3c7e4f7fa7d58/src/jetstream.cr#L187-L206
func parseEvent(msg *nats.Msg) (*reflex.Event, error) {
	split := strings.Split(msg.Reply, ".")
	if len(split) != 9 {
		return nil, errors.New("failed parsing msg metadata")
	}

	stream := split[2]
	seq := split[5]
	timestamp := split[7]

	_, err := strconv.ParseInt(seq, 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "failed parsing msg seq")
	}

	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "failed parsing msg timestamp")
	}

	return &reflex.Event{
		ID:        seq,
		ForeignID: stream,
		Timestamp: time.Unix(0, ts),
		MetaData:  msg.Data,
	}, nil
}
