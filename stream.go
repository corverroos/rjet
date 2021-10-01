package rjet

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/reflex"
	"github.com/nats-io/nats.go"
)

var ErrDroppedMsg = errors.New("rjet: message dropped")

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
		nats.OrderedConsumer(),
	}

	if sopts.Lag > 0 {
		return nil, errors.New("lag option not supported")
	} else if sopts.StreamToHead {
		return nil, errors.New("stream to head option not supported")
	}

	var expect uint64

	if sopts.StreamFromHead {
		subopts = append(subopts, nats.DeliverNew())
	} else if after == "" {
		subopts = append(subopts, nats.DeliverAll())
	} else if after != "" {
		seq, err := strconv.ParseUint(after, 10, 64)
		if err != nil {
			return nil, errors.Wrap(err, "failed parsing redis stream entry id: "+after)
		}
		expect = seq + 1
		subopts = append(subopts, nats.StartSequence(expect))
	}

	// Since reflex manages its own cursors, we create an ephemeral consumers
	sub, err := s.js.SubscribeSync("", subopts...)
	if err != nil {
		return nil, err
	}

	return &streamclient{
		ctx: ctx,
		sub: sub,
	}, nil
}

type streamclient struct {
	ctx    context.Context
	sub    *nats.Subscription
	expect uint64
}

func (s *streamclient) Recv() (*reflex.Event, error) {
start:
	msg, err := s.sub.NextMsgWithContext(s.ctx)
	if errors.Is(err, nats.ErrSlowConsumer) {
		return nil, errors.Wrap(ErrDroppedMsg, err.Error())
	} else if err != nil {
		return nil, errors.Wrap(err, "jetstream next")
	}

	e, id, err := parseEvent(msg)
	if err != nil {
		return nil, err
	}

	if s.expect != 0 && s.expect < id {
		return nil, errors.Wrap(ErrDroppedMsg, "got unexpected seq", j.MKV{"got": id, "expect": s.expect})
	} else if s.expect != 0 && s.expect > id {
		// Duplicate message... lets swallow it
		goto start
	}

	s.expect = id + 1

	// TODO(corver): Is this still required given the above check?
	if d, err := s.sub.Dropped(); err != nil {
		return nil, err
	} else if d > 0 {
		return nil, errors.Wrap(ErrDroppedMsg, "subscription dropped", j.MKV{"got": id, "expect": s.expect})
	}

	return e, nil
}

// parseEvent parses a reflex event from a jetstream msg.
// Get metadata from reply subject: $JS.ACK.<stream>.<consumer>.<delivered count>.<stream sequence>.<consumer sequence>.<timestamp>.<pending messages>
// See for reference: https://docs.nats.io/jetstream/nats_api_reference#acknowledging-messages
// Also see: https://github.com/jgaskins/nats/blob/3ebac6a59ab1d0482c1ef5c42ee3c7e4f7fa7d58/src/jetstream.cr#L187-L206
func parseEvent(msg *nats.Msg) (*reflex.Event, uint64, error) {
	split := strings.Split(msg.Reply, ".")
	if len(split) != 9 {
		return nil, 0, errors.New("failed parsing msg metadata")
	}

	stream := split[2]
	seq := split[5]
	timestamp := split[7]

	id, err := strconv.ParseUint(seq, 10, 64)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed parsing msg seq")
	}

	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed parsing msg timestamp")
	}

	return &reflex.Event{
		ID:        seq,
		ForeignID: stream,
		Timestamp: time.Unix(0, ts),
		MetaData:  msg.Data,
	}, id, nil
}
