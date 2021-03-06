package rjet

import (
	"context"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/reflex"
	"github.com/nats-io/nats.go"
)

// TypeHeader is the nats header that defines the reflex event type.
// See SetTypeHeader for usage.
//
// Note if the type defaults to 0 if the header isn't present.
const TypeHeader = "reflex_type"

// SetTypeHeader sets the event type header.
func SetTypeHeader(h nats.Header, typ reflex.EventType) {
	h.Set(TypeHeader, strconv.Itoa(typ.ReflexType()))
}

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
		o:    o,
	}, nil
}

// Stream represents a redis stream.
type Stream struct {
	// name of the nats jet stream.
	name string

	// js is the nats jetstream client.
	js nats.JetStreamContext

	// o is the stream options.
	o o
}

// Stream implements reflex.StreamFunc and returns a StreamClient that
// streams events from the nats stream after the provided sequence.
// Stream is safe to call from multiple goroutines, but the returned
// StreamClient is only safe for a single goroutine to use.
//
// Note that the stream sequence number after references must already exist.
// If it hasn't been created yet, it is equivalent to StreamFromHead,
//
// Note this is a slow operation (tens of milliseconds) since it creates an ephemeral consumer
// of the nats stream.
//
// The returns reflex.StreamClient implementation is also an io.Closer. It is advised
// to close the stream after use, since it seems to improve performance.
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
	}

	var (
		head   uint64
		toHead bool
	)
	if sopts.StreamToHead {
		if s.o.subj != "" {
			return nil, errors.New("stream to head with subject filter not supported")
		}

		info, err := s.js.StreamInfo(s.name)
		if err != nil {
			return nil, errors.Wrap(err, "stream info")
		}
		head = info.State.LastSeq
		toHead = true
	}

	var startSeq uint64

	if sopts.StreamFromHead {
		subopts = append(subopts, nats.DeliverNew())
	} else if after == "" {
		subopts = append(subopts, nats.DeliverAll())
	} else if after != "" {
		var err error
		startSeq, err = strconv.ParseUint(after, 10, 64)
		if err != nil {
			return nil, errors.Wrap(err, "failed parsing redis stream entry id: "+after)
		}
		subopts = append(subopts, nats.StartSequence(startSeq+1))
	}

	// Since reflex manages its own cursors, we create an ephemeral consumers
	sub, err := s.js.SubscribeSync(s.o.subj, subopts...)
	if err != nil {
		return nil, err
	}

	return &streamclient{
		ctx:      ctx,
		sub:      sub,
		o:        s.o,
		filtered: s.o.subj != "",
		toHead:   toHead,
		head:     head,
		lastSSeq: startSeq,
	}, nil
}

// MaybeClose closes the stream client if it implements io.Closer, like *streamclient does.
func MaybeClose(cl reflex.StreamClient) error {
	closer, ok := cl.(io.Closer)
	if !ok {
		return nil
	}

	return closer.Close()
}

type streamclient struct {
	ctx context.Context
	sub *nats.Subscription
	o   o

	filtered bool
	toHead   bool
	head     uint64

	lastSSeq uint64
	lastCSeq uint64
}

func (s *streamclient) Close() error {
	return s.sub.Unsubscribe()
}

func (s *streamclient) Recv() (*reflex.Event, error) {

	if s.toHead && s.lastSSeq >= s.head {
		return nil, reflex.ErrHeadReached
	}

start:
	msg, err := s.sub.NextMsgWithContext(s.ctx)
	if errors.Is(err, nats.ErrSlowConsumer) {
		return nil, errors.Wrap(ErrDroppedMsg, err.Error())
	} else if err != nil {
		return nil, errors.Wrap(err, "jetstream next")
	}

	e, sseq, cseq, err := parseEvent(msg, s.o.foreignIDParser)
	if err != nil {
		return nil, err
	}

	if s.toHead && sseq > s.head {
		return nil, reflex.ErrHeadReached
	}

	if s.lastCSeq == 0 { // First message
		if !s.filtered && s.lastSSeq != 0 && s.lastSSeq+1 != sseq {
			return nil, errors.Wrap(ErrDroppedMsg, "unexpected start stream seq", j.MKV{"got": sseq, "last": s.lastSSeq})
		}
	} else if s.lastCSeq+1 < cseq {
		return nil, errors.Wrap(ErrDroppedMsg, "unexpected consumer seq", j.MKV{"got": cseq, "last": s.lastCSeq})
	} else if s.lastCSeq+1 > cseq {
		// Duplicate message... lets swallow it
		goto start
	}

	s.lastSSeq = sseq
	s.lastCSeq = cseq

	// TODO(corver): Is this still required given the above checks?
	if d, err := s.sub.Dropped(); err != nil {
		return nil, err
	} else if d > 0 {
		return nil, errors.Wrap(ErrDroppedMsg, "subscription known dropped")
	}

	return e, nil
}

// parseEvent parses a reflex event from a jetstream msg and also returns the stream and consumer sequences.
// Get metadata from reply subject: $JS.ACK.<stream>.<consumer>.<delivered count>.<stream sequence>.<consumer sequence>.<timestamp>.<pending messages>
// See for reference: https://docs.nats.io/jetstream/nats_api_reference#acknowledging-messages
// Also see: https://github.com/jgaskins/nats/blob/3ebac6a59ab1d0482c1ef5c42ee3c7e4f7fa7d58/src/jetstream.cr#L187-L206
func parseEvent(msg *nats.Msg, fParser func(*nats.Msg) string) (*reflex.Event, uint64, uint64, error) {
	split := strings.Split(msg.Reply, ".")
	if len(split) != 9 {
		return nil, 0, 0, errors.New("failed parsing msg metadata")
	}

	sseq := split[5]
	cseq := split[6]
	timestamp := split[7]

	sid, err := strconv.ParseUint(sseq, 10, 64)
	if err != nil {
		return nil, 0, 0, errors.Wrap(err, "failed parsing msg stream seq")
	}

	cid, err := strconv.ParseUint(cseq, 10, 64)
	if err != nil {
		return nil, 0, 0, errors.Wrap(err, "failed parsing msg stream seq")
	}

	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return nil, 0, 0, errors.Wrap(err, "failed parsing msg timestamp")
	}

	var typ eventtype
	if t := msg.Header.Get(TypeHeader); t != "" {
		i, err := strconv.Atoi(t)
		if err != nil {
			return nil, 0, 0, errors.Wrap(err, "parse type header")
		}
		typ = eventtype(i)
	}

	foriegnID := msg.Subject
	if fParser != nil {
		foriegnID = fParser(msg)
	}

	return &reflex.Event{
		ID:        sseq,
		ForeignID: foriegnID,
		Type:      typ,
		Timestamp: time.Unix(0, ts),
		MetaData:  msg.Data,
	}, sid, cid, nil
}

type eventtype int

func (e eventtype) ReflexType() int {
	return int(e)
}
