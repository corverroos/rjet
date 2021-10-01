package rjet

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/nats-io/nats.go"
)

func NewCursorStore(js nats.JetStreamContext, namespace string) *CursorStore {
	return &CursorStore{
		js:        js,
		namespace: namespace,
		ensured:   make(map[string]bool),
	}
}

type CursorStore struct {
	js        nats.JetStreamContext
	namespace string

	mu      sync.Mutex
	ensured map[string]bool
}

func (c *CursorStore) name(cursor string) string {
	return fmt.Sprintf("reflex-cursors-%s-%s", c.namespace, cursor)
}

func (c *CursorStore) ensureStream(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ensured[name] {
		return nil
	}

	_, err := c.js.AddStream(&nats.StreamConfig{
		Name:         name,
		Description:  "Reflex managed cursor",
		Subjects:     []string{name},
		Retention:    nats.LimitsPolicy,
		MaxConsumers: 0,
		MaxMsgs:      10, // We only need 1, but we keep 5 to help with debugging.
		Discard:      nats.DiscardOld,
		MaxAge:       time.Hour * 60 * 24 * 365, // Keep for a year
		Storage:      nats.FileStorage,
	})
	if err != nil {
		return err
	}

	c.ensured[name] = true

	return nil
}

func (c *CursorStore) GetCursor(ctx context.Context, cursor string) (string, error) {
	name := c.name(cursor)

	if _, err := c.js.StreamInfo(name); errors.Is(err, nats.ErrStreamNotFound) {
		// No stream, so no cursor.
		return "", nil
	}

	subopts := []nats.SubOpt{
		nats.BindStream(name),
		nats.ReplayInstant(),
		nats.DeliverLast(),
	}

	// We just want the latest value, so create an ephemeral consumer
	sub, err := c.js.SubscribeSync("", subopts...)
	if err != nil {
		return "", errors.Wrap(err, "ensure")
	}

	msg, err := sub.NextMsgWithContext(ctx)
	if err != nil {
		return "", err
	}

	if err := sub.Unsubscribe(); err != nil {
		return "", err
	}

	return string(msg.Data), nil
}

func (c *CursorStore) SetCursor(_ context.Context, cursor string, value string) error {
	name := c.name(cursor)

	if err := c.ensureStream(name); err != nil {
		return errors.Wrap(err, "ensure")
	}

	_, err := c.js.PublishAsync(name, []byte(value))
	if err != nil {
		return errors.Wrap(err, "publish")
	}

	return err
}

func (c *CursorStore) Flush(_ context.Context) error {
	<-c.js.PublishAsyncComplete()

	// TODO(corver): Handle async errors...
	return nil
}
