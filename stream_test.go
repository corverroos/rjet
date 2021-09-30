package rjet_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/corverroos/rjet"
	"github.com/luno/fate"
	"github.com/luno/jettison/jtest"
	"github.com/nats-io/nats.go"

	"github.com/luno/reflex"
	"github.com/matryer/is"
)

const (
	stream = "test-stream"
	insub1 = "input-subjet.1"
	insub2 = "input-subjet.2"
	insubs = "input-subjet.*"
)

func randStr() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func setup(t *testing.T) (context.Context, nats.JetStreamContext, *is.I) {
	rand.Seed(time.Now().UnixNano())
	is := is.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	c, err := nats.Connect(nats.DefaultURL)
	is.NoErr(err)
	t.Cleanup(func() {
		c.Close()
	})

	t0 := time.Now()
	for {
		status := c.Status()
		if status == nats.CONNECTED {
			break
		}
		if time.Since(t0) > time.Second {
			t.Fatalf("nats not connected")
		}
	}

	js, err := c.JetStream()
	is.NoErr(err)

	clean := func() {
		for _, stream := range []string{stream, "reflex-cursors-" + ns + "-" + cursor} {
			err := js.PurgeStream(stream)
			if err == nil || strings.Contains(err.Error(), "stream not found") {
			} else {
				is.NoErr(err)
			}

			err = js.DeleteStream(stream)
			if err == nil || strings.Contains(err.Error(), "stream not found") {
			} else {
				is.NoErr(err)
			}
		}
	}

	clean()
	t.Cleanup(clean)

	return ctx, js, is
}

func TestBasicStream(t *testing.T) {
	ctx, js, ii := setup(t)
	s, err := rjet.NewStream(js, stream, rjet.WithDefaultStream(insubs))
	jtest.RequireNil(t, err)

	insertRand := func(ii *is.I, from, to int) {
		ii.Helper()

		for i := from; i <= to; i++ {
			sub := insub1
			if rand.Float64() < 0.5 {
				sub = insub2
			}
			_, err := js.Publish(sub, []byte(fmt.Sprint(i)))
			jtest.RequireNil(t, err)
		}
	}

	assertRand := func(ii *is.I, sc reflex.StreamClient, from, to int) {
		ii.Helper()

		for i := from; i <= to; i++ {
			e, err := sc.Recv()
			jtest.RequireNil(t, err)
			ii.Equal(string(e.MetaData), fmt.Sprint(i))
		}
	}

	log.SetFlags(log.Lmicroseconds)

	// Start async consumer that first blocks
	var wg sync.WaitGroup
	sc0, err := s.Stream(ctx, "")
	jtest.RequireNil(t, err)
	wg.Add(1)
	go func() {
		assertRand(ii, sc0, 1, 8)
		wg.Done()
	}()

	sc1, err := s.Stream(ctx, "")
	jtest.RequireNil(t, err)

	insertRand(ii, 1, 5)
	assertRand(ii, sc1, 1, 5)
	insertRand(ii, 6, 6)
	insertRand(ii, 7, 8)
	assertRand(ii, sc1, 6, 7)
	assertRand(ii, sc1, 8, 8)

	sc2, err := s.Stream(ctx, "")
	jtest.RequireNil(t, err)
	assertRand(ii, sc2, 1, 8)

	sc3, err := s.Stream(ctx, "3")
	jtest.RequireNil(t, err)
	assertRand(ii, sc3, 4, 8)

	wg.Wait()
}

func TestRobust(t *testing.T) {
	ctx, js, ii := setup(t)
	store := rjet.NewCursorStore(js, ns)

	s, err := rjet.NewStream(js, stream, rjet.WithDefaultStream(insubs))
	jtest.RequireNil(t, err)

	runOnce := func(ii *is.I, from, to int) {
		//ii.Helper()

		expect := from
		consumer := reflex.NewConsumer(cursor, func(ctx context.Context, f fate.Fate, e *reflex.Event) error {
			fmt.Printf("JCR: got=%+v\n", e)
			ii.Equal(string(e.MetaData), fmt.Sprint(expect))
			expect++
			return nil
		})

		for i := from; i < to; i++ {
			_, err := js.Publish(insub1, []byte(fmt.Sprint(i)))
			jtest.RequireNil(t, err)
		}

		// rjet doesn't support StreamToHead, so use timeout. :(
		ctx2, cancel := context.WithTimeout(ctx, time.Millisecond*500)
		defer cancel()

		err := reflex.Run(ctx2, reflex.NewSpec(s.Stream, store, consumer))
		jtest.Require(t, context.DeadlineExceeded, err)
		ii.Equal(expect, to)
	}

	// rjet is slow to detect zero cursors
	err = store.SetCursor(ctx, cursor, "")
	jtest.RequireNil(t, err)

	runOnce(ii, 0, 10)

	val1, err := store.GetCursor(ctx, cursor)
	jtest.RequireNil(t, err)

	runOnce(ii, 11, 20)

	val2, err := store.GetCursor(ctx, cursor)
	jtest.RequireNil(t, err)

	ii.True(val1 < val2)
}
