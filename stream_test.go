package rjet_test

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/corverroos/rjet"
	"github.com/luno/fate"
	"github.com/luno/jettison/jtest"
	"github.com/luno/reflex"
	"github.com/matryer/is"
	"github.com/nats-io/nats.go"
	"golang.org/x/sync/errgroup"
)

const (
	stream = "test-stream"
	insub1 = "input-subjet.1"
	insub2 = "input-subjet.2"
	insubs = "input-subjet.*"
)

var (
	subqlen = 100
)

func randStr() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func setup(t testing.TB) (context.Context, nats.JetStreamContext, *is.I, *proxy) {
	rand.Seed(time.Now().UnixNano())
	is := is.New(t)

	ctx, cancel := context.WithCancel(context.Background())

	p, purl := startProxy(t, ctx, "localhost:4222")

	c, err := nats.Connect("nats://"+purl,
		nats.ReconnectWait(time.Millisecond), // Fast reconnects
		nats.RetryOnFailedConnect(true),
		nats.SyncQueueLen(subqlen), // Use a small queue
	)
	is.NoErr(err)

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

	js, err := c.JetStream(nats.MaxWait(time.Millisecond * 500))
	is.NoErr(err)

	clean := func() {
		for _, stream := range []string{stream, "reflex-cursors-" + ns + "-" + cursor} {
			_ = js.PurgeStream(stream)
			_ = js.DeleteStream(stream)
		}
	}

	clean()
	t.Cleanup(func() {
		clean()
		c.Close()
		cancel()
	})

	return ctx, js, is, p
}

func TestBasicStream(t *testing.T) {
	ctx, js, ii, _ := setup(t)
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

	// Start async consumer that should first block, then streams everything.
	var wg sync.WaitGroup
	sc1, err := s.Stream(ctx, "")
	jtest.RequireNil(t, err)
	wg.Add(1)
	go func() {
		assertRand(ii, sc1, 1, 8)
		ii.NoErr(sc1.(io.Closer).Close())
		wg.Done()
	}()

	// Second consumer, synchronously insert and stream a few times.
	sc2, err := s.Stream(ctx, "")
	jtest.RequireNil(t, err)

	insertRand(ii, 1, 5)
	assertRand(ii, sc2, 1, 5)
	insertRand(ii, 6, 6)
	insertRand(ii, 7, 8)
	assertRand(ii, sc2, 6, 7)
	assertRand(ii, sc2, 8, 8)
	ii.NoErr(sc2.(io.Closer).Close())

	// Third consumer, streams everything in the past.
	sc3, err := s.Stream(ctx, "")
	jtest.RequireNil(t, err)
	assertRand(ii, sc3, 1, 8)
	ii.NoErr(sc3.(io.Closer).Close())

	// forth consumer, streams everything from a point the past.
	sc4, err := s.Stream(ctx, "3")
	jtest.RequireNil(t, err)
	assertRand(ii, sc4, 4, 8)
	ii.NoErr(sc4.(io.Closer).Close())

	wg.Wait()
}

func TestRobust(t *testing.T) {
	ctx, js, ii, _ := setup(t)
	store := rjet.NewCursorStore(js, ns)

	s, err := rjet.NewStream(js, stream, rjet.WithDefaultStream(insubs))
	jtest.RequireNil(t, err)

	runOnce := func(ii *is.I, from, to int) {
		ii.Helper()

		expect := from
		consumer := reflex.NewConsumer(cursor, func(ctx context.Context, f fate.Fate, e *reflex.Event) error {
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

var slow = flag.Bool("slow", false, "enable very slow tests")

func TestSlowConsumer(t *testing.T) {
	ctx, js, ii, _ := setup(t)

	s, err := rjet.NewStream(js, stream, rjet.WithDefaultStream(insubs))
	jtest.RequireNil(t, err)

	total := 2000
	if *slow {
		total = 100000
	}

	// Async insert messages
	go func() {
		for i := 1; i <= total; i++ {
			// Dedup on i
			h := make(nats.Header)
			h.Set(nats.MsgIdHdr, fmt.Sprint(i))

			_, err := js.PublishMsgAsync(&nats.Msg{
				Subject: insub1,
				Header:  h,
				Data:    []byte(fmt.Sprint(i)),
			})
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, nats.ErrTimeout) {
				// This sometimes happens due to broken connections, just try again.
				// Note that the message might have been published, that is why we dedup.
				i--
				continue
			}
			jtest.RequireNil(t, err, "Published", i)
			if i%1000 == 0 {
				fmt.Println("Published", i)
			}
		}
	}()

	// Keep on slow streaming, retrying after dropped messages, until we got everything
	var (
		after string
		got   int
	)
	for {
	retry:
		sc, err := s.Stream(ctx, after)
		jtest.RequireNil(t, err)

		// Start a consumer that sleeps a second every 1000 msgs.
		for {
			e, err := sc.Recv()
			if errors.Is(err, rjet.ErrDroppedMsg) {
				goto retry
			}
			jtest.RequireNil(t, err)

			after = e.ID
			got++

			ii.Equal(e.IDInt(), int64(got))
			if got%1000 == 0 {
				fmt.Println("Streamed", got)
				time.Sleep(time.Second)
			}
			if got == total {
				// Woohoo we got everything!
				return
			}
		}
	}
}

func TestBrokenConns(t *testing.T) {
	ctx, js, ii, p := setup(t)
	store := rjet.NewCursorStore(js, ns)

	// Sleeps random < n*100ms
	sleepRandom := func(n int) {
		time.Sleep(time.Duration(n*rand.Intn(100)) * time.Millisecond)
	}

	// Send and expect 20 messages
	total := 20
	received := make(chan *reflex.Event, total)

	consumer := reflex.NewConsumer(cursor, func(ctx context.Context, f fate.Fate, e *reflex.Event) error {
		sleepRandom(2) // Simulate slow consumer (up to 200ms)
		received <- e
		return nil
	})

	s, err := rjet.NewStream(js, stream, rjet.WithDefaultStream(insubs))
	jtest.RequireNil(t, err)

	// Async consume forever, expect connection closed errors.
	go func() {
		for {
			err := reflex.Run(ctx, reflex.NewSpec(s.Stream, store, consumer))
			if ctx.Err() != nil {
				return
			}
			jtest.Require(t, nats.ErrConnectionClosed, err)
		}
	}()

	// Async insert messages
	go func() {
		for i := 0; i < total; i++ {
			// Dedup on i
			h := make(nats.Header)
			h.Set(nats.MsgIdHdr, fmt.Sprint(i))

			_, err := js.PublishMsg(&nats.Msg{
				Subject: insub1,
				Header:  h,
				Data:    []byte(fmt.Sprint(i)),
			})
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, nats.ErrTimeout) {
				// This sometimes happens due to broken connections, just try again.
				// Note that the message might have been published, that is why we dedup.
				i--
				continue
			}
			jtest.RequireNil(t, err, "Published", i)
			sleepRandom(1) // Simulate slow producer (up to 100ms)
		}
	}()

	// Async break connections
	go func() {
		var broken int
		t.Cleanup(func() {
			ii.True(broken > 0) // No broken connections
		})
		for {
			// Simulate broken connections (400ms to 500ms)
			time.Sleep(time.Millisecond * 400)
			sleepRandom(1)
			n, err := p.BreakAll()
			if ctx.Err() != nil {
				return
			}
			if err != nil && strings.Contains(err.Error(), "use of closed network connection") {
				continue
			}
			jtest.RequireNil(t, err)
			broken += n
		}
	}()

	// Wait to receive all correct messages
	var prev *reflex.Event
	for i := 0; i < total; i++ {
		e := <-received
		expID := int64(i + 1)
		if e.IDInt() > expID { // Note this happens sometimes :(
			t.Fatalf("Missed message, prev %v, next %v", prev.ID, e.ID)
		}
		if e.IDInt() < expID {
			t.Fatalf("Duplicate message, prev %v, next %v", prev.ID, e.ID)
		}
		prev = e
	}
}

func TestSubjectFilter(t *testing.T) {
	ctx, js, ii, _ := setup(t)

	const total = 10

	// Ensure stream is created
	_, err := rjet.NewStream(js, stream, rjet.WithDefaultStream(insubs))
	ii.NoErr(err)

	// Insert 10 to each subscription.
	for i := 0; i < total; i++ {
		_, err := js.Publish(insub1, []byte(fmt.Sprint(i)))
		ii.NoErr(err)

		_, err = js.Publish(insub2, []byte(fmt.Sprint(i*2)))
		ii.NoErr(err)
	}

	assert := func(ii *is.I, subj string, multi int) {
		ii.Helper()

		s, err := rjet.NewStream(js, stream, rjet.WithSubjectFilter(subj))
		jtest.RequireNil(t, err)

		sc, err := s.Stream(ctx, "")
		jtest.RequireNil(t, err)

		for i := 0; i < total; i++ {
			e, err := sc.Recv()
			jtest.RequireNil(t, err, i)

			ii.Equal(e.MetaData, []byte(fmt.Sprint(i*multi)))
		}
	}

	assert(ii, insub1, 1)
	assert(ii, insub2, 2)
}

func TestBench01(t *testing.T) {
	subqlen = 0
	ctx, js, ii, _ := setup(t)

	const (
		total   = 10000
		payload = 64
	)

	b := make([]byte, payload)
	_, err := rand.Read(b)
	ii.NoErr(err)

	s, err := rjet.NewStream(js, stream, rjet.WithDefaultStream(insub1))
	ii.NoErr(err)

	t0 := time.Now()

	go func() {
		for i := 0; i < total; i++ {
			_, err := js.PublishAsync(insub1, b)
			ii.NoErr(err)
		}
		<-js.PublishAsyncComplete()
		delta := time.Since(t0)
		fmt.Printf("Publish done after %s, %.1f msgs/sec\n", delta, total/delta.Seconds())
	}()

	sc, err := s.Stream(ctx, "")
	ii.NoErr(err)

	for i := 0; i < total; i++ {
		_, err := sc.Recv()
		ii.NoErr(err)
	}

	delta := time.Since(t0)

	fmt.Printf("Duration=%dms, Total=%d, Payload=%d bytes, Throughput=%.0f msgs/sec\n", delta.Milliseconds(), total, payload, total/delta.Seconds())
}

func startProxy(t testing.TB, ctx context.Context, target string) (*proxy, string) {
	l, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "localhost:0")
	if err != nil {
		t.Error(err)
	}

	p := &proxy{l: l}

	go func() {
		for {
			in, err := l.Accept()
			if err != nil {
				t.Error(err)
			}

			out, err := net.Dial("tcp", target)
			if err != nil {
				t.Error(err)
			}

			t.Cleanup(func() {
				in.Close()
				out.Close()
			})

			go p.handle(t, in, out)
		}
	}()

	return p, l.Addr().String()
}

type proxy struct {
	l      net.Listener
	target string

	mu    sync.Mutex
	conns []net.Conn
}

func (p *proxy) BreakAll() (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, conn := range p.conns {
		err := conn.Close()
		if err != nil {
			return 0, err
		}
	}

	l := len(p.conns)
	p.conns = nil

	return l, nil
}

func (p *proxy) handle(t testing.TB, in, out net.Conn) {
	p.mu.Lock()
	p.conns = append(p.conns, in)
	p.mu.Unlock()

	var eg errgroup.Group

	go eg.Go(func() error {
		_, err := io.Copy(in, out)
		return err
	})

	go eg.Go(func() error {
		_, err := io.Copy(out, in)
		return err
	})

	err := eg.Wait()
	if err != nil && strings.Contains(err.Error(), "use of closed network connection") {
		return
	} else if err != nil {
		// Most connections (io.EOF) do not result in an error
		t.Error(err)
	}
}
