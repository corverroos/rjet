# rjet

A [reflex](https://github.com/luno/reflex) stream client for a [NATS JetStream](https://docs.nats.io/jetstream/jetstream).

It provides an API for consuming data from a NATS JS stream with at-least-once delivery semantics. Since reflex does its
own cursor management, it uses ordered ephemeral consumers. 

It also provides a reflex.CursorStore implementation for storing cursors in a NATS JS stream (probably not a good idea).

### Usage

```
// Define your consumer business logic
fn := func(ctx context.Context, fate fate.Fate, e *reflex.Event) error {
  fmt.Print("Consuming redis stream event", e)
  return fate.Tempt() // Fate injects application errors at runtime, enforcing idempotent logic.
}

// Define some more variables
var namespace, streamName, consumerName string
var ctx context.Context  

// Connect to nats (ensure nats is running with -js flag)
con, _ := nats.Connect(nats.DefaultURL)
js, _ := con.JetStreamContext()

// Setup rredis and reflex
stream := rjet.NewStream(js, streamName, rjet.WithDefaultSteam(streamName)) // Let rjet configure the underlying stream
cstore := rjet.NewCursorStore(js, namespace)

consumer := reflex.NewConsumer(consumerName, fn)
spec := reflex.NewSpec(stream.Stream, cstore, consumer)

// Insert some data concurrently
go func() {
  for {
    _, _ = js.Publish(streamName, []byte(fmt.Sprint(time.Now())))
    time.Sleep(time.Second)
  }
}()

// Stream forever!
// Progress is stored in the cursor store, so restarts or any error continue where it left off.
for {
  err := reflex.Run(context.Backend(), spec)
  if err != nil { // Note Run always returns non-nil error
    log.Printf("stream error: %v", err)
  }
}
```

## Notes

- NATS JS ephemeral consumers are push based and therefore inherently "best-effort". `rjet` handles this by detecting 
dropped messages and return ErrDroppedMsg. The application code should just retry from the previous cursor.
  - Slow consumers drop messages, since messages are queued in a channel inside the NATS client. 
  - Dropped connections may result in dropped message even though the nats client auto-reconnects. It is suggested to use a very short nats.ReconnectWait. 

> Note: ErrDroppedMsg is expected in some edge cases, application code should just restart streaming from the previous cursor (default reflex behaviour). 