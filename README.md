# rjet

A [reflex](https://github.com/luno/reflex) stream client for a [NATS Jetstream](https://docs.nats.io/jetstream/jetstream).

It provides an API for consuming data from a nats jetstream stream with at-least-once semantics.
It also provides a reflex.CursorStore implementation for storing cursors in a nats jetstream (probably not a good idea).

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

- Since reflex events have specific fields (type, foreignID, timestamp, data) the redis stream entries need to adhere to a specific format. It is therefore advised to use Stream.Insert to ensure the correct format.
- At-least-once semantics are also provided by redis consumer groups, but it doesn't provide strict ordering. rredis maintains strict ordering and can provide sharding using reflex/rpatterns.Parralel.