# Consuming

Consumers are used to consume the service API on the client side.

## Creating a consumer

The `#[api]` macro generates a `Consumer` type for your API trait.

Create it with [`Consumer::consume`](https://docs.rs/dnet-rpc/0.1.1/dnet_rpc/consumer/trait.Consume.html#tymethod.consume) function over a `dnet` transport:

```rust
use dnet::rpc::Consume;
use dnet::{codecs::BincodeCodec, tcp::TcpTransport};

let stream = tokio::net::TcpStream::connect(address).await?;
let transport = TcpTransport::buffered(stream, BincodeCodec::default());
let consumer = Consumer::consume(transport, Default::default(), Default::default());
```

## Calling remote methods

Once the consumer is created, you call remote methods as ordinary async Rust methods:

```rust
consumer.hello_world().await?;
let sum = consumer.add_numbers(2, 3).await?;
```

Consumer methods return (after awaiting) a [`Result`](https://docs.rs/dnet-rpc/0.1.1/dnet_rpc/consumer/type.Result.html) which error is [`Error`](https://docs.rs/dnet-rpc/0.1.1/dnet_rpc/enum.Error.html) inside `rpc` module.

## Stream methods

If the API declares a streaming method, the consumer call returns a stream handle:

```rust
let mut time_stream = consumer.stream_time(std::time::Duration::from_secs(1)).await?;
```

> [!NOTE]
> Notice that you have to await your streaming request first - only then you will receive an actual stream instance.

You can then read values from the stream until it closes:

```rust
use futures::StreamExt;
while let Some(time) = time_stream.next().await {
    println!("Time: {}", time);
}
```

Streaming consumer methods return type [`Stream`](https://docs.rs/dnet-rpc/0.1.1/dnet_rpc/consumer/struct.Stream.html) inside `rpc`'s `consumer` module.

## Aborting requests and streams

Generated request futures and stream handles support aborting.<br>
For example:

```rust
use tokio::time::{sleep, Duration};

let mut long_task_future = consumer.long_task();
let aborter = long_task_future.aborter();

tokio::select! {
    result = &mut long_task_future => {
        println!("Task completed: {result:?}");
    }

    _ = sleep(Duration::from_secs(10)) => {
        aborter.abort();
        println!("Task aborted due to taking too long.");
    }
}
```

## Notes

- A consumer can make many requests over the same transport.
- Each request is matched to a response using an internal request ID.
- Streaming values are delivered in order until the producer closes the stream.
