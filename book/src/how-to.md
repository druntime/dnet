# How to use `dnet`

`dnet` is a easy-to-use async messaging abstraction. You define the message shapes your application needs, choose a transport and a codec, then send and receive messages over that transport.

This page uses TCP examples, but the same concepts apply to other transports such as WebSocket, UDP, channel transport, etc.

## Dependencies

To use `dnet`, add it to your `Cargo.toml`, along with [`serde`](https://crates.io/crates/serde) for message serialization and [`futures`](https://crates.io/crates/futures) for async stream and sink extensions:

```toml
[dependencies]
dnet = "*"
serde = { version = "*", features = ["derive"] }
futures = "*"
```

## Messages

Define the messages your application will send and receive. Use `serde` to derive serialization and deserialization for your message types.

```rust
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ServerMessage {
    some_string: String,
    some_int: u32,
}
```

Incoming and outgoing messages can be of different types:

```rust
#[derive(Serialize, Deserialize)]
pub struct ClientMessage {
    some_float: f64,
}
```

## Transport

A transport wraps an underlying I/O channel and provides `dnet` message send/receive operations.

Here we are using TCP, but the same concepts apply to other transports such as WebSocket, UDP, channel transport, etc.

> [!NOTE]
> Different transports require different setup of the underlying protocol/channel/etc. before they can wrap them. See the transport documentation for details.

```rust
use dnet::{codecs::BincodeCodec, tcp::TcpTransport};
use tokio::net::TcpStream;

let stream = TcpStream::connect("127.0.0.1:8080").await?;
let mut transport = TcpTransport::<_, _, ClientMessage, ServerMessage>::buffered(
    stream,
    BincodeCodec::default(),
);
```

## Sending

Send values with `future`'s `Sink` [`send(...)`](https://docs.rs/futures/latest/futures/sink/trait.SinkExt.html#method.send):

```rust
use futures::SinkExt;

transport.send(ServerMessage { 
    some_string: "Hello, world!".into(), 
    some_int: 42 
}).await?;
```

## Receiving

Receive values with `future`'s `Stream` [`next()`](https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html#method.next):

```rust
use futures::StreamExt;

match transport.next().await {
    Some(Ok(message)) => {
        println!("received message from the server: {}", message.some_string);
        transport.send(ClientMessage { some_float: 3.14 }).await?;
    }
    Some(Err(error)) => {
        // handle receive error
    }
    None => {
        // transport closed
    }
}
```

or by using the [`receive()`](https://docs.rs/dnet/latest/dnet/trait.Receive.html#tymethod.receive) method:

```rust
use dnet::Receive as _;

let incoming: ClientMessage = transport.receive().await?;
println!("received message from the client: {}", message.some_float);
```

## Logging

`dnet` transports support logging via the [`Logging`](https://docs.rs/dnet/latest/dnet/trait.Logging.html) trait (enabled with the "logging" feature). To see transport logs, initialize a [`tracing`](https://docs.rs/tracing/latest/tracing/) subscriber from [`tracing-subscriber`](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/), then enable logging on the transport.

```toml
[dependencies]
dnet = { version = "*", features = ["logging"] }
tracing-subscriber = "*"
```

```rust
use dnet::{codecs::BincodeCodec, tcp::TcpTransport};
use tracing_subscriber;

tracing_subscriber::fmt().init();

let stream = TcpStream::connect("127.0.0.1:8080").await?;
let mut transport = TcpTransport::<_, _, ServerMessage, ClientMessage>::buffered(
    stream,
    BincodeCodec::default(),
);
// optionally: transport.set_logging_name("tcp-client");
transport.enable_logging();
```

## Examples

See the repository [examples](https://github.com/druntime/dnet/tree/main/dnet/examples) for complete working code.
