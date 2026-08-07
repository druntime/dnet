# Core concepts

In `dnet`, there are three central concepts that structure how messaging works:

## Message

A "Message" is a struct defined by the user of the library. It represents the payload that will be sent or received over a `dnet` transport.

- Outgoing messages are sent by your application.
- Incoming messages are received from a peer.
- The types used for outgoing and incoming messages may differ.

`dnet` itself does not impose a fixed message schema; you define the message shapes that make sense for your application.

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OutgoingMessage {
    content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IncomingMessage {
    content: String,
    timestamp: u64,
}
```

## Codec

A "Codec" is a struct(/enum) implementing `dnet`'s [`Codec`](https://docs.rs/dnet/latest/dnet/trait.Codec.html) that defines how messages are encoded and decoded.

The codec is passed as a parameter to a `Transport` and is responsible for converting message values to and from a wire format.

"Codec" implementations work with message types that implement [Serde](https://serde.rs/)'s [`Serialize`](https://docs.rs/serde/latest/serde/trait.Serialize.html) and [`Deserialize`](https://docs.rs/serde/latest/serde/trait.Deserialize.html) traits.

```rust
let codec = dnet::codecs::BincodeCodec::default();
```

## Transport

A `Transport` is a struct(/enum) defined by the `dnet` library (or by a third-party implementation that adheres to the `dnet` transport contract).

A transport wraps a lower-level networking protocol (like TCP or UDP), channel, or other communication medium and provides an asynchronous interface for sending and receiving messages.

See [Transport contract](./transport-contract.md) for details on the `dnet` transport interface.

```rust
use futures::SinkExt;
use tokio::net::TcpStream;

use dnet::Receive as _;

let stream = TcpStream::connect("127.0.0.1:1234").await?;
let mut transport = TcpTransport::buffered(stream, codec);

transport.send(OutgoingMessage { content: "Hello from dnet".to_string() }).await?;
let incoming: IncomingMessage = transport.receive().await?;
```
