# MessagePort transport

[`MessagePortTransport`](https://docs.rs/dnet/0.1.4/wasm32-unknown-unknown/dnet/message_port/struct.MessagePortTransport.html) wraps a browser [`MessagePort`](https://developer.mozilla.org/en-US/docs/Web/API/MessagePort) and exposes it as a `dnet` transport.
It is useful when you need a dedicated channel between two JavaScript contexts, such as:

- two sides of a [`MessageChannel`](https://developer.mozilla.org/en-US/docs/Web/API/MessageChannel),
- a page and an iframe,
- a page and a worker when the port is passed with `postMessage()`,
- or any other API that uses `MessagePort`.

- [`MessagePortTransport::new(port, codec).await`](https://docs.rs/dnet/0.1.4/wasm32-unknown-unknown/dnet/message_port/struct.MessagePortTransport.html#method.new) creates a transport over the given `MessagePort`.
- [`MessagePortTransport::new_with_name(port, codec, name).await`](https://docs.rs/dnet/0.1.4/wasm32-unknown-unknown/dnet/message_port/struct.MessagePortTransport.html#method.new_with_name) creates a named transport.

> [!NOTE]
> The underlying `MessagePort` is started automatically and the transport waits for the peer side to open before returning.

Multiple named transports can coexist on the same `MessagePort`, and they can be used at the same time as an unnamed transport on that port.
This makes it easy to multiplex several logical channels over the same underlying port pair.

If you only need a direct worker-host channel, prefer [`WebWorkerTransport`](./webworker.md).

If you need to send transferable objects across the port, see the [Transferable](./transferable.md) transport section.

## Example

Create a `MessageChannel`, then wrap each port with `MessagePortTransport`.

```rust
use futures::SinkExt;
use web_sys::MessageChannel;
use dnet::{codecs::BincodeCodec, message_port::MessagePortTransport, Receive};

let channel = MessageChannel::new().unwrap();
let port1 = channel.port1();
let port2 = channel.port2();

let mut left = MessagePortTransport::new(port1, BincodeCodec::default()).await?;
left.send("hello from left".to_string()).await?;

// pass the other port to another context, or just use it in the same context:

let mut right = MessagePortTransport::new(port2, BincodeCodec::default()).await?;

let reply: String = right.receive().await?;
right.send(format!("hello from right, got: {}", reply)).await?;
```
