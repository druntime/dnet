# Transports

`dnet` provides a variety of transport implementations for different networking scenarios.

Transports are implemented for both native and WebAssembly targets (when appropriate).

## Provided transports

- [TCP](./tcp.md) - transport over [tokio](https://docs.rs/tokio/latest/tokio/)'s TCP sockets (native only)
- [UDP](./udp.md) - transport over [tokio](https://docs.rs/tokio/latest/tokio/)'s UDP sockets (native only)
- [QUIC](./quic.md) - transports over [quinn](https://docs.rs/quinn/latest/quinn/)'s QUIC protocol (native only)
- [WebSocket](./websocket.md) - transport over WebSockets (native and WebAssembly, both server - [axum](https://docs.rs/axum/latest/axum/)'s [`WebSocket`](https://docs.rs/axum/latest/axum/extract/ws/struct.WebSocket.html) and client - [tungstenite](https://docs.rs/tungstenite/latest/tungstenite/)'s [`WebSocket`](https://docs.rs/tungstenite/latest/tungstenite/protocol/struct.WebSocket.html) on native, and [web-sys](https://docs.rs/web-sys/latest/web_sys/)'s [`WebSocket`](https://docs.rs/web-sys/latest/web_sys/struct.WebSocket.html) on WebAssembly)
- [MessagePort](./message-port.md) - transport over [MessagePort](https://developer.mozilla.org/en-US/docs/Web/API/MessagePort) (WebAssembly only)
- [Web Worker](./webworker.md) - transport to and from a [Web Worker](https://developer.mozilla.org/en-US/docs/Web/API/Worker) (WebAssembly only)
- [Transferable](./transferable.md) - transport for [transferable objects](https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API/Transferable_objects) (WebAssembly only)
- [Framed transport](./framed.md) - framed message transport
- [Length-delimited transport](./length-delimited.md) - length-delimited message transport
- [Utilities](./utils.md)

## Available for licensing

- [DataChannel](./data-channel.md) - transport over [WebRTC](https://webrtc.org/) [data channels](https://developer.mozilla.org/en-US/docs/Web/API/WebRTC_API/Using_data_channels) (native and WebAssembly)

## Logging

The "logging" feature is not enabled by default. If you want transport-level diagnostics, enable it in your `Cargo.toml`:

```toml
[dependencies]
dnet = { version = "0.1.4", features = ["logging"] }
```

When the "logging" feature is enabled, supported transports implement the [`Logging`](https://docs.rs/dnet/latest/dnet/trait.Logging.html) trait. You can then enable logging at runtime and assign a logical transport name for easier filtering and debugging:

```rust
use dnet::Logging;

let mut transport = /* create transport */;
transport.set_logging_name("peer-1");
transport.enable_logging();
```

Named transports are especially useful when you use multiple transports of the same kind in your application.
