# WebSocket transport

[`WebSocketTransport`](https://docs.rs/dnet/latest/dnet/websocket/struct.WebSocketTransport.html) wraps a WebSocket connection and exposes it as a `dnet` transport.
It sends and receives binary messages using a configured `Codec` to (de)serialize them.

On `wasm32`, this transport wraps a browser [`web_sys::WebSocket`](https://docs.rs/web-sys/latest/web_sys/struct.WebSocket.html).
On native targets it wraps a [`tokio_tungstenite`](https://github.com/snapview/tokio-tungstenite)'s [`WebSocketStream`](https://docs.rs/tokio-tungstenite/latest/tokio_tungstenite/struct.WebSocketStream.html).
For `axum` servers, use [`dnet::websocket::axum::WebSocketTransport`](https://docs.rs/dnet/latest/dnet/websocket/axum/struct.WebSocketTransport.html) with [`axum::extract::ws::WebSocket`](https://docs.rs/axum/latest/axum/extract/ws/struct.WebSocket.html).

> [!NOTE]
> This transports `WebSocket` binary frames only. Text, ping, pong and other non-binary frames are ignored by the receiver.

## Browser example

```rust
use futures::SinkExt;
use dnet::{codecs::BincodeCodec, websocket::WebSocketTransport, Receive};
use web_sys::WebSocket;

let web_socket = WebSocket::new("ws://localhost:8080/ws")?;
let mut transport = WebSocketTransport::new(web_socket, BincodeCodec::default()).await?;

transport.send("hello".to_string()).await?;
let reply: String = transport.receive().await?;
```

## Native example

```rust
use futures::SinkExt;
use dnet::{codecs::BincodeCodec, websocket::WebSocketTransport, Receive};
use tokio_tungstenite::connect_async;

let (stream, _) = connect_async("ws://localhost:8080/ws").await?;
let mut transport = WebSocketTransport::new(stream, BincodeCodec::default());

transport.send("hello".to_string()).await?;
let reply: String = transport.receive().await?;
```

If you want the transport to create the connection for you, native clients can use `WebSocketTransport::new_with_address(url, codec).await`.

## Axum server example

```rust
use futures::SinkExt;
use dnet::{codecs::BincodeCodec, websocket::axum::WebSocketTransport, Receive};
use axum::extract::ws::WebSocket;

async fn handle_socket(socket: WebSocket) {
    let (mut sender, mut receiver) =
        WebSocketTransport::new(socket, BincodeCodec::default()).split();

    sender.send("welcome".to_string()).await.unwrap();
    let message: String = receiver.receive().await.unwrap();
    println!("received {message}");
}
```
