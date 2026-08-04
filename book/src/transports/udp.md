# UDP transport

UDP transport wrapper for Tokio's [`UdpSocket`].

This transport inherits UDP semantics:
- unreliable: messages may be dropped,
- unordered: messages may arrive out of order or be duplicated,
- size-limited: messages are bounded by the datagram size for the underlying socket.

The transport type is [`UdpTransport<U, Codec, Incoming, Outgoing>`](https://docs.rs/dnet/latest/dnet/udp/struct.UdpTransport.html) where `U` borrows a `UdpSocket`.
It supports both connected sockets via `Sink<Outgoing>` and unconnected send via [`send_to`](https://docs.rs/dnet/latest/dnet/udp/struct.UdpTransport.html#method.send_to).

[`UdpSocket`]: https://docs.rs/tokio/latest/tokio/net/struct.UdpSocket.html

> [!WARNING]
> This is an unreliable transport.<br>
> Messages may be dropped, arrive out of order, or be delivered multiple times.

## Example

### Connected UDP

```rust
use futures::SinkExt;
use tokio::net::UdpSocket;
use dnet::{codecs::BincodeCodec, udp::UdpTransport, Receive};

let udp_socket = UdpSocket::bind("127.0.0.1:1234").await?;
udp_socket.connect("127.0.0.1:1235").await?;
let mut transport = UdpTransport::new(udp_socket, BincodeCodec::default());

transport.send("hello".to_string()).await?;
let reply: String = transport.receive().await?;
```

### Unconnected send/receive

```rust
use tokio::net::UdpSocket;
use dnet::{codecs::BincodeCodec, udp::UdpTransport};

let udp_socket = UdpSocket::bind("127.0.0.1:1236").await?;
let mut transport = UdpTransport::new(udp_socket, BincodeCodec::default());

transport.send_to("hello".to_string(), "127.0.0.1:1237").await?;
let (message, peer_addr) = transport.receive_from().await?;
println!("received {:?} from {}", message, peer_addr);
```

## Notes

- Use [`send_to`](https://docs.rs/dnet/latest/dnet/udp/struct.UdpTransport.html#method.send_to) when you need to send to different endpoints.
- Use the sink interface after `UdpSocket::connect(...)` for a simple connected UDP workflow.
- Use [`receive_from()`](https://docs.rs/dnet/latest/dnet/udp/struct.UdpTransport.html#method.receive_from) if you need the sender address for each incoming message.

[`UdpTransport`]: https://docs.rs/dnet/latest/dnet/udp/struct.UdpTransport.html
