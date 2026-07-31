# TCP transport

TCP transport wrappers in `dnet` are intended for Tokio TCP streams and can be used for plain TCP or TLS-over-TCP.

- [`TcpFramedTransport`](https://docs.rs/dnet/latest/dnet/tcp/framed/struct.TcpFramedTransport.html) requires a codec implementing [`Framing`].
- [`TcpTransport`](https://docs.rs/dnet/latest/dnet/tcp/struct.TcpTransport.html) uses [`length_delimited::Codec`] to frame messages.

## Buffered constructors

The `buffered` variants are recommended when the transport sees many small messages or when you want to reduce syscall overhead.
They wrap the underlying stream in buffered I/O and can improve throughput without changing framing behavior.

## Examples

### Plain TCP

> [!WARNING]
> We are creating an insecure (unencrypted) connection here.

#### Server

A server accepts a plain TCP connection, receives a `String`, then sends an `i32` back.

```rust
use futures::SinkExt;
use tokio::net::TcpListener;
use dnet::{codecs::BincodeCodec, tcp::TcpTransport, Receive};

let listener = TcpListener::bind("127.0.0.1:8080").await?;
let (tcp_stream, _peer_addr) = listener.accept().await?;
let mut transport = TcpTransport::buffered(tcp_stream, BincodeCodec::default());

let name: String = transport.receive().await?;
transport.send(name.len() as i32).await?;
```

#### Client

A client connects over plain TCP, sends a `String`, and receives an `i32` reply.

```rust
use futures::SinkExt;
use tokio::net::TcpStream;
use dnet::{codecs::BincodeCodec, tcp::TcpTransport, Receive};

let tcp_stream = TcpStream::connect("127.0.0.1:8080").await?;
let mut transport = TcpTransport::buffered(tcp_stream, BincodeCodec::default());

transport.send("hello".to_string()).await?;
let length: i32 = transport.receive().await?;
```

### TLS

TLS (Transport Layer Security) encrypts and authenticates the TCP connection, protecting messages from eavesdropping and tampering. See [Transport Layer Security](https://en.wikipedia.org/wiki/Transport_Layer_Security) for more details.

> [!WARNING]
> Cryptography is hard - make sure you know what you're doing.

#### Server

A TLS server accepts the socket first, then upgrades it before wrapping it in `TcpTransport`.

```rust
use futures::SinkExt;
use native_tls::{Identity, TlsAcceptor};
use tokio_native_tls::TlsAcceptor as TokioTlsAcceptor;
use tokio::net::TcpListener;
use dnet::{codecs::BincodeCodec, tcp::TcpTransport, Receive};

let listener = TcpListener::bind("0.0.0.0:8443").await?;
let (tcp_stream, _peer_addr) = listener.accept().await?;

let identity = Identity::from_pkcs12(&pkcs12_bytes, "password")?;
let tls_acceptor = TlsAcceptor::new(identity)?;
let tokio_acceptor = TokioTlsAcceptor::from(tls_acceptor);
let tls_stream = tokio_acceptor.accept(tcp_stream).await?;

let mut transport = TcpTransport::buffered(tls_stream, BincodeCodec::default());
let message: String = transport.receive().await?;
transport.send(message.len() as i32).await?;
```

#### Client

A TLS client connects to the server and then negotiates TLS before wrapping the stream.

```rust
use futures::SinkExt;
use native_tls::TlsConnector;
use tokio_native_tls::TlsConnector as TokioTlsConnector;
use tokio::net::TcpStream;
use dnet::{codecs::BincodeCodec, tcp::TcpTransport, Receive};

let tcp_stream = TcpStream::connect("example.com:8443").await?;
let connector = TlsConnector::new()?;
let tokio_connector = TokioTlsConnector::from(connector);
let tls_stream = tokio_connector.connect("example.com", tcp_stream).await?;

let mut transport = TcpTransport::buffered(tls_stream, BincodeCodec::default());
transport.send("hello".to_string()).await?;
let length: i32 = transport.receive().await?;
```

[`Framing`]: https://docs.rs/dnet/latest/dnet/io/framed/trait.Framing.html
[`length_delimited::Codec`]: https://docs.rs/dnet/latest/dnet/io/length_delimited/struct.Codec.html
