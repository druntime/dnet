# Framed transport

[`FramedTransport`](https://docs.rs/dnet/latest/dnet/io/framed/struct.FramedTransport.html) is a generic transport wrapper for [`AsyncRead`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncRead.html) + [`AsyncWrite`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncWrite.html) streams that uses a [framing](../codecs/framing/framing.md) codec to divide a continuous byte stream into discrete messages.

This transport requires a codec implementing [`dnet::io::framed::Framing`](https://docs.rs/dnet/latest/dnet/io/framed/trait.Framing.html). 

## What it does

- Wraps a lower-level transport that implements [`AsyncRead`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncRead.html) and [`AsyncWrite`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncWrite.html).
- Encodes outgoing messages into frames using the provided codec.
- Reads incoming bytes into an internal buffer and decodes complete frames as they arrive.

## Constructors

- [`FramedTransport::new(transport, codec)`](https://docs.rs/dnet/latest/dnet/io/framed/struct.FramedTransport.html#method.new) creates a raw framed transport.
- [`FramedTransport::buffered(transport, codec)`](https://docs.rs/dnet/latest/dnet/io/framed/struct.FramedTransport.html#method.buffered) wraps the transport in buffered I/O for better throughput with many small writes or reads.

## When to use it

- Use [`FramedTransport`](https://docs.rs/dnet/latest/dnet/io/framed/struct.FramedTransport.html) when you need a generic framed protocol over any async byte stream.
- Use [`TcpFramedTransport`](./tcp.md) or [`QuicFramedTransport`](./quic.md) when you want a transport-specific wrapper that already exposes the same framed behavior.
- Use [`length_delimited::Codec`](../codecs/framing/length-delimited.md) for the common case of messages prefixed with their length.

## Notes

- A framing codec can implement custom delimiting strategies beyond length-prefix framing.
- The codec must preserve partial input until a full frame can be decoded.
- [`FramedTransport::buffered`](https://docs.rs/dnet/latest/dnet/io/framed/struct.FramedTransport.html#method.buffered) is often a good default for network streams.
