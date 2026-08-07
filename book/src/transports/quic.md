# QUIC transports

`dnet` provides wrappers around Quinn primitives. The QUIC implementation includes both reliable stream-based transports over [`SendStream`](https://docs.rs/quinn/latest/quinn/struct.SendStream.html)/[`RecvStream`](https://docs.rs/quinn/latest/quinn/struct.RecvStream.html) and an unreliable datagram transport over [`Connection`](https://docs.rs/quinn/latest/quinn/struct.Connection.html).

## Available transports

- [`QuicFramedTransport`](https://docs.rs/dnet/latest/dnet/quic/framed/struct.QuicFramedTransport.html) - [framed](./framed.md) transport for QUIC streams.
- [`QuicTransport`](https://docs.rs/dnet/latest/dnet/quic/struct.QuicTransport.html) - length-delimited transport for QUIC streams.
- [`QuicUnreliableTransport`](https://docs.rs/dnet/latest/dnet/quic/unreliable/struct.QuicUnreliableTransport.html) - unreliable datagram transport built on QUIC datagrams.

## Reliable QUIC streams

`QuicTransport` is the most convenient option for typical message passing over QUIC streams. It composes `QuicFramedTransport` with [`dnet::io::length_delimited::Codec`](../codecs/framing/length-delimited.md).

`QuicTransport` supports:

- bidirectional streams (wrapping `SendStream` and `RecvStream`).
- unidirectional send-only and receive-only streams (wrapping `SendStream` or `RecvStream`).
- buffered versions for better throughput on many small messages.
- configurable maximum message length.

> [!NOTE]
> This is a reliable transport.<br>
> Reliable delivery and in-order delivery are handled by QUIC stream semantics.

## Unreliable QUIC datagrams

`QuicUnreliableTransport` wraps a `quinn::Connection` and uses QUIC datagrams for message transport.

This transport inherits QUIC datagram semantics:

- unreliable: messages may be dropped.
- unordered: messages can arrive out of order.
- duplicate delivery is possible.
- messages are bounded by the underlying datagram size.

`QuicUnreliableTransport` can be configured with [`Configuration`](https://docs.rs/dnet/latest/dnet/quic/unreliable/struct.Configuration.html) to:

- use [`Connection::send_datagram_wait`](https://docs.rs/quinn/latest/quinn/struct.Connection.html#method.send_datagram_wait) when `wait_after_send` is enabled.
- optionally close the underlying connection on transport drop or close.

> [!WARNING]
> This is an unreliable transport.<br>
> Messages may be dropped, arrive out of order, or be delivered multiple times.

## When to use which transport

- Use `QuicTransport` when you need a reliable, stream-based message channel.
- Use `QuicFramedTransport` when you want a custom framing codec on top of QUIC streams.
- Use `QuicUnreliableTransport` when you want datagram-style messaging with lower latency and no reliability guarantees.

## Example

See [QUIC example](https://github.com/druntime/dnet/tree/main/dnet/examples/quic).
