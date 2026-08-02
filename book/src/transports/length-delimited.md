# Length-delimited transport

The [`LengthDelimitedTransport`](https://docs.rs/dnet/latest/dnet/io/length_delimited/struct.LengthDelimitedTransport.html) is a convenience wrapper around the [`FramedTransport`](./framed.md)  transport that uses a length-delimited framing codec.

It prefixes each message with a 4‑byte big-endian length (in bytes) and then writes the encoded message payload. This is the common framing scheme when sending discrete messages over a byte stream (for example TCP, IPC, or QUIC streams).

Under the hood the module composes a [`FramedTransport`](./framed.md) with [`dnet::io::length_delimited::Codec`](../codecs/framing/length_delimited.md), so you get the same framed transport semantics but with the length-prefixed codec already set up.

## When to use

Use `LengthDelimitedTransport` when you want a simple length-prefixed framing for sending serialized messages over a byte stream.

If you need a different framing scheme, use [`FramedTransport`](./framed.md) directly with a custom codec.

## Example

See [IPC example](https://github.com/druntime/dnet/tree/main/dnet/examples/ipc).
