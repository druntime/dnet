# Framing codecs

Framing codecs are used by [`FramedTransport`](https://docs.rs/dnet/latest/dnet/io/framed/struct.FramedTransport.html).

## What framing means

`FramedTransport` wraps a lower-level transport implementing [`AsyncRead`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncRead.html) and [`AsyncWrite`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncWrite.html). That means the transport sends a continuous stream of bytes, which must first be split into frames if we want to decode them into separate messages on the receiving side.

Framing codecs are responsible for this split.

## How framing codecs work

Framing codecs differ from normal codecs in that their error type implements [`NotEnoughData`](https://docs.rs/dnet/latest/dnet/io/framed/trait.NotEnoughData.html) — this tells `FramedTransport` that the codec needs more bytes before it can decode the incoming message.

```rust
/// Codecs implementing this trait can be used in [FramedTransport].
pub trait Framing: DecodeWithMessageLength {}

impl<T> Framing for T
where
    T: Decode + DecodeWithMessageLength,
    <T as Decode>::Error: NotEnoughData,
{
}
```

`DecodeWithMessageLength` can be used to obtain the byte length of a message in addition to the decoded message itself, before decoding.
This functionality is currently used only for logging.

## Framing schemes

Different frame splitting methods are possible. The `dnet` library currently includes one:
- [`length-delimited codec`](./length_delimited.md) — which writes the length of each frame in bytes at the beginning of the frame.
