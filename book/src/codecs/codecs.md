# Codecs

A `dnet` codec is responsible for converting messages between Rust values and wire format.

`dnet` separates encoding and decoding into two traits, then combines them into a single [`Codec`](https://docs.rs/dnet/latest/dnet/trait.Codec.html) trait.

## Encode

```rust
/// Trait for encoders.
pub trait Encode {
    /// Error type.
    type Error: std::error::Error;

    /// Encode message into writer.
    fn encode<W, T>(&mut self, writer: W, message: &T) -> Result<(), Self::Error>
    where
        W: Write,
        T: Serialize;
}
```

[`Encode`](https://docs.rs/dnet/latest/dnet/trait.Encode.html) takes a writable sink and a serializable message. Implementations may write messages in any wire format, such as JSON, binary, or length-delimited frames.

## Decode

```rust
/// Trait for decoders.
pub trait Decode {
    /// Error type.
    type Error: std::error::Error;

    /// Decode message from reader.
    fn decode<R, T>(&mut self, data: R) -> Result<T, Self::Error>
    where
        R: Read,
        for<'de> T: Deserialize<'de>;
}
```

[`Decode`](https://docs.rs/dnet/latest/dnet/trait.Decode.html) reads bytes from a readable source and produces a deserialized message value. The generic error type allows decoding failures to be reported precisely.

## Codec

```rust
/// Trait for `dnet` codecs.
pub trait Codec: Encode + Decode {}
```

`Codec` is a simple alias trait combining `Encode` and `Decode`. A `dnet` transport uses a `Codec` to transform outgoing messages into bytes and incoming bytes back into message values.

## Provided codecs

`dnet` ships with the following codecs in `dnet`'s [`codecs`](https://docs.rs/dnet-codecs/0.1.1/dnet_codecs/) module (behind the "codecs" feature, enabled by default):

- [`BincodeCodec`](./bincode.md)
  - Binary codec using [`bincode`].
  - Best for compact, fast wire format when size is important.
- [`JsonCodec`](./json.md)
  - Text codec using [`serde_json`].
  - Best for readable, interoperable payloads and debugging.

In addition, `dnet` includes length-delimiting [framing](framing/framing.md) codec for use with the [`FramedTransport`](../transports/framed.md):

- [`length-delimited::Codec`](framing/length-delimited.md)
  - Prefixes each frame with its length in bytes.

[`bincode`]: https://docs.rs/crate/bincode/2.0.1
[`serde_json`]: https://docs.rs/serde_json/
