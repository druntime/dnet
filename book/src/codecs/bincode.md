# Bincode codec

The [`BincodeCodec`](https://docs.rs/dnet/latest/dnet/codecs/struct.BincodeCodec.html) serializes and deserializes messages using [`bincode`](https://docs.rs/bincode/).

## When to use it

Use `BincodeCodec` when you want a fast, compact wire format and both peers agree on the same Rust data types.

It is a good choice when message size and speed matter.
