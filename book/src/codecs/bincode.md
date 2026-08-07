# `bincode` codec

The [`BincodeCodec`](https://docs.rs/dnet-codecs/latest/dnet_codecs/bincode/struct.BincodeCodec.html) serializes and deserializes messages using [`bincode`](https://docs.rs/crate/bincode/2.0.1).

## When to use it

Use `BincodeCodec` when you want a fast, compact wire format and when message size and speed matter.
