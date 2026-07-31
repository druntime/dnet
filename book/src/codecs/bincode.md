# `bincode` codec

The [`BincodeCodec`](https://docs.rs/dnet-codecs/0.1.0/dnet_codecs/bincode/struct.BincodeCodec.html) serializes and deserializes messages using [`bincode`](https://docs.rs/bincode/).

## When to use it

Use `BincodeCodec` when you want a fast, compact wire format and both peers agree on the same Rust data types.

It is a good choice when message size and speed matter.
