# JSON codec

The [`JsonCodec`](https://docs.rs/dnet-codecs/0.1.0/dnet_codecs/json/struct.JsonCodec.html) serializes and deserializes messages using [`serde_json`]. It is a text-based codec that is easy to inspect and useful for debugging, interoperability, and transport protocols that expect JSON.

[`serde_json`]: https://docs.rs/serde_json/

## When to use it

Use `JsonCodec` when readability and interoperability are more important than binary compactness.

It is a good fit for HTTP-friendly transports, browser interoperability, debugging, or when message structure must be inspected easily.
