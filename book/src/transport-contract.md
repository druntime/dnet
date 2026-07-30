# Transport contract

A `dnet` transport implements both [`futures::Sink`](https://docs.rs/futures/latest/futures/sink/trait.Sink.html) and [`futures::Stream`](https://docs.rs/futures/latest/futures/stream/trait.Stream.html) to provide a unified asynchronous transport interface.

A `dnet` transport must satisfy these requirements:

- Implement `Sink<Outgoing, Error = crate::Error<Error>>` for outgoing messages.
- Implement `Stream<Item = Result<Incoming, Error>>` for incoming messages.
- The `Sink` error type is always `dnet::Error<Other>`, where `Other` is the transport-specific error type.
- Receiving from the `Stream` yields `Result<Incoming, Error>`; the stream does not use `dnet::Error` for the item error.
- The generic `Error` type of the result yielded by the `Stream` matches the generic `Other` error of the `dnet::Error<Other>` used by the `Sink`.
- The `Stream` returns `None` to signal that the transport is closed, regardless of the underlying cause.
- When the `logging` feature is enabled, transports also implement `logging::Logging`.

```rust
/// Transport error.
#[derive(Debug, PartialEq, Eq)]
pub enum Error<Other> {
    /// Occurs when transport is closed.
    Closed,

    /// Other non-predefined transport-specific error.
    Other(Other),
}

#[cfg(not(feature = "logging"))]
/// Trait for transports implementing `dnet` interface.
pub trait Transport<Incoming, Outgoing, Error>:
    Sink<Outgoing, Error = crate::Error<Error>> + Stream<Item = Result<Incoming, Error>>
{
}

#[cfg(not(feature = "logging"))]
impl<T, Incoming, Outgoing, Error> Transport<Incoming, Outgoing, Error> for T where
    T: Sink<Outgoing, Error = crate::Error<Error>> + Stream<Item = Result<Incoming, Error>>
{
}

#[cfg(feature = "logging")]
/// Trait for transports implementing `dnet` interface.
pub trait Transport<Incoming, Outgoing, Error>:
    Sink<Outgoing, Error = crate::Error<Error>>
    + Stream<Item = Result<Incoming, Error>>
    + logging::Logging
{
}

#[cfg(feature = "logging")]
impl<T, Incoming, Outgoing, Error> Transport<Incoming, Outgoing, Error> for T where
    T: Sink<Outgoing, Error = crate::Error<Error>>
        + Stream<Item = Result<Incoming, Error>>
        + logging::Logging
{
}
```
