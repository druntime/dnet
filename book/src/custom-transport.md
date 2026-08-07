# Writing a custom `dnet` transport

A `dnet` transport must implement the transport contract described in the [transport contract](./transport-contract.md).

## What your transport must do

- Implement `futures::Sink<Outgoing, Error = dnet::Error<Other>>`.
- Implement `futures::Stream<Item = Result<Incoming, Other>>`.
- Preferably implement `futures::stream::FusedStream` as well.
- Conditionally implement `dnet_base::Logging` behind a "logging" feature in your crate.

The `Other` type is your transport-specific error type.

### Sink

The outgoing side of a transport always uses `dnet::Error<Other>` as the `Sink` error type. 

Example:

```rust
impl<Codec, Incoming, Outgoing> Sink<Outgoing> for MyTransport<Codec, Incoming, Outgoing> {
    type Error = dnet::Error<MyError>;
    // ... poll_ready, start_send, poll_flush, poll_close ...
}
```

### Stream

The incoming side is a `Stream` of `Result<Incoming, Other>`, where the same `Other` error type is used directly in the stream item.

> [!NOTE]
> Do **not** wrap stream item errors in `dnet::Error`.

Example:

```rust
impl<Codec, Incoming, Outgoing> Stream for MyTransport<Codec, Incoming, Outgoing> {
    type Item = Result<Incoming, MyError>;
    // ... poll_next ...
}
```

The stream must return `None` to signal that the transport is closed (regardless of cause).

### FusedStream

Implementing `FusedStream` is not strictly required but recommended.

```rust
impl<Codec, Incoming, Outgoing> FusedStream for MyTransport<Codec, Incoming, Outgoing> {
    fn is_terminated(&self) -> bool {
        // todo
    }
}
```

### Logging support

Conditionally implement `dnet_base::Logging` behind a "logging" feature in your crate.

See `dnet` transport implementations for reference of what to log, how and where.

Example:

```rust
pub struct MyTransport<Codec, Incoming, Outgoing> {
    // ...

    #[cfg(feature = "logging")]
    logger: dnet::Logger,
}

impl<Codec, Incoming, Outgoing> MyTransport<Codec, Incoming, Outgoing> {
    pub fn new(/* ... */) -> Self {
        WebSocketTransport {
            // ...

            #[cfg(feature = "logging")]
            logger: dnet::Logger::new::<Self>(),
        }
    }
}

// ... todo: remember to log in your poll methods 

#[cfg(feature = "logging")]
impl<Codec, Incoming, Outgoing> dnet_base::Logging for MyTransport<Codec, Incoming, Outgoing> {
    const KIND: &'static str = "MyTransport";

    fn with_logger<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&dnet_base::Logger) -> R,
    {
        f(&self.logger)
    }

    fn with_logger_mut<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut dnet_base::Logger) -> R,
    {
        f(&mut self.logger)
    }
}
```

## Testing transports

Transports may be tested using the [`dnet-tests`](https://docs.rs/dnet-tests/latest/dnet_tests/) crate.
It provides helpers such as `dnet_tests::test_transport`, `dnet_tests::test_unit_message`, and `dnet_tests::test_stream` for validating that your transport correctly sends, receives, and closes.

You can use `dnet_tests::dtest_configure!()` and `#[dtest]` attribute macro to mark your tests in your test modules when your transport supports both: native and WASM targets.

## Common mistakes

- Transport not closing on drop. Some underlying transports require an explicit `Drop` implementation to flush or close resources correctly.
- Returning `Result<Incoming, dnet::Error<TransportError>>` from the stream. The stream item should be `Result<Incoming, TransportError>` and the closing of the transport is notified by returning `None`.
- Returning `Poll::Pending` too early. If the underlying transport returned `Poll::Ready`, but you can't construct (and return) a message yet, keep polling in a loop until you can construct a concrete return value - return `Poll::Pending` only when underlying transport returns `Poll::Pending` (or you track [waking](https://doc.rust-lang.org/std/task/struct.Waker.html) with some other mechanism).
- Forgetting to implement `Logging`. When the `logging` feature is enabled, the transport should implement `dnet_base::Logging` or it may not work correctly with other `dnet` features.
- Implementing `Logging` trait but forgetting to actually log anything.
