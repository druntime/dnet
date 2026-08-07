# API

The first step when using `dnet` RPC is defining your service API.

An API is a Rust trait that describes the remote methods exposed by a producer (server) and available to a consumer (client). The trait acts as the single source of truth for your RPC interface - `dnet` generates all request/response types and client/server glue code from it.

## Defining the API trait

Use [`#[api]`](https://docs.rs/dnet-rpc/0.1.1/dnet_rpc/attr.api.html) marker on a trait that describes your remote service interface:

```rust
// ...

use dnet::rpc::{api, no_ack};

#[api]
pub trait Api {
    /// Print "Hello World!" message on the server.
    #[no_ack]
    async fn hello_world(&self);

    /// Add two numbers together.
    async fn add_numbers(&self, a: i32, b: i32) -> i32;

    /// Concatenate two strings together.
    async fn concatenate_strings(&self, a: String, b: String) -> String;

    /// Repeatedly send server time at a given interval.
    async fn stream_time(&self, interval: Duration) -> impl Stream<Item = NaiveTime>;
}
```

The macro generates the following items:

- `Request` enum - serializable request payloads for each method
- `Response` enum - serializable response payloads and stream items
- `Consumer` struct - client-side API for making RPC calls
- `impl_produce` macro - producer-side helper for wiring your implementation to the RPC runtime

> [!WARNING]
> The generated types (`Request`, `Response`, `Consumer`, and `impl_produce`) have fixed names. Because of this, defining multiple `#[api]` traits in the same Rust module will result in name collisions.
>
> Define each API trait in its own module.

## Method types

The generated API understands several kinds of methods:

- ordinary asynchronous request/response methods
- methods marked with `#[no_ack]` are fire-and-forget and do not wait for a response
- async methods that return `impl Stream<Item = T>` create a stream from producer to consumer
- methods marked with `#[abortable]` use an `AbortionToken` argument on the producer side that can be used to react to abort event triggered on the client-side

### Fire-and-forget methods

Annotate a method with `#[no_ack]` marker when the consumer should not wait for a response.
The producer receives the request, but it does not send a response back.

### Streaming methods

If a method returns `impl Stream<Item = T>`, the generated RPC consumer will receive a stream handle when the request is made.
Values produced by the server are sent to the consumer until the stream closes or is aborted.

### Aborting requests

The `#[abortable]` attribute adds an `AbortionToken` to the producer method signature.
The consumer can abort a request or stream, and the token allows the producer to react to that event.

## Serialization and `no_serde`

By default, generated `Request` and `Response` enums derive `Serialize` and `Deserialize` using `serde`.
If you need to disable serialization for one or both enums, use the [`#[no_serde]`](https://docs.rs/dnet-rpc/0.1.1/dnet_rpc/attr.no_serde.html) attribute.

The `#[no_serde]` macro can be used as:

- `#[no_serde]` - disable serialization for both request and response
- `#[no_serde(request)]` - disable (de)serialization for request only
- `#[no_serde(response)]` - disable (de)serialization for response only
- `#[no_serde(request_serialize)]` - disable request serialization only
- `#[no_serde(request_deserialize)]` - disable request deserialization only
- `#[no_serde(response_serialize)]` - disable response serialization only
- `#[no_serde(response_deserialize)]` - disable response deserialization only

This is useful when used with arguments/return types that are not serializable and RPC is meant to be used over `dnet` transports that don't require serialization before sending messages or message encoding/decoding is meant to be implemented manually, using non-`serde` scheme.

## Transferable objects

Use the `#[transferable]` and `#[into_transferable]` attributes when your API needs to use browser [transferable objects](https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API/Transferable_objects) (your RPC service is meant to be used over a [`TransferableTransport`](../transports/transferable.md)).

Attributes can be applied to individual function arguments.

Because Rust does not allow placing attributes directly on a return type, mark the whole method with `#[transferable]` or `#[into_transferable]` when you intend the return value (or stream items) to be transferable. 

> [!NOTE]
> `#[transferable]` is not supported for stream responses - use `#[into_transferable]` for streaming methods.

When any `#[transferable]` / `#[into_transferable]` attribute is used in an `#[api]` trait, the API generation automatically switches to the transferable serialization mode. The generated `Request` and `Response` enums derive [`IntoTransferable`](https://docs.rs/dnet-js/latest/dnet_js/derive.IntoTransferable.html) and the appropriate fields are annotated (for example `#[transferable]` on transferable fields and `#[into_transferable]` on nested/into-converted fields).

Because transferable mode uses `IntoTransferable` conversions instead of `serde`, use of these attributes implies `#[no_serde]` for the API.

This feature is intended for use with the [transferable transport](../transports/transferable.md) - see its documentation for more details.

Example API using transferable attributes:

```rust
use dnet_rpc::api;
use js_sys::{ArrayBuffer, Float32Array, Uint32Array, Uint8Array};
use futures::Stream;

#[api]
pub trait Api {
    #[no_ack]
    async fn test_no_ack(&self, #[transferable] some_array_buffer: ArrayBuffer);

    async fn add(&self, #[into_transferable] floats: Float32Array) -> f32;

    #[transferable]
    async fn get_some_transferable(&self) -> ArrayBuffer;

    #[into_transferable]
    async fn stream(&self) -> impl Stream<Item = Uint32Array>;
}
```

Generated request/response enums (transferable mode) derive `IntoTransferable` and annotate fields appropriately. For example:

```rust
#[derive(Debug, Clone, ::dnet_js::IntoTransferable)]
pub enum Request {
    SendTransferable { #[transferable] data: OffscreenCanvas },
    GetData {},
}

#[derive(Debug, Clone, ::dnet_js::IntoTransferable)]
pub enum Response {
    SendTransferable(()),
    GetData(#[into_transferable] Data),
}
```

## Where to go next

Read the [Producing](./producing.md) page for the producer implementation details, and the [Consuming](./consuming.md) page for the client-side consumption of the API.
