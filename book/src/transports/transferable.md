# Transferable transport

[`TransferableTransport`](https://docs.rs/dnet-js/latest/dnet_js/transferable/struct.TransferableTransport.html) is a `dnet` transport for browser `postMessage` targets that need to transfer both data and [transferable objects](https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API/Transferable_objects).

It wraps a target implementing [`PostMessage`](https://docs.rs/dnet-js/latest/dnet_js/trait.PostMessage.html) such as:

- [`web_sys::Worker`](https://docs.rs/web-sys/latest/web_sys/struct.Worker.html)
- [`web_sys::DedicatedWorkerGlobalScope`](https://docs.rs/web-sys/latest/web_sys/struct.DedicatedWorkerGlobalScope.html)
- [`web_sys::MessagePort`](https://docs.rs/web-sys/latest/web_sys/struct.MessagePort.html)

`TransferableTransport` works with messages that implement the [`IntoTransferable`](https://docs.rs/dnet-js/latest/dnet_js/transferable/trait.IntoTransferable.html) / [`FromTransferable`](https://docs.rs/dnet-js/latest/dnet_js/transferable/trait.FromTransferable.html) conversion traits:

- `IntoTransferable<Context, Error>` first converts outgoing messages into an (arbitrary) type implementing [`Transferable`](https://docs.rs/dnet-js/latest/dnet_js/transferable/trait.Transferable.html) trait.
- `Transferable` trait implementation is then responsible for preparing message for transfer through the transport by converting it into [`WithTransferable`](https://docs.rs/dnet-js/latest/dnet_js/transferable/struct.WithTransferable.html) type, which contains both the serialized message data (packed into JS object) and an JS array of transferable objects.
- For incoming messages (on the receiving end), `Transferable` trait implementation is responsible for reconstructing the original message from the received JS object (handling of transferable objects is done automatically by the browser).
- `FromTransferable<Context, Error>` reconstructs incoming messages from the (arbitrary) `Transferable` type.

> [!NOTE]
> `Transferable` trait uses arbitrary `Context` (passed by the user to `TransferableTransport` during construction).<br>
> It may be used to store codec, works buffers, etc., required for message serialization and deserialization.

## When to use

Use `TransferableTransport` when you need to send browser [transferable objects](https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API/Transferable_objects), like:

- [`ArrayBuffer`](https://docs.rs/js-sys/latest/js_sys/struct.ArrayBuffer.html)
- [`OffscreenCanvas`](https://docs.rs/web-sys/latest/web_sys/struct.OffscreenCanvas.html)
- [`MessagePort`](https://docs.rs/web-sys/latest/web_sys/struct.MessagePort.html)

If you only need serializable messages - without transferable objects - prefer [`dnet::js::Transport`](https://docs.rs/dnet-js/latest/dnet_js/struct.Transport.html) or its more specialized variants, such as [`MessagePortTransport`](https://docs.rs/dnet/latest/wasm32-unknown-unknown/dnet/message_port/struct.MessagePortTransport.html) or [`WebWorkerTransport`](https://docs.rs/dnet/latest/wasm32-unknown-unknown/dnet/webworker/struct.WebWorkerTransport.html).

> [!NOTE]
> When directly using this or `dnet::js::Transport` over a `MessagePort`, remember to call [`port.start()`](https://docs.rs/web-sys/latest/web_sys/struct.MessagePort.html#method.start) on both sides before sending messages.

## Implementing `IntoTransferable` and `FromTransferable`

Implementing `IntoTransferable`, `FromTransferable` and `Transferable` manually is straightforward, but can be tedious.

Use [`#[derive(IntoTransferable)]`](https://docs.rs/dnet-js/latest/dnet_js/derive.IntoTransferable.html) to implement those traits automatically:

```rust
use js_sys::ArrayBuffer;
use dnet::js::{IntoTransferable, Transferable};

#[derive(IntoTransferable)]
struct Message {
    some_field: String,

    #[transferable]
    buffer: ArrayBuffer,
}
```

Transferable fields must be marked with `#[transferable]`.

> [!NOTE]
> Generated `IntoTransferable` implementation expects you to use [`dnet::js::wrapper::Context`](https://docs.rs/dnet-js/latest/dnet_js/wrapper/struct.Context.html) with your transport.<br>
> If you need to use a different context type, implement `IntoTransferable` and `FromTransferable` manually.

### Nested `IntoTransferable`s

Messages deriving `IntoTransferable` can contain nested fields that also implement `IntoTransferable`.

Mark such fields with `#[into_transferable]`:

```rust
use js_sys::ArrayBuffer;
use dnet::js::IntoTransferable;

#[derive(IntoTransferable)]
struct Nested {
    #[transferable]
    buffer: ArrayBuffer,
}

#[derive(IntoTransferable)]
struct Message {
    some_field: String,

    #[into_transferable]
    nested: Nested,
}
```

> [!NOTE]
> For convenience `IntoTransferable` is implemented for typed JS [typed array](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/TypedArray) types - see `IntoTransferable`'s [foreign impls](https://docs.rs/dnet-js/latest/dnet_js/transferable/trait.IntoTransferable.html#foreign-impls).
>
> Example:
> ```rust
> use js_sys::Uint16Array;
> use dnet::js::IntoTransferable;
>
> #[derive(Debug, IntoTransferable)]
> struct Message {
>      #[into_transferable]
>      array: Uint16Array,
> }
> ```

> [!WARNING]
> `#[derive(IntoTransferable)]` macro will generate extra structures named:
> - `<YourTypeName>Wrapper`,
> - `<YourTypeName>Stripped`,
> - `<YourTypeName>IntoTransferables` (only if nested `#[into_transferable]` fields are present),
> 
> where `<YourTypeName>` is the name of your message.
> 
> If name collision(s) occurs, consider putting your message type into a separate module or - if not possible - implementing `IntoTransferable` and `FromTransferable` manually.

## Example

The following example demonstrates how a host and a worker can exchange messages with transferable objects using `TransferableTransport`:

```rust
use futures::SinkExt;
use js_sys::{ArrayBuffer, Uint8Array};
use std::rc::Rc;
use web_sys::{MessageChannel, Worker, WorkerOptions, WorkerType};

use dnet::{
    codecs::BincodeCodec,
    js::{wrapper::Context, IntoTransferable, TransferableTransport},
    Receive,
};

#[derive(IntoTransferable)]
struct Message {
    #[transferable]
    buffer: ArrayBuffer,
}

async fn host_example() -> Result<(), Box<dyn std::error::Error>> {
    let options = WorkerOptions::new();
    options.set_type(WorkerType::Module);
    let worker = Worker::new_with_options("./worker.js", &options)?;

    let context = Context::new(BincodeCodec::default());
    let mut transport = TransferableTransport::<_, _, Message, Message, _>::new(
        &worker,
        None,
        context,
        true,
    )
    .await?;

    let buffer = ArrayBuffer::new_with_length(8);
    Uint8Array::new(&buffer).copy_from(&[1, 2, 3, 4, 5, 6, 7, 8]);

    transport.send(Message { buffer }).await?;
    Ok(())
}

async fn worker_example(
    global: Rc<web_sys::DedicatedWorkerGlobalScope>,
) -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new(BincodeCodec::default());
    let mut transport = TransferableTransport::<_, _, Message, Message, _>::new(
        &global,
        None,
        context,
        false,
    )
    .await?;

    let received = transport.receive().await?;
    let Message { buffer } = received;
    assert_eq!(Uint8Array::new(&buffer).to_vec(), vec![1, 2, 3, 4, 5, 6, 7, 8]);

    Ok(())
}
```
