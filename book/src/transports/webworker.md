# Web Worker transport

[`WebWorkerTransport`](https://docs.rs/dnet/0.1.4/wasm32-unknown-unknown/dnet/webworker/struct.WebWorkerTransport.html) is used for communication between a host page (or host worker) and a Web Worker.
It uses the worker's [`postMessage`](https://developer.mozilla.org/en-US/docs/Web/API/Worker/postMessage) method and [`onmessage`](https://developer.mozilla.org/en-US/docs/Web/API/Worker/onmessage) events (and their equivalents in worker context - [`DedicatedWorkerGlobalScope.postMessage`](https://developer.mozilla.org/en-US/docs/Web/API/DedicatedWorkerGlobalScope/postMessage) and [`DedicatedWorkerGlobalScope.onmessage`](https://developer.mozilla.org/en-US/docs/Web/API/DedicatedWorkerGlobalScope/onmessage)) to exchange messages.

- [`WebWorkerTransport::new(worker, codec).await`](https://docs.rs/dnet/0.1.4/wasm32-unknown-unknown/dnet/webworker/struct.WebWorkerTransport.html#method.new) creates a transport for the worker on the host side.
- [`WebWorkerTransport::new_with_name(worker, codec, name).await`](https://docs.rs/dnet/0.1.4/wasm32-unknown-unknown/dnet/webworker/struct.WebWorkerTransport.html#method.new_with_name) creates a named transport for the worker on the host side.
- [`WebWorkerTransport::new_in_worker(codec).await`](https://docs.rs/dnet/0.1.4/wasm32-unknown-unknown/dnet/webworker/struct.WebWorkerTransport.html#method.new_in_worker) creates a transport inside the worker.
- [`WebWorkerTransport::new_with_name_in_worker(codec, name).await`](https://docs.rs/dnet/0.1.4/wasm32-unknown-unknown/dnet/webworker/struct.WebWorkerTransport.html#method.new_with_name_in_worker) creates a named transport inside the worker.

> [!NOTE]
> `new_in_worker` and `new_with_name_in_worker` must be called inside a worker context, [`Error::NotInWorker`](https://docs.rs/dnet/0.1.4/wasm32-unknown-unknown/dnet/webworker/type.Error.html#variant.NotInWorker) is returned otherwise.

Named transports are useful when the host or worker needs multiple channels.
Multiple named transports can be used simultaneously alongside an unnamed transport in the same page or worker.

Workers can themselves host nested workers and use `WebWorkerTransport::new(...)` from within a worker context to create transports to child workers.

This transport is appropriate when you want a direct worker-host channel. If you need to use a dedicated [MessagePort](https://developer.mozilla.org/en-US/docs/Web/API/MessagePort) or send [transferable](https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API/Transferable_objects) objects, see the [MessagePort](./message-port.md) and [Transferable](./transferable.md) transport sections.

## Host example

```rust
use futures::SinkExt;
use dnet::{codecs::BincodeCodec, webworker::WebWorkerTransport, Receive};
use web_sys::{Worker, WorkerOptions, WorkerType};

let options = WorkerOptions::new();
options.set_type(WorkerType::Module);
let worker = Worker::new_with_options("./worker.js", &options).unwrap();

let mut transport = WebWorkerTransport::new(worker, BincodeCodec::default()).await?;

transport.send("hello from host".to_string()).await?;
let reply: String = transport.receive().await?;
```

## Worker example

```rust
use futures::SinkExt;
use dnet::{codecs::BincodeCodec, webworker::WebWorkerTransport, Receive};

let mut transport = WebWorkerTransport::new_in_worker(BincodeCodec::default()).await?;

let request: String = transport.receive().await?;
transport.send(format!("hello from worker: {}", request)).await?;
```
