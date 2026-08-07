# Remote Procedure Call

`dnet`'s [`rpc`](https://docs.rs/dnet-rpc/0.1.1/dnet_rpc/) module (enabled with the "rpc" feature, which is enabled by default) provides a type-safe implementation of Remote Procedure Calls ([RPC](https://en.wikipedia.org/wiki/Remote_procedure_call)) that works over any transport supported by `dnet`.

## What is RPC

Remote Procedure Call (RPC) allows code running in one execution context to invoke functions that execute in another context as if they were ordinary local function calls.

The two sides may be running:

* in different processes,
* on different computers connected over a network,
* in different browser tabs,
* inside Web Workers,
* or on any other pair of peers connected through a `dnet` transport.

With `dnet`, you define your API once as a Rust trait, implement it on one side, and obtain a strongly typed client on the other. Serialization, message routing, request/response matching, and asynchronous communication are handled automatically.

## Steps

Implementing an RPC service consists of a few simple steps:

1. Define your service interface (see [API](./api.md) section).
2. Implement a producer for your service (see [Producing](./producing.md) section).
3. Establish a connection between two peers using any supported `dnet` transport.
4. On the server side, register the service by calling [`produce(...)`](https://docs.rs/dnet-rpc/0.1.1/dnet_rpc/producer/trait.Produce.html#tymethod.produce).
5. On the client side, create a typed client by calling [`consume(...)`](https://docs.rs/dnet-rpc/0.1.1/dnet_rpc/consumer/trait.Consume.html#tymethod.consume).

Once the connection is established, you simply call methods on the generated consumer (see [Consuming](./consuming.md) section). The RPC layer transparently serializes arguments, sends requests over the selected transport, executes the corresponding server-side method, and returns the result.

## Example

For a complete example, see the [RPC example](https://github.com/druntime/dnet/tree/main/dnet/examples/rpc) in the `dnet` repository.
