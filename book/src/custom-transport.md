# Writing a custom `dnet` transport

TODO:

Note what has to be done to create transport
- implement futures::Sink
- implement futures::Stream
- its recommended to also implement FusedStream
- implement (conditionally, under "logging" feature) Logging trait - note that methods from dnet::logging may be used - usually its a matter of calling the right one in the right place - see `dnet` transports for reference

transports may be tested using `dnet-test` crate.

## Common mistakes
- Transport not closing on drop - some underlying implementations may require you to implement `Drop` trait for your transport struct
- Using `Result<Incoming, dnet::Error<TransportError>>` as stream item, instead of `Result<Incoming, TransportError>` - closed transport for streams is denoted by returning `None`
- returning `Poll::Pending` in poll methods when underlying transport returns `Poll::Ready`, but we don't have enough information yet to construct a return value of our poll method - always remember to poll underlying transport in a loop and only break out when constructing a return value is possible or `Poll::Pending` is returned
- forgetting about implementing Logging - while its not strictly required, transport won't work with other `dnet` features if its "logging" feature is enabled