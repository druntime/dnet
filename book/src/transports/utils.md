# `dnet` utilities

`dnet` comes with the following set of utilities (under "utils" feature, enabled by default):

- [`Filtering`](https://docs.rs/dnet-utils/latest/dnet_utils/filter/struct.Filtering.html) — wraps a transport and drops incoming or outgoing messages based on a predicate.
- [`Mapping`](https://docs.rs/dnet-utils/latest/dnet_utils/map/struct.Mapping.html) — adapts a transport to a different message type by mapping outgoing messages and unmapping incoming ones.
- [`Split2`](https://docs.rs/dnet-utils/latest/dnet_utils/split/trait.Split2.html), [`Split3`](https://docs.rs/dnet-utils/latest/dnet_utils/split/trait.Split3.html), [`Split4`](https://docs.rs/dnet-utils/latest/dnet_utils/split/trait.Split4.html), and [`Split5`](https://docs.rs/dnet-utils/latest/dnet_utils/split/trait.Split5.html) — [split](https://docs.rs/dnet-utils/latest/dnet_utils/split/index.html) a transport carrying a tagged message enum into multiple transports with different message types.
- [`Unwrapping`](https://docs.rs/dnet-utils/latest/dnet_utils/unwrap/trait.Unwrapping.html) — converts incoming messages that implement [`Unwrap`](https://docs.rs/dnet-utils/latest/dnet_utils/unwrap/trait.Unwrap.html) into their inner payloads.
- [`Latest`](https://docs.rs/dnet-utils/latest/dnet_utils/latest/struct.Latest.html) — wraps a numbered transport and only exposes the newest messages, discarding older ones.
- [`ChannelTransport`](https://docs.rs/dnet-utils/latest/dnet_utils/channel/struct.ChannelTransport.html) — transport over [futures](https://github.com/rust-lang/futures-rs) channels, useful for testing and debugging. Can be conveniently created with [`transports()`](https://docs.rs/dnet-utils/latest/dnet_utils/channel/fn.transports.html) function.
- [`MergedTransport`](https://docs.rs/dnet-utils/latest/dnet_utils/merge/struct.MergedTransport.html) — merges a [sink](https://docs.rs/futures/latest/futures/sink/trait.Sink.html) and [stream](https://docs.rs/futures/latest/futures/stream/trait.Stream.html) into a single transport.
- [`Numbered`](https://docs.rs/dnet-utils/latest/dnet_utils/number/struct.Numbered.html) — wraps a transport and adds sequence numbers to messages.
- [`Wall`](https://docs.rs/dnet-utils/latest/dnet_utils/wall/struct.Wall.html) — a transport that rejects every operation, returning an error on send, receive, flush, or close.
- [`Void`](https://docs.rs/dnet-utils/latest/dnet_utils/void/struct.Void.html) — a transport that accepts outgoing messages (but "sends" them into a 'void') and never produces incoming ones.

And:

- [`Pipe`](https://docs.rs/dnet-utils/latest/dnet_utils/pipe/struct.Pipe.html) — connects two transports in both directions so messages can flow back and forth. It is not a transport itself; it is a helper for linking two transports together.
