# WebRTC data channel

The [WebRTC](https://webrtc.org/) [data channel](https://developer.mozilla.org/en-US/docs/Web/API/WebRTC_API/Using_data_channels) transport lets peers exchange arbitrary messages using WebRTC data channels. It provides a peer-to-peer messaging option that works both in browser environments and natively, enabling low-latency, direct connections between endpoints.

Beyond the core transport, the WebRTC support in this project includes higher-level features that simplify real-world use: 
- built-in handling of signaling so peers can be connected with minimal setup,
- server- and client-side connection management to coordinate multiple peer connections through a shared signaling channel,
- an option for manual (custom) signaling when you need tighter control; and automatic and robust ICE candidate negotiation.

The implementation supports both browser and native runtimes.

> [!NOTE]
> The WebRTC data channel transport is part of the `dnet-webrtc` offering and is available under a commercial license.<br>
> The commercial package includes full source access, production-focused support, and example applications illustrating common deployment patterns.
> 
> See `dnet` [README](https://github.com/druntime/dnet/blob/main/README.md) for more information.
