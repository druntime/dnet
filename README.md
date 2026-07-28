# dnet

[![Test Status](https://github.com/druntime/dnet/actions/workflows/rust.yml/badge.svg)](https://github.com/druntime/dnet/actions)
[![Crate](https://img.shields.io/crates/v/dnet.svg)](https://crates.io/crates/dnet)
[![API](https://docs.rs/dnet/badge.svg)](https://docs.rs/dnet)

**dnet** is a unified messaging abstraction library for Rust that allows sending and receiving messages over a variety of transport protocols with a consistent, easy-to-use async interface.

https://crates.io/crates/dnet

## Usage

See [examples](https://github.com/druntime/dnet/tree/master/dnet/examples). 

## Supported Transports

| Transport             |    Native    |    Browser    | Description                                                                                                                                    |
|:----------------------|:------------:|:-------------:|:-----------------------------------------------------------------------------------------------------------------------------------------------|
| TCP                   | ✅            | *n/a*        | Transport over [Tokio](https://tokio.rs/) TCP implementation.                                                                                  |
| UDP                   | ✅            | *n/a*        | Transport over [Tokio](https://tokio.rs/) UDP implementation.                                                                                  |
| QUIC                  | ✅            | *n/a*        | Transport over [Quinn](https://github.com/quinn-rs/quinn) QUIC implementation.                                                                 |
| MessagePort           | *n/a*        | ✅            | Transport over [MessagePort](https://developer.mozilla.org/en-US/docs/Web/API/MessagePort).                                                    |
| Web Worker            | *n/a*        | ✅            | Communication with [Web Workers](https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API/Using_web_workers).                          |
| WebSocket             | ✅            | ✅            | Transport over [WebSockets](https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API).                                                  |
| Channel               | ✅            | ✅            | Transport over [futures](https://github.com/rust-lang/futures-rs) channels.                                                                    |
| WebRTC data channel*  | ✅            | ✅            | Transport over [WebRTC](https://webrtc.org/) [data channels](https://developer.mozilla.org/en-US/docs/Web/API/WebRTC_API/Using_data_channels). |

Additionally any transport implementing [Tokio](https://tokio.rs/)'s 
[`AsyncRead`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncRead.html) 
and [`AsyncWrite`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncWrite.html) 
traits is also supported.

*available for licensing, not available in the free version of the library - see [WebRTC section](#webrtc-support).

## WebRTC Support

The `dnet-webrtc` crate provides robust WebRTC transport capabilities:

- Transport over WebRTC data channels,
- [WebRTC signaling](https://developer.mozilla.org/en-US/docs/Web/API/WebRTC_API/Signaling_and_video_calling) is handled automatically, both client and server-side - you simply decide which peers to connect together, the rest is done for you,
- Server and client-side connection management - handle signaling of multiple connections over a single shared transport to signaling server,
- Manual (custom) signaling is still an option, with minimal code changes, when needed,
- Automatic ICE candidate negotiation,
- Support for both browser and native environments.

### Licensing

The `dnet-webrtc` crate is available under a commercial license. The commercial version includes:

- Full source code access,
- Technical support,
- Production deployment assistance,
- Regular updates and maintenance,
- Pong clone peer-to-peer game example,
- Example demonstrating client-server architecture using WebRTC data channels to connect multiple client peers to a single host.

For licensing inquiries and pricing, please contact: [dzduniak@gmail.com](mailto:dzduniak@gmail.com?subject=dnet-webrtc%20License%20Inquiry&body=Hi%2C%0A%0AI%27m%20interested%20in%20acquiring%20a%20license%20for%20dnet-webrtc.%20Could%20you%20please%20provide%20information%20about%20licensing%20options%20and%20pricing%3F%0A%0ABest%20regards)

## License

This project is licensed under the MIT License.
