#![warn(missing_docs)]

//! Message passing infrastructure.
//!
//! See [repository](https://github.com/druntime/dnet) for more info.

pub use dnet_base::*;

#[cfg(feature = "codecs")]
pub use dnet_codecs as codecs;

#[cfg(feature = "utils")]
pub use dnet_utils as utils;

#[cfg(feature = "rpc")]
pub use dnet_rpc as rpc;

#[cfg(all(feature = "io", not(target_arch = "wasm32")))]
pub mod io;

#[cfg(all(feature = "tcp", not(target_arch = "wasm32")))]
pub mod tcp;

#[cfg(all(feature = "udp", not(target_arch = "wasm32")))]
pub mod udp;

#[cfg(all(feature = "quic", not(target_arch = "wasm32")))]
pub mod quic;

#[cfg(target_arch = "wasm32")]
pub use dnet_js as js;

#[cfg(feature = "websocket")]
pub mod websocket;

#[cfg(all(feature = "message_port", target_arch = "wasm32"))]
pub mod message_port;

#[cfg(all(feature = "webworker", target_arch = "wasm32"))]
pub mod webworker;
