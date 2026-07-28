//! Transport for communication over
//! [WebSocket](https://developer.mozilla.org/en-US/docs/Web/API/WebSocket).
//!
//! Provides implementations for:
//! - browsers,
//! - native applications (through [tokio-tungstenite](https://github.com/snapview/tokio-tungstenite)),
//! - [axum](https://github.com/tokio-rs/axum) servers.

#[cfg(not(target_arch = "wasm32"))]
pub mod axum;

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
pub use native::*;

#[cfg(target_arch = "wasm32")]
mod wasm;
#[cfg(target_arch = "wasm32")]
pub use wasm::*;
