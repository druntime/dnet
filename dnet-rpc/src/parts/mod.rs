//! Parts used internally to build RPC infrastructure for an API.
//!
//! Used inside macros.
//!
//! As a user of `dnet-rpc` you shouldn't need to use anything from here.

pub mod consumer;
pub mod producer;

pub use dnet_utils::channel::transports;
pub use dnet_utils::pipe::Pipe;
