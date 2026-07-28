#![warn(missing_docs)]

//! Message codecs.

#[cfg(feature = "bincode")]
pub mod bincode;
#[cfg(feature = "bincode")]
pub use bincode::BincodeCodec;

#[cfg(feature = "json")]
pub mod json;
#[cfg(feature = "json")]
pub use json::JsonCodec;
