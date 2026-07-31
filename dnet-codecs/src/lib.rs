#![warn(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! Message codecs.

#[cfg(feature = "bincode")]
pub mod bincode;
#[cfg(feature = "bincode")]
pub use bincode::BincodeCodec;

#[cfg(feature = "json")]
pub mod json;
#[cfg(feature = "json")]
pub use json::JsonCodec;
