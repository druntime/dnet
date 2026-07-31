#![warn(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! Transport-related utilities.

use dportable::create_non_sync_send_variant_for_wasm;

#[cfg(feature = "channel")]
pub mod channel;

#[cfg(feature = "pipe")]
pub mod pipe;

#[cfg(feature = "merged")]
pub mod merge;

#[cfg(feature = "filtering")]
pub mod filter;

#[cfg(feature = "mapping")]
pub mod map;

#[cfg(feature = "unwrapping")]
pub mod unwrap;

#[cfg(feature = "splitting")]
pub mod split;

#[cfg(feature = "numbered")]
pub mod number;

#[cfg(feature = "latest")]
pub mod latest;

#[cfg(feature = "void")]
pub mod void;

#[cfg(feature = "wall")]
pub mod wall;

create_non_sync_send_variant_for_wasm! {
    /// [Send] on native targets, empty trait on WASM.
    ///
    /// Helper trait.
    pub trait ConditionalSend: Send {}
    impl<T> ConditionalSend for T where T: Send {}
}
