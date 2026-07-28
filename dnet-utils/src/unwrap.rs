//! Unwrapping incoming messages.

#![allow(clippy::type_complexity)]

#[cfg(feature = "logging")]
use dnet_base::Logging;

use super::map::{Map, Mapping};

/// Trait implemented by messages that can be unwrapped.
pub trait Unwrap {
    /// Unwrapped message type.
    type Output;

    /// Unwrap message.
    fn unwrap(self) -> Self::Output;
}

/// Transforming transport into one unwrapping incoming messages.
pub trait Unwrapping<Incoming, Outgoing, Error>:
    dnet_base::Transport<Incoming, Outgoing, Error> + Sized + Unpin
where
    Incoming: Unwrap,
    Error: std::error::Error,
{
    /// Convert transport into one unwrapping incoming messages.
    fn unwrapping(
        self,
    ) -> Mapping<
        Self,
        Incoming,
        Outgoing,
        fn(Incoming) -> <Incoming as Unwrap>::Output,
        fn(Outgoing) -> Outgoing,
        Error,
    > {
        #[allow(unused_mut)]
        let mut transport: Mapping<
            Self,
            Incoming,
            Outgoing,
            fn(Incoming) -> <Incoming as Unwrap>::Output,
            fn(Outgoing) -> Outgoing,
            Error,
        > = self.unmap(Unwrap::unwrap);

        #[cfg(feature = "logging")]
        transport.with_logger_mut(|logger| logger.override_kind_with_str("Unwrapping"));

        transport
    }
}

impl<T, Incoming, Outgoing, Error> Unwrapping<Incoming, Outgoing, Error> for T
where
    T: dnet_base::Transport<Incoming, Outgoing, Error> + Unpin,
    Incoming: Unwrap,
    Error: std::error::Error,
{
}
