//! Producer related functionality for abortable requests.

use std::future::Future;

use dportable::{CancellationToken, Cancelled, CancelledOwned};
use serde::{Deserialize, Serialize};

/// Result of an abortable request.
pub type Result<T> = std::result::Result<T, Aborted>;

/// Helper struct for abortable request producer-side handlers.
///
/// It is used as return value for request handlers that were aborted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Aborted;

/// Token triggering task abortion.
///
/// Used internally by producers.
pub type AborterToken = CancellationToken;

/// Child of [AbortionToken] created with [AbortionToken::child_token] method.
///
/// It can be cloned as opposed to [AbortionToken] which is not cloneable.
pub type AbortionChildToken = CancellationToken;

/// Future returned by [AbortionToken::aborted] method.
pub type AbortedFuture<'a> = Cancelled<'a>;

/// Future returned by [AbortionToken::aborted_owned] method.
pub type AbortedFutureOwned = CancelledOwned;

/// Abortion token for producers.
///
/// Signifies that consumer has aborted the request.
#[derive(Debug)]
pub struct AbortionToken(AborterToken);

impl AbortionToken {
    /// Creates new abortion token.
    pub fn new(token: AborterToken) -> Self {
        Self(token)
    }

    /// Creates new token that is a child of this token.
    pub fn child_token(&self) -> AbortionChildToken {
        self.0.child_token()
    }

    /// Checks if the request was aborted by the consumer.
    pub fn is_aborted(&self) -> bool {
        self.0.is_cancelled()
    }

    /// Returns a future that resolves when the request is aborted by the consumer.
    pub fn aborted(&self) -> AbortedFuture<'_> {
        self.0.cancelled()
    }

    /// Returns an owned future that resolves when the request is aborted by the consumer.
    pub fn aborted_owned(self) -> AbortedFutureOwned {
        self.0.cancelled_owned()
    }

    /// Runs the provided future until it is aborted by the consumer.
    pub async fn run_until_aborted<F>(&self, fut: F) -> Option<F::Output>
    where
        F: Future,
    {
        self.0.run_until_cancelled(fut).await
    }

    /// Runs the provided future until it is aborted by the consumer, consuming the token.
    pub async fn run_until_aborted_owned<F>(self, fut: F) -> Option<F::Output>
    where
        F: Future,
    {
        self.0.run_until_cancelled_owned(fut).await
    }
}
