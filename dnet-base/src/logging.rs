//! Logging utilities for `dnet` transports.

use std::{any::type_name, task::Poll};

use tracing::{debug, error, info, trace, trace_span, warn, Span};

/// Trait implemented by `dnet` transports that support logging.
pub trait Logging {
    /// Short human-readable type of `dnet` the transport.
    const KIND: &'static str;

    /// Execute closure with transport's logger.
    fn with_logger<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Logger) -> R;

    /// Execute closure with transport's logger (mutable).
    fn with_logger_mut<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut Logger) -> R;

    /// Set transport name (for logging purposed).
    fn set_logging_name(&mut self, name: &str) {
        self.with_logger_mut(|logger| logger.set_transport_name(Some(name.to_string())));
    }

    /// Get transport name (for logging purposed).
    fn get_logging_name(&mut self) -> Option<String> {
        self.with_logger_mut(|logger| logger.get_transport_name().map(|name| name.to_string()))
    }

    /// Enable logging for this transport.
    fn enable_logging(&mut self) {
        self.with_logger_mut(|logger| logger.enable());
    }

    /// Disable logging for this transport.
    fn disable_logging(&mut self) {
        self.with_logger_mut(|logger| logger.disable());
    }

    /// Is logging enabled for this transport.
    fn is_logging_enabled(&self) -> bool {
        self.with_logger(|logger| logger.is_enabled())
    }
}

/// Logging utility for `dnet` transports.
pub struct Logger {
    kind: String,
    name: Option<String>,
    enabled: bool,
}

impl Logger {
    /// Create new logger for transport.
    pub fn new<T>() -> Self
    where
        T: Logging,
    {
        let kind = T::KIND.to_string();
        let name = None;
        let enabled = false;
        Logger {
            kind,
            name,
            enabled,
        }
    }

    /// Create new logger for transport that already has a name.
    ///
    /// Usually `dnet` transports don't have names, but when they do
    /// you may use this constructor to make transport's logical name the
    /// same as the one used for logging purposes.
    ///
    /// Note that logging name may still be changed later by the transport user
    /// and at that point logical name and logging name may be different.
    pub fn new_for_already_named<T>(transport_name: &str) -> Self
    where
        T: Logging,
    {
        let kind = T::KIND.to_string();
        let name = Some(transport_name.to_string());
        let enabled = false;
        Logger {
            kind,
            name,
            enabled,
        }
    }

    /// Log that the transport was opened successfully.
    ///
    /// Note: not all transports emit an "open" event - transports that do
    /// not wait for an explicit open should skip calling this.
    #[inline]
    pub fn log_open_success(&self) {
        if self.enabled {
            let span = self.span();
            let _guard = span.enter();
            info!("transport open");
        }
    }

    /// Log that opening the transport failed.
    ///
    /// Useful for transports that report an explicit open failure to the
    /// application. Transports that don't wait for opening can ignore this.
    #[inline]
    pub fn log_open_failure(&self, error: impl std::error::Error) {
        if self.enabled {
            let span = self.span();
            let _guard = span.enter();
            error!("opening transport failed: {error}");
        }
    }

    /// Helper method for logging readiness based on result.
    ///
    /// See [log_ready_success](Logger::log_ready_success) and
    /// [log_ready_failure](Logger::log_ready_failure) for more details.
    #[inline]
    pub fn log_ready<S, F>(&self, result: &Poll<Result<S, F>>)
    where
        F: std::error::Error,
    {
        match result {
            Poll::Ready(Ok(_)) => self.log_ready_success(),
            Poll::Ready(Err(error)) => self.log_ready_failure(error),
            _ => {} // ignore
        }
    }

    /// Log that the transport is ready for sending messages.
    ///
    /// Currently this does not log anything to keep the log less verbose, but it may be changed in the future.
    #[inline]
    pub fn log_ready_success(&self) {
        // ignore
    }

    /// Log that the transport failed to become ready for sending messages.
    #[inline]
    pub fn log_ready_failure(&self, error: impl std::error::Error) {
        if self.enabled {
            let span = self.span();
            let _guard = span.enter();
            error!("failed to become ready for sending: {error}");
        }
    }

    /// Helper method for logging message staging based on result.
    ///
    /// See [log_message_staging_success](Logger::log_message_staging_success) and
    /// [log_message_staging_failure](Logger::log_message_staging_failure) for more
    /// info.
    #[inline]
    pub fn log_message_staging<O, S, F>(&self, result: &Result<S, F>)
    where
        F: std::error::Error,
    {
        match result {
            Ok(_) => {
                self.log_message_staging_success::<O>();
            }
            Err(error) => self.log_message_staging_failure(error),
        }
    }

    /// Log successful staging of an outgoing message for sending.
    ///
    /// Used by transports that stage outgoing messages by putting them into buffer
    /// before encoding and sending.
    /// Not every transport stages messages before sending - this method may be unused.
    #[inline]
    pub fn log_message_staging_success<O>(&self) {
        if self.enabled {
            let span = self.span();
            let _guard = span.enter();
            trace!(
                message_type = type_name::<O>(),
                "staged message for sending"
            );
        }
    }

    /// Log failure while staging an outgoing message for sending.
    ///
    /// Used by transports that stage outgoing messages by putting them into buffer
    /// before encoding and sending.
    /// Not every transport stages messages before sending - this method may be unused.
    #[inline]
    pub fn log_message_staging_failure(&self, error: impl std::error::Error) {
        if self.enabled {
            let span = self.span();
            let _guard = span.enter();
            error!("failed to stage message for sending: {error}");
        }
    }

    /// Helper method for logging message preparation based on result.
    ///
    /// See [log_message_preparation_success](Logger::log_message_preparation_success) and
    /// [log_message_preparation_failure](Logger::log_message_preparation_failure) for more
    /// info.
    #[inline]
    pub fn log_message_preparation<O, S, F>(
        &self,
        send_buffer_len_before: usize,
        send_buffer_len_after: usize,
        result: &Result<S, F>,
    ) where
        F: std::error::Error,
    {
        match result {
            Ok(_) => {
                let message_size = send_buffer_len_after - send_buffer_len_before;
                self.log_message_preparation_success::<O>(Some(message_size));
            }
            Err(error) => self.log_message_preparation_failure(error),
        }
    }

    /// Log successful preparation of an outgoing message.
    ///
    /// Used by transports that buffer or prepare messages before sending (flushing).
    /// Not every transport buffers messages before sending.
    #[inline]
    pub fn log_message_preparation_success<O>(&self, size: Option<usize>) {
        if self.enabled {
            let span = self.span();
            let _guard = span.enter();
            if let Some(size) = size {
                trace!(
                    message_type = type_name::<O>(),
                    size_in_bytes = size,
                    "prepared message for sending"
                );
            } else {
                trace!(
                    message_type = type_name::<O>(),
                    "prepared message for sending"
                );
            }
        }
    }

    /// Log failure while preparing an outgoing message.
    ///
    /// Used by buffered transports that perform message preparation steps
    /// before sending (flushing); may be unused by simpler transports.
    #[inline]
    pub fn log_message_preparation_failure(&self, error: impl std::error::Error) {
        if self.enabled {
            let span = self.span();
            let _guard = span.enter();
            error!("failed to prepare message for sending: {error}");
        }
    }

    /// Helper method for logging message sending based on result.
    ///
    /// See [log_sending_success](Logger::log_sending_success) and
    /// [log_sending_failure](Logger::log_sending_failure) for more
    /// info.
    #[inline]
    pub fn log_sending<O, F>(&self, result: &Result<(), F>, size: Option<usize>)
    where
        F: std::error::Error,
    {
        match result {
            Ok(_) => self.log_sending_success::<O>(size),
            Err(error) => self.log_sending_failure(error),
        }
    }

    /// Log that a message was sent successfully.
    #[inline]
    pub fn log_sending_success<O>(&self, size: Option<usize>) {
        if self.enabled {
            let span = self.span();
            let _guard = span.enter();
            if let Some(size) = size {
                debug!(
                    message_type = type_name::<O>(),
                    size_in_bytes = size,
                    "message sent"
                );
            } else {
                debug!(message_type = type_name::<O>(), "message sent");
            }
        }
    }

    /// Log that sending a message failed.
    #[inline]
    pub fn log_sending_failure(&self, error: impl std::error::Error) {
        if self.enabled {
            let span = self.span();
            let _guard = span.enter();
            error!("failed to send message: {error}");
        }
    }

    /// Log that a message data arrived (was received and enqueued).
    ///
    /// This is used by transports that receive messages data in the background
    /// and place them into a buffer for later deserialization and
    /// consumption; not all transports will use arrival logs.
    ///
    /// It is called "unknown" because at this point we don't know
    /// if message deserialization will succeed or not.
    #[inline]
    pub fn log_message_arrived_unknown(&self, size: Option<usize>) {
        if self.enabled {
            let span = self.span();
            let _guard = span.enter();
            if let Some(size) = size {
                trace!(size_in_bytes = size, "new message arrived");
            } else {
                trace!("new message arrived");
            }
        }
    }

    /// Log that a message arrived (was received and enqueued).
    ///
    /// This is used by transports that receive messages in the background
    /// and place them into a buffer for later consumption; not all transports
    /// will use arrival logs.
    #[inline]
    pub fn log_message_arrived_success<I>(&self, _item: &I, size: Option<usize>) {
        if self.enabled {
            let span = self.span();
            let _guard = span.enter();
            if let Some(size) = size {
                trace!(
                    message_type = type_name::<I>(),
                    size_in_bytes = size,
                    "new message arrived"
                );
            } else {
                trace!(message_type = type_name::<I>(), "new message arrived");
            }
        }
    }

    /// Log failure during background arrival/enqueue of a received message.
    ///
    /// Relevant for transports that buffer incoming messages in the background;
    /// may be unused by transports that only receive message while precessing
    /// `next` method.
    #[inline]
    pub fn log_message_arrived_failure(&self, error: impl std::error::Error) {
        if self.enabled {
            let span = self.span();
            let _guard = span.enter();
            error!("failed to process arriving message: {error}");
        }
    }

    /// Helper method for logging receiving based on result.
    ///
    /// See [log_receiving_success](Logger::log_receiving_success),
    /// [log_receiving_failure](Logger::log_receiving_failure) and
    /// [log_remote_closed](Logger::log_remote_closed) for more info.
    #[inline]
    pub fn log_receiving<I, F>(
        &self,
        result: &Poll<Option<Result<I, F>>>,
        message_length: Option<usize>,
    ) where
        F: std::error::Error,
    {
        match result {
            Poll::Ready(Some(Ok(item))) => self.log_receiving_success(item, message_length),
            Poll::Ready(Some(Err(error))) => self.log_receiving_failure(error),
            Poll::Ready(None) => self.log_remote_closed(),
            _ => {} // ignore
        }
    }

    /// Log that a message was successfully received.
    #[inline]
    pub fn log_receiving_success<I>(&self, _item: &I, size: Option<usize>) {
        if self.enabled {
            let span = self.span();
            let _guard = span.enter();
            if let Some(size) = size {
                debug!(
                    message_type = type_name::<I>(),
                    size_in_bytes = size,
                    "received message"
                );
            } else {
                debug!(message_type = type_name::<I>(), "received message");
            }
        }
    }

    /// Log that receiving a message failed.
    #[inline]
    pub fn log_receiving_failure(&self, error: impl std::error::Error) {
        if self.enabled {
            let span = self.span();
            let _guard = span.enter();
            error!("failed to receive message: {error}");
        }
    }

    /// Log that an attempt to receive from a `Void` transport was made.
    pub fn log_receive_from_void(&self) {
        if self.enabled {
            let span = self.span();
            let _guard = span.enter();
            warn!("attempted to receive from void");
        }
    }

    /// Helper method for logging flush.
    ///
    /// See [log_flush_success](Logger::log_flush_success) and
    /// [log_flush_failure](Logger::log_flush_failure) for more
    /// info.
    #[inline]
    pub fn log_flush<S, F>(&self, result: &Poll<Result<S, F>>)
    where
        F: std::error::Error,
    {
        match result {
            Poll::Ready(Ok(_)) => self.log_flush_success(),
            Poll::Ready(Err(error)) => self.log_flush_failure(error),
            _ => {} // ignore
        }
    }

    /// Log that buffered messages were flushed successfully.
    #[inline]
    pub fn log_flush_success(&self) {
        if self.enabled {
            let span = self.span();
            let _guard = span.enter();
            debug!("outgoing messages flushed");
        }
    }

    /// Log that flushing buffered messages failed.
    #[inline]
    pub fn log_flush_failure(&self, error: impl std::error::Error) {
        if self.enabled {
            let span = self.span();
            let _guard = span.enter();
            error!("failed to flush outgoing messages: {error}");
        }
    }

    /// Helper method for logging closing of the transport based on result.
    ///
    /// See [log_closed_success](Logger::log_closed_success),
    /// [log_closed_failure](Logger::log_closed_failure) for more info.
    #[inline]
    pub fn log_close<S, F>(&self, result: &Poll<Result<S, F>>)
    where
        F: std::error::Error,
    {
        match result {
            Poll::Ready(Ok(_)) => self.log_closed_success(),
            Poll::Ready(Err(error)) => self.log_closed_failure(error),
            _ => {} // ignore
        }
    }

    /// Log that the transport was closed locally successfully.
    #[inline]
    pub fn log_closed_success(&self) {
        if self.enabled {
            let span = self.span();
            let _guard = span.enter();
            info!("transport closed");
        }
    }

    /// Log that closing the transport locally failed.
    #[inline]
    pub fn log_closed_failure(&self, error: impl std::error::Error) {
        if self.enabled {
            let span = self.span();
            let _guard = span.enter();
            error!("failed to close transport: {error}");
        }
    }

    /// Log that the remote peer closed the transport.
    #[inline]
    pub fn log_remote_closed(&self) {
        if self.enabled {
            let span = self.span();
            let _guard = span.enter();
            info!("remote transport closed");
        }
    }

    /// Log that an incoming message was filtered out.
    #[inline]
    pub fn log_incoming_filtered_out<I>(&self) {
        if self.enabled {
            let span = self.span();
            let _guard = span.enter();
            trace!(
                message_type = type_name::<I>(),
                "incoming message filtered out"
            );
        }
    }

    /// Log that an outgoing message was filtered out.
    #[inline]
    pub fn log_outgoing_filtered_out<I>(&self) {
        if self.enabled {
            let span = self.span();
            let _guard = span.enter();
            trace!(
                message_type = type_name::<I>(),
                "outgoing message filtered out"
            );
        }
    }

    /// Set transport name for logging purposes.
    pub fn set_transport_name(&mut self, name: Option<String>) {
        self.name = name;
    }

    /// Get transport name for logging purposes.
    pub fn get_transport_name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Enable logging.
    pub fn enable(&mut self) {
        self.enabled = true;
    }

    /// Disable logging.
    pub fn disable(&mut self) {
        self.enabled = false;
    }

    /// Is logging enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Override existing logger kind with another transport type kind.
    pub fn override_kind<T>(&mut self)
    where
        T: Logging,
    {
        self.kind = T::KIND.to_string();
    }

    /// Override existing logger kind with provided string.
    pub fn override_kind_with_str(&mut self, kind: &str) {
        self.kind = kind.to_string();
    }

    /// Override existing logger kind with another (buffered) transport type kind.
    pub fn override_kind_buffered<T>(&mut self)
    where
        T: Logging,
    {
        self.kind = format!("{}(buffered)", T::KIND);
    }

    /// Override existing logger kind with another (part) transport type kind.
    pub fn override_kind_part<T>(&mut self, variant: usize)
    where
        T: Logging,
    {
        self.kind = format!("{}({variant})", T::KIND);
    }

    /// Create a tracing span for this transport.
    pub fn span(&self) -> Span {
        if let Some(name) = &self.name {
            trace_span!("dnet", transport = self.kind, name = name.as_str())
        } else {
            trace_span!("dnet", transport = self.kind)
        }
    }
}
