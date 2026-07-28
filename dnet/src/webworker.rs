//! Transport for communication with
//! [Web Workers](https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API).

use std::{
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

#[cfg(feature = "logging")]
use dnet_base::Logging;
use dnet_base::{Decode, Encode};
use futures::{stream::FusedStream, Sink, Stream};
use pin_project::pin_project;
use serde::Serialize;
use wasm_bindgen::JsCast;
use web_sys::{DedicatedWorkerGlobalScope, EventTarget};

use crate::js::{self, PostMessage, Transport};

/// [WebWorkerTransport] error.
pub type Error<Codec> = js::Error<Codec>;

/// Transport for communication with
/// [Web Workers](https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API).
#[pin_project]
pub struct WebWorkerTransport<T, Codec, Incoming, Outgoing>
where
    T: JsCast + AsRef<EventTarget> + PostMessage,
    Codec: crate::Codec,
    <Codec as Encode>::Error: 'static,
    <Codec as Decode>::Error: 'static,
    Incoming: Serialize,
    Outgoing: Serialize,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    for<'de> Outgoing: serde::de::Deserialize<'de>,
{
    #[pin]
    inner: Transport<T, Codec, Incoming, Outgoing>,
}

impl<T, Codec, Incoming, Outgoing> WebWorkerTransport<T, Codec, Incoming, Outgoing>
where
    T: JsCast + AsRef<EventTarget> + PostMessage,
    Codec: crate::Codec,
    <Codec as Encode>::Error: 'static,
    <Codec as Decode>::Error: 'static,
    Incoming: Serialize,
    Outgoing: Serialize,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    for<'de> Outgoing: serde::de::Deserialize<'de>,
{
    /// Create new transport for communication with worker.
    pub async fn new(worker: T, codec: Codec) -> Result<Self, Error<Codec>> {
        let mut inner = Transport::new(&Rc::new(worker), None, codec, false).await?;

        #[cfg(feature = "logging")]
        inner.with_logger_mut(|logger| {
            logger.override_kind_with_str(&format!("{}(host)", Self::KIND))
        });

        inner.wait_for_open().await;
        Ok(WebWorkerTransport { inner })
    }

    /// Create new named transport for communication with worker.
    pub async fn new_with_name(worker: T, codec: Codec, name: &str) -> Result<Self, Error<Codec>> {
        let mut inner =
            Transport::new(&Rc::new(worker), Some(name.to_string()), codec, false).await?;

        #[cfg(feature = "logging")]
        inner.with_logger_mut(|logger| {
            logger.override_kind_with_str(&format!("{}(host)", Self::KIND))
        });

        inner.wait_for_open().await;
        Ok(WebWorkerTransport { inner })
    }
}

impl<Codec, Incoming, Outgoing>
    WebWorkerTransport<DedicatedWorkerGlobalScope, Codec, Incoming, Outgoing>
where
    Codec: crate::Codec,
    <Codec as Encode>::Error: 'static,
    <Codec as Decode>::Error: 'static,
    Incoming: Serialize,
    Outgoing: Serialize,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    for<'de> Outgoing: serde::de::Deserialize<'de>,
{
    /// Create new transport for communication inside worker.
    ///
    /// This method should be used only inside worker.
    pub async fn new_in_worker(codec: Codec) -> Result<Self, Error<Codec>> {
        let global = js_sys::global()
            .dyn_into::<DedicatedWorkerGlobalScope>()
            .map_err(|_| Error::<Codec>::NotInWorker)?;
        #[allow(unused_mut)]
        let mut inner = Transport::new(&Rc::new(global), None, codec, false).await?;

        #[cfg(feature = "logging")]
        inner.with_logger_mut(|logger| {
            logger.override_kind_with_str(&format!("{}(worker)", Self::KIND))
        });

        Ok(WebWorkerTransport { inner })
    }

    /// Create new named transport for communication inside worker.
    ///
    /// This method should be used only inside worker.
    pub async fn new_with_name_in_worker(codec: Codec, name: &str) -> Result<Self, Error<Codec>> {
        let global = js_sys::global()
            .dyn_into::<DedicatedWorkerGlobalScope>()
            .map_err(|_| Error::<Codec>::NotInWorker)?;
        #[allow(unused_mut)]
        let mut inner =
            Transport::new(&Rc::new(global), Some(name.to_string()), codec, false).await?;

        #[cfg(feature = "logging")]
        inner.with_logger_mut(|logger| {
            logger.override_kind_with_str(&format!("{}(worker)", Self::KIND))
        });

        Ok(WebWorkerTransport { inner })
    }
}

impl<T, Codec, Incoming, Outgoing> Sink<Outgoing>
    for WebWorkerTransport<T, Codec, Incoming, Outgoing>
where
    T: JsCast + AsRef<EventTarget> + PostMessage,
    Codec: crate::Codec,
    <Codec as Encode>::Error: 'static,
    <Codec as Decode>::Error: 'static,
    Incoming: Serialize,
    Outgoing: Serialize,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    for<'de> Outgoing: serde::de::Deserialize<'de>,
{
    type Error = crate::Error<Error<Codec>>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Outgoing) -> Result<(), Self::Error> {
        self.project().inner.start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_close(cx)
    }
}

impl<T, Codec, Incoming, Outgoing> Stream for WebWorkerTransport<T, Codec, Incoming, Outgoing>
where
    T: JsCast + AsRef<EventTarget> + PostMessage,
    Codec: crate::Codec,
    <Codec as Encode>::Error: 'static,
    <Codec as Decode>::Error: 'static,
    Incoming: Serialize,
    Outgoing: Serialize,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    for<'de> Outgoing: serde::de::Deserialize<'de>,
{
    type Item = Result<Incoming, Error<Codec>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

impl<T, Codec, Incoming, Outgoing> FusedStream for WebWorkerTransport<T, Codec, Incoming, Outgoing>
where
    T: JsCast + AsRef<EventTarget> + PostMessage,
    Codec: crate::Codec,
    <Codec as Encode>::Error: 'static,
    <Codec as Decode>::Error: 'static,
    Incoming: Serialize,
    Outgoing: Serialize,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    for<'de> Outgoing: serde::de::Deserialize<'de>,
{
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

#[cfg(feature = "logging")]
impl<T, Codec, Incoming, Outgoing> dnet_base::Logging
    for WebWorkerTransport<T, Codec, Incoming, Outgoing>
where
    T: JsCast + AsRef<EventTarget> + PostMessage,
    Codec: crate::Codec,
    <Codec as Encode>::Error: 'static,
    <Codec as Decode>::Error: 'static,
    Incoming: Serialize,
    Outgoing: Serialize,
    for<'de> Incoming: serde::de::Deserialize<'de>,
    for<'de> Outgoing: serde::de::Deserialize<'de>,
{
    const KIND: &'static str = "WebWorker";

    fn with_logger<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&dnet_base::Logger) -> R,
    {
        self.inner.with_logger(f)
    }

    fn with_logger_mut<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut dnet_base::Logger) -> R,
    {
        self.inner.with_logger_mut(f)
    }
}
#[cfg(test)]
mod tests {
    use dnet_tests::dtest_configure;
    use futures::SinkExt;
    use js_sys::Array;
    use wasm_bindgen::prelude::*;
    use wasm_bindgen_test::wasm_bindgen_test;
    use web_sys::{Blob, BlobPropertyBag, Url, Worker, WorkerOptions, WorkerType};

    use crate::{codecs::BincodeCodec, Receive};

    use super::WebWorkerTransport;

    dtest_configure!();

    async fn start_worker() -> Worker {
        let worker_code = include_str!("../../tests-js/tests_webworker.js");

        let blob_parts = Array::of1(&JsValue::from_str(&worker_code));
        let options = BlobPropertyBag::new();
        options.set_type("application/javascript");
        let blob = Blob::new_with_str_sequence_and_options(&blob_parts, &options).unwrap();

        let url = Url::create_object_url_with_blob(&blob).unwrap();

        let worker_options = WorkerOptions::new();
        worker_options.set_type(WorkerType::Module);
        let worker = Worker::new_with_options(&url, &worker_options).unwrap();
        worker
    }

    #[wasm_bindgen_test]
    async fn test_transport() {
        let worker = start_worker().await;
        let mut transport: WebWorkerTransport<Worker, BincodeCodec, String, i32> =
            WebWorkerTransport::new(worker, BincodeCodec::default())
                .await
                .unwrap();

        #[cfg(feature = "logging")]
        {
            use dnet_base::Logging;
            dnet_tests::init_subscriber();
            transport.enable_logging();
        }

        transport.send(77).await.unwrap();
        assert_eq!(transport.receive().await.unwrap(), "ok");
    }

    #[wasm_bindgen_test]
    async fn test_named_transport() {
        let worker = start_worker().await;
        let mut transport: WebWorkerTransport<Worker, BincodeCodec, String, i32> =
            WebWorkerTransport::new_with_name(worker, BincodeCodec::default(), "named")
                .await
                .unwrap();

        #[cfg(feature = "logging")]
        {
            use dnet_base::Logging;
            dnet_tests::init_subscriber();
            transport.enable_logging();
        }

        transport.send(88).await.unwrap();
        assert_eq!(transport.receive().await.unwrap(), "ok-named");
    }
}
