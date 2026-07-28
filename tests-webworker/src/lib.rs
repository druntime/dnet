#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(start)]
async fn start() {
    use dnet::{codecs::BincodeCodec, webworker::WebWorkerTransport, Logging, Receive};
    use futures::SinkExt;
    use js_utils::spawn;
    use web_sys::DedicatedWorkerGlobalScope;

    dnet_tests::init_subscriber();

    spawn(async {
        let mut transport: WebWorkerTransport<
            DedicatedWorkerGlobalScope,
            BincodeCodec,
            i32,
            String,
        > = WebWorkerTransport::new_with_name_in_worker(BincodeCodec::default(), "named")
            .await
            .unwrap();

        transport.enable_logging();

        assert_eq!(transport.receive().await.unwrap(), 88);
        transport.send("ok-named".to_string()).await.unwrap();
    });

    let mut transport: WebWorkerTransport<DedicatedWorkerGlobalScope, BincodeCodec, i32, String> =
        WebWorkerTransport::new_in_worker(BincodeCodec::default())
            .await
            .unwrap();

    transport.enable_logging();

    assert_eq!(transport.receive().await.unwrap(), 77);
    transport.send("ok".to_string()).await.unwrap();
}
