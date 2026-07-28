#![cfg(target_arch = "wasm32")]

use std::{cell::RefCell, rc::Rc, time::Duration};

use dnet_codecs::BincodeCodec;
use dnet_js::{wrapper::Context, TransferableTransport};
use dnet_rpc::{
    abortable, api, no_ack,
    producer::{
        self,
        abortable::{self, Aborted, AbortionToken},
        Produce,
    },
    Consume, Produce,
};
use futures::{channel::mpsc::unbounded, join, select, FutureExt, Stream, StreamExt};
use wasm_bindgen_test::wasm_bindgen_test;
use web_sys::{
    js_sys::{ArrayBuffer, Float32Array, Uint32Array, Uint8Array},
    MessageChannel,
};

use dportable::{spawn, test::dtest_configure, time::sleep};

dtest_configure!();

#[api]
pub trait Api {
    #[no_ack]
    async fn test_no_ack(&self, #[transferable] some_array_buffer: ArrayBuffer);

    async fn add(&self, #[into_transferable] floats: Float32Array) -> f32;

    #[transferable]
    async fn get_some_transferable(&self) -> ArrayBuffer;

    #[into_transferable]
    async fn stream(&self) -> impl Stream<Item = Uint32Array>;

    #[abortable]
    #[into_transferable]
    async fn abortable(&self) -> Uint8Array;
}

#[derive(Produce)]
struct Producer {
    was_running_for_a_bit: Rc<RefCell<bool>>,
    was_aborted: Rc<RefCell<bool>>,
}

impl Producer {
    async fn test_no_ack(&self, some_array_buffer: ArrayBuffer) {
        assert_eq!(some_array_buffer.byte_length(), 3);
        assert_eq!(Uint8Array::new(&some_array_buffer).to_vec(), vec![1, 2, 3]);
        sleep(Duration::from_secs(1)).await
    }

    async fn add(&self, floats: Float32Array) -> f32 {
        let vec = floats.to_vec();
        vec.iter().sum()
    }

    async fn get_some_transferable(&self) -> ArrayBuffer {
        let array = Uint8Array::new_with_length(3);
        array.copy_from(&[4, 5, 6]);
        array.buffer()
    }

    async fn stream(&self) -> impl Stream<Item = Uint32Array> {
        let (sender, receiver) = unbounded();
        let numbers: Vec<_> = (0..10).collect();
        spawn(async move {
            for chunk in numbers.as_slice().chunks(2) {
                let array = Uint32Array::new_with_length(chunk.len() as u32);
                array.copy_from(chunk);
                let _ = sender.unbounded_send(array);
                sleep(Duration::from_millis(10)).await;
            }
        });
        receiver
    }

    async fn abortable(&self, token: AbortionToken) -> abortable::Result<Uint8Array> {
        let mut already_set = false;
        for _ in 0..4 {
            if token.is_aborted() {
                *self.was_aborted.borrow_mut() = true;
                return Err(Aborted);
            } else {
                if !already_set {
                    *self.was_running_for_a_bit.borrow_mut() = true;
                    already_set = true;
                }
                // abortable task is still running
            }
            sleep(Duration::from_millis(5)).await;
        }
        let array = Uint8Array::new_with_length(1);
        array.set_index(0, 42);
        Ok(array)
    }
}

#[wasm_bindgen_test]
async fn test_rpc() {
    let channel = MessageChannel::new().unwrap();

    let left_port = Rc::new(channel.port1());
    let right_port = Rc::new(channel.port2());

    let left = TransferableTransport::new(
        &left_port,
        None,
        Context::new(BincodeCodec::default()),
        true,
    );
    let right = TransferableTransport::new(
        &right_port,
        None,
        Context::new(BincodeCodec::default()),
        true,
    );

    left_port.start();
    right_port.start();

    let (left, right) = join!(left, right);
    let left = left.unwrap();
    let right = right.unwrap();

    let was_running_for_a_bit = Rc::new(RefCell::new(false));
    let was_aborted = Rc::new(RefCell::new(false));
    let producer = Producer {
        was_running_for_a_bit: was_running_for_a_bit.clone(),
        was_aborted: was_aborted.clone(),
    };

    producer.produce(left, producer::Configuration::default(), Default::default());

    let consumer = Consumer::consume(right, Default::default(), Default::default());

    let array = Uint8Array::new_with_length(3);
    array.copy_from(&[1, 2, 3]);
    let mut without_ack = consumer.test_no_ack(array.buffer());
    select! {
        _ = sleep(Duration::from_millis(10)).fuse() => {
            panic!("sleep returned earlier")
        }
        _ = without_ack => { }
    };

    let floats = Float32Array::new_with_length(4);
    floats.copy_from(&[1.0, 2.0, 3.0, 4.0]);
    assert_eq!(consumer.add(floats).await.unwrap(), 10.0);

    let array_buffer = consumer.get_some_transferable().await.unwrap();
    assert_eq!(array_buffer.byte_length(), 3);
    assert_eq!(Uint8Array::new(&array_buffer).to_vec(), vec![4, 5, 6]);

    let stream = consumer.stream().await.unwrap();
    let received = stream.collect::<Vec<Uint32Array>>().await;
    for (i, array) in received.iter().enumerate() {
        let vec = array.to_vec();
        assert_eq!(vec, vec![i as u32 * 2, i as u32 * 2 + 1]);
    }

    let abortable_result = consumer.abortable().await.unwrap();
    let abortable_vec = abortable_result.to_vec();
    assert_eq!(abortable_vec, vec![42]);

    let mut abortable = consumer.abortable();
    let aborter = abortable.aborter();
    spawn(async move {
        let _ = abortable.await;
    });
    sleep(Duration::from_millis(10)).await;
    aborter.abort();
    // let's give some time for abort request to reach producer:
    sleep(Duration::from_millis(10)).await;

    assert!(*was_running_for_a_bit.borrow_mut());
    assert!(*was_aborted.borrow_mut());
}
