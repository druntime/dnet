use std::{sync::Arc, time::Duration};

use dnet_rpc::{
    abortable, api, no_ack,
    producer::Produce,
    producer::{
        self,
        abortable::{self, Aborted, AbortionToken},
    },
    Consume, Produce,
};
use dnet_tests::{dtest, dtest_configure};
use futures::{channel::mpsc::unbounded, select, FutureExt, Stream, StreamExt};

use dportable::{spawn, time::sleep, Mutex};

dtest_configure!();

#[api]
pub trait Api {
    #[no_ack]
    async fn test_no_ack(&self, wait_for: u32);

    async fn add(&self, a: u32, b: u32) -> u32;

    async fn stream(&self) -> impl Stream<Item = u32>;

    #[abortable]
    async fn abortable(&self) -> u32;
}

#[derive(Produce)]
struct Producer {
    was_running_for_a_bit: Arc<Mutex<bool>>,
    was_aborted: Arc<Mutex<bool>>,
}

impl Producer {
    async fn test_no_ack(&self, wait_for: u32) {
        sleep(Duration::from_secs(wait_for.into())).await
    }

    async fn add(&self, a: u32, b: u32) -> u32 {
        a + b
    }

    async fn stream(&self) -> impl Stream<Item = u32> {
        let (sender, receiver) = unbounded();
        spawn(async move {
            for i in 0..5 {
                let _ = sender.unbounded_send(i);
                sleep(Duration::from_millis(10)).await;
            }
        });
        receiver
    }

    async fn abortable(&self, token: AbortionToken) -> abortable::Result<u32> {
        let mut already_set = false;
        for _ in 0..4 {
            if token.is_aborted() {
                *self.was_aborted.lock() = true;
                return Err(Aborted);
            } else {
                if !already_set {
                    *self.was_running_for_a_bit.lock() = true;
                    already_set = true;
                }
                // abortable task is still running
            }
            sleep(Duration::from_millis(5)).await;
        }
        Ok(42)
    }
}

#[dtest]
async fn test_rpc() {
    let (mut left, mut right) = dnet_utils::channel::transports();

    dnet_tests::init_logging(&mut left, &mut right);

    let was_running_for_a_bit = Arc::new(Mutex::new(false));
    let was_aborted = Arc::new(Mutex::new(false));
    let producer = Producer {
        was_running_for_a_bit: was_running_for_a_bit.clone(),
        was_aborted: was_aborted.clone(),
    };

    producer.produce(left, producer::Configuration::default(), Default::default());

    let consumer = Consumer::consume(right, Default::default(), Default::default());

    let mut without_ack = consumer.test_no_ack(5);
    select! {
        _ = sleep(Duration::from_millis(10)).fuse() => {
            panic!("sleep returned earlier")
        }
        _ = without_ack => { }
    };

    assert_eq!(consumer.add(2, 3).await.unwrap(), 5);

    let stream = consumer.stream().await.unwrap();
    assert_eq!(stream.collect::<Vec<u32>>().await, vec![0, 1, 2, 3, 4]);

    assert_eq!(consumer.abortable().await.unwrap(), 42);

    let mut abortable = consumer.abortable();
    let aborter = abortable.aborter();
    spawn(async move {
        let _ = abortable.await;
    });
    sleep(Duration::from_millis(10)).await;
    aborter.abort();
    // let's give some time for abort request to reach producer:
    sleep(Duration::from_millis(10)).await;

    assert!(*was_running_for_a_bit.lock());
    assert!(*was_aborted.lock());
}
