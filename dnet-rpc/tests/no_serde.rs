use dnet_rpc::{
    api, consumer, no_serde,
    producer::{self, Produce},
    Consume, Produce,
};
use dnet_tests::{dtest, dtest_configure};

dtest_configure!();

#[derive(Debug, Clone)]
pub struct NotSerializable {}

#[api]
#[no_serde]
pub trait Api {
    async fn test_no_serde(&self, not_serializable: NotSerializable);
}

#[derive(Produce)]
struct Producer {}

impl Producer {
    async fn test_no_serde(&self, _not_serializable: NotSerializable) {}
}

#[dtest]
async fn test_no_serde() {
    let (mut left, mut right) = dnet_utils::channel::transports();

    dnet_tests::init_logging(&mut left, &mut right);

    let producer = Producer {};
    producer.produce(left, producer::Configuration::default(), Default::default());

    let consumer = Consumer::consume(
        right,
        consumer::Configuration::default(),
        Default::default(),
    );

    consumer.test_no_serde(NotSerializable {}).await.unwrap();
}
