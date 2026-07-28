//! Common tests for `dnet` transport implementations.

#![warn(missing_docs)]

use std::fmt::Debug;
use std::time::Duration;

use dnet_base::{Messages, Receive, Transport};

use futures::{stream, SinkExt, StreamExt};

pub use dportable::test::dtest;

pub use dportable::test::dtest_configure;

/// Test transport by sending strings and integers between two connected instances.
pub async fn test_transport<L, R, E1, E2>(mut left: L, mut right: R)
where
    L: Transport<u32, String, E1> + Unpin,
    R: Transport<String, u32, E2> + Unpin,
    E1: Debug,
    E2: Debug,
{
    init_logging(&mut left, &mut right);

    left.send("Hello World!".to_string()).await.unwrap();
    left.send("Hello World again!".to_string()).await.unwrap();
    right.send(128).await.unwrap();
    right.send(1).await.unwrap();

    assert_eq!(right.receive().await.unwrap(), "Hello World!");
    assert_eq!(right.receive().await.unwrap(), "Hello World again!");
    assert_eq!(left.receive().await.unwrap(), 128);
    assert_eq!(left.receive().await.unwrap(), 1);
}

/// Test transport by sending units (`()`) between two connected instances.
/// 
/// When used with `BincodeCodec` it can verify sending message of length 0 (in bytes) works.
pub async fn test_unit_message<L, R, E1, E2>(mut left: L, mut right: R)
where
    L: Transport<(), (), E1> + Unpin,
    R: Transport<(), (), E2> + Unpin,
    E1: Debug,
    E2: Debug,
{
    init_logging(&mut left, &mut right);

    left.send(()).await.unwrap();
    left.send(()).await.unwrap();
    right.send(()).await.unwrap();
    right.send(()).await.unwrap();

    right.receive().await.unwrap();
    right.receive().await.unwrap();
    left.receive().await.unwrap();
    left.receive().await.unwrap();
}

/// Test transport by collecting received messages while treating transport as a stream.
/// 
/// It verifies if transport closing (or dropping) on one side is 
/// communicated to the other side - without it, stream would never complete.
pub async fn test_stream<L, R, E1, E2>(left: L, right: R)
where
    L: Transport<(), u32, E1> + Unpin,
    R: Transport<u32, (), E2> + Unpin,
    E1: Debug,
    E2: Debug,
{
    test_stream_with_sleep_before_drop(left, right, Duration::ZERO).await
}

/// Same as [test_stream] except we [sleep](dportable::time::sleep) for specified 
/// duration before dropping the sending transport.
/// 
/// Used by unreliable QUIC transport - there is no mechanism for the underlying 
/// unreliable transport to wait for messages to be flushed.
pub async fn test_stream_with_sleep_before_drop<L, R, E1, E2>(
    mut left: L,
    mut right: R,
    duration: Duration,
) where
    L: Transport<(), u32, E1> + Unpin,
    R: Transport<u32, (), E2> + Unpin,
    E1: Debug,
    E2: Debug,
{
    init_logging(&mut left, &mut right);

    left.send_all(&mut stream::iter(vec![1, 2, 3].into_iter().map(Ok)))
        .await
        .unwrap();
    if !duration.is_zero() {
        dportable::time::sleep(duration).await;
    }
    drop(left);

    assert_eq!(right.messages().collect::<Vec<u32>>().await, vec![1, 2, 3]);
}

/// Init tracing subscriber and enable logging for given transports.
pub fn init_logging<T1, T2, T1I, T1O, T2I, T2O, E1, E2>(left: &mut T1, right: &mut T2)
where
    T1: Transport<T1I, T1O, E1>,
    T2: Transport<T2I, T2O, E2>,
{
    #[cfg(not(feature = "logging"))]
    {
        let _ = (left, right);
    }

    #[cfg(feature = "logging")]
    {
        init_subscriber();

        left.enable_logging();
        left.set_logging_name("left");

        right.enable_logging();
        right.set_logging_name("right");
    }
}

/// Init tracing subscriber.
#[cfg(feature = "logging")]
pub fn init_subscriber() {
    use std::sync::Once;

    static INIT: Once = Once::new();
    INIT.call_once(|| {
        #[cfg(target_arch = "wasm32")]
        wasm_tracing::set_as_global_default();

        #[cfg(not(target_arch = "wasm32"))]
        tracing_subscriber::fmt().with_env_filter("trace").init();
    });
}
