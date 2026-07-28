use std::mem::replace;

use dnet::rpc::{
    abortable, api,
    producer::abortable::{self, Aborted, AbortionToken},
    Produce,
};
use dportable::yield_now;
use js_utils::console_log;
use num_bigint::BigUint;
use num_traits::{One, Zero};

#[api]
pub trait Api {
    async fn is_running(&self) -> bool;

    #[abortable]
    async fn fibonacci(&self, n: u64) -> BigUint;

    #[abortable]
    async fn factorial(&self, n: u64) -> BigUint;
}

#[derive(Debug, Produce)]
pub struct Producer {}

impl Producer {
    async fn is_running(&self) -> bool {
        true
    }

    async fn fibonacci(&self, n: u64, token: AbortionToken) -> abortable::Result<BigUint> {
        let mut f0 = Zero::zero();
        let mut f1 = One::one();
        for i in 0..n {
            let f2 = f0 + &f1;
            f0 = replace(&mut f1, f2);

            if i % 100 == 0 {
                if token.is_aborted() {
                    console_log!("Producer: fibonacci({n}) task was aborted.");
                    return Err(Aborted);
                }
                // let's cooperate with other tasks and give event loop chance to process events:
                yield_now().await;
            }
        }
        Ok(f0)
    }

    async fn factorial(&self, n: u64, token: AbortionToken) -> abortable::Result<BigUint> {
        let mut result = One::one();
        for i in 1..=n {
            result *= i;

            if i % 100 == 0 {
                if token.is_aborted() {
                    console_log!("Producer: {n}! task was aborted.");
                    return Err(Aborted);
                }
                // let's cooperate with other tasks and give event loop chance to process events:
                yield_now().await;
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use dnet::rpc::producer::abortable::AbortionToken;
    use dportable::{
        test::{dtest, dtest_configure},
        CancellationToken,
    };
    use num_bigint::BigUint;

    dtest_configure!();

    use crate::api::Producer;

    async fn fibonacci(n: u64) -> BigUint {
        let producer = Producer {};
        let token = AbortionToken::new(CancellationToken::new());
        producer.fibonacci(n, token).await.unwrap()
    }

    async fn factorial(n: u64) -> BigUint {
        let producer = Producer {};
        let token = AbortionToken::new(CancellationToken::new());
        producer.factorial(n, token).await.unwrap()
    }

    #[dtest]
    async fn test_fibonacci() {
        assert_eq!(fibonacci(0).await, 0u32.into());
        assert_eq!(fibonacci(2).await, 1u32.into());
        assert_eq!(fibonacci(3).await, 2u32.into());
        assert_eq!(fibonacci(4).await, 3u32.into());
    }

    #[dtest]
    async fn test_factorial() {
        assert_eq!(factorial(0).await, 1u32.into());
        assert_eq!(factorial(1).await, 1u32.into());
        assert_eq!(factorial(2).await, 2u32.into());
        assert_eq!(factorial(3).await, 6u32.into());
    }
}
