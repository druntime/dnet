//! Implementation of [IntoTransferable] and [FromTransferable] for
//! JavaScript [typed arrays](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/TypedArray).

use std::marker::PhantomData;

use dnet_base::{Codec, Decode, Encode};
use js_sys::{
    Array, ArrayBuffer, BigInt64Array, BigUint64Array, Float16Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int8Array, Uint16Array, Uint32Array, Uint8Array,
};
use js_utils::JsError;
use wasm_bindgen::{JsCast, JsValue};

use crate::{
    wrapper::{self, Context},
    FromTransferable, IntoTransferable, Transferable, WithTransferable,
};

/// Helper trait implemented here by [typed arrays](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/TypedArray).
pub trait TypedArray: JsCast {
    /// Get TypedArray's [ArrayBuffer] (using its `buffer()` method`).
    fn array_buffer(&self) -> ArrayBuffer;
}

/// Wrapper over [typed arrays](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/TypedArray).
///
/// Used to implement [IntoTransferable]/[FromTransferable] with [wrapper::Context] for the use
/// in structs/enums using [derive@IntoTransferable] derive.
pub struct Wrapper<Codec, T>
where
    T: TypedArray,
{
    /// Wrapped [TypedArray](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/TypedArray).
    pub typed_array: T,

    _codec: PhantomData<Codec>,
}

impl<C, T> Transferable for Wrapper<C, T>
where
    C: Codec,
    T: TypedArray,
{
    type Context = Context<C>;

    type Error = wrapper::Error<<C as Encode>::Error, <C as Decode>::Error>;

    fn prepare_for_transfer(
        self,
        _context: &mut Self::Context,
    ) -> Result<WithTransferable, Self::Error> {
        let transfer = Array::of1(&self.typed_array.array_buffer());
        Ok(WithTransferable {
            data: self.typed_array.into(),
            transfer,
        })
    }

    fn reconstruct(object: JsValue, _context: &mut Self::Context) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Wrapper {
            typed_array: object.dyn_into().map_err(|_| wrapper::Error::WrongType)?,
            _codec: PhantomData,
        })
    }
}

macro_rules! impl_transferable_for_typed_array {
    ($ty:ty) => {
        impl IntoTransferable<(), JsError> for $ty {
            type Output = Self;

            fn into_transferable(self) -> Self::Output {
                self
            }
        }

        impl FromTransferable<(), JsError> for $ty {
            type Input = Self;

            fn from_transferable(input: Self::Input) -> Self {
                input
            }
        }

        impl Transferable for $ty {
            type Context = ();

            type Error = JsError;

            fn prepare_for_transfer(
                self,
                _context: &mut Self::Context,
            ) -> Result<WithTransferable, Self::Error> {
                let transfer = Array::of1(&self.buffer());
                Ok(WithTransferable {
                    data: self.into(),
                    transfer,
                })
            }

            fn reconstruct(
                object: JsValue,
                _context: &mut Self::Context,
            ) -> Result<Self, Self::Error>
            where
                Self: Sized,
            {
                object.dyn_into().map_err(JsError::from)
            }
        }

        impl<C>
            IntoTransferable<Context<C>, wrapper::Error<<C as Encode>::Error, <C as Decode>::Error>>
            for $ty
        where
            C: Codec,
        {
            type Output = Wrapper<C, Self>;

            fn into_transferable(self) -> Self::Output {
                Wrapper {
                    typed_array: self,
                    _codec: PhantomData,
                }
            }
        }

        impl<C>
            FromTransferable<Context<C>, wrapper::Error<<C as Encode>::Error, <C as Decode>::Error>>
            for $ty
        where
            C: Codec,
        {
            type Input = Wrapper<C, Self>;

            fn from_transferable(input: Self::Input) -> Self {
                input.typed_array
            }
        }

        impl TypedArray for $ty {
            fn array_buffer(&self) -> ArrayBuffer {
                self.buffer()
            }
        }
    };
}

impl_transferable_for_typed_array!(Int8Array);
impl_transferable_for_typed_array!(Uint8Array);
impl_transferable_for_typed_array!(Int16Array);
impl_transferable_for_typed_array!(Uint16Array);
impl_transferable_for_typed_array!(Int32Array);
impl_transferable_for_typed_array!(Uint32Array);
impl_transferable_for_typed_array!(Float16Array);
impl_transferable_for_typed_array!(Float32Array);
impl_transferable_for_typed_array!(Float64Array);
impl_transferable_for_typed_array!(BigInt64Array);
impl_transferable_for_typed_array!(BigUint64Array);

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use dnet_base::Receive;
    use dnet_codecs::{bincode, BincodeCodec};
    use dnet_macros::IntoTransferable;
    use dnet_tests::dtest_configure;
    use futures::{join, SinkExt};
    use js_sys::{
        BigInt64Array, BigUint64Array, Float16Array, Float32Array, Float64Array, Int16Array,
        Int32Array, Int8Array, Uint16Array, Uint32Array, Uint8Array,
    };
    use js_utils::JsError;
    use wasm_bindgen_test::wasm_bindgen_test;
    use web_sys::{MessageChannel, MessagePort};

    use crate::{wrapper, TransferableTransport};

    dtest_configure!();

    async fn create_transports_simple<I, O>() -> (
        TransferableTransport<MessagePort, (), I, O, JsError>,
        TransferableTransport<MessagePort, (), O, I, JsError>,
    )
    where
        I: crate::FromTransferable<(), JsError> + crate::IntoTransferable<(), JsError>,
        O: crate::FromTransferable<(), JsError> + crate::IntoTransferable<(), JsError>,
    {
        let channel = MessageChannel::new().unwrap();

        let left_port = Rc::new(channel.port1());
        let right_port = Rc::new(channel.port2());

        let left = TransferableTransport::new(&left_port, None, (), true);
        let right = TransferableTransport::new(&right_port, None, (), true);

        left_port.start();
        right_port.start();

        let (left, right) = join!(left, right);
        let left = left.unwrap();
        let right = right.unwrap();

        (left, right)
    }

    type Context = wrapper::Context<BincodeCodec>;
    type Error = wrapper::Error<bincode::EncodeError, bincode::DecodeError>;

    async fn create_transports<I, O>() -> (
        TransferableTransport<MessagePort, Context, I, O, Error>,
        TransferableTransport<MessagePort, Context, O, I, Error>,
    )
    where
        I: crate::FromTransferable<Context, Error> + crate::IntoTransferable<Context, Error>,
        O: crate::FromTransferable<Context, Error> + crate::IntoTransferable<Context, Error>,
    {
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

        (left, right)
    }

    #[wasm_bindgen_test]
    async fn test_int8_array_into_transferable() {
        let (mut left, mut right) = create_transports_simple::<Int8Array, Int8Array>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let array = Int8Array::new_with_length(3);
        array.copy_from(&[1, 2, 3]);

        left.send(array).await.unwrap();

        let received = right.receive().await.unwrap();

        assert_eq!(received.length(), 3);
        assert_eq!(received.to_vec(), vec![1, 2, 3]);
    }

    #[wasm_bindgen_test]
    async fn test_int8_array_into_transferable_in_derive() {
        #[derive(Debug, IntoTransferable)]
        struct Message {
            #[into_transferable]
            array: Int8Array,
        }

        #[derive(Debug, IntoTransferable)]
        struct Empty {}

        let (mut left, mut right) = create_transports::<Empty, Message>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let array = Int8Array::new_with_length(3);
        array.copy_from(&[1, 2, 3]);

        let message = Message { array };
        left.send(message).await.unwrap();

        let received = right.receive().await.unwrap();

        assert_eq!(received.array.length(), 3);
        assert_eq!(received.array.to_vec(), vec![1, 2, 3]);
    }

    #[wasm_bindgen_test]
    async fn test_uint8_array_into_transferable() {
        let (mut left, mut right) = create_transports_simple::<Uint8Array, Uint8Array>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let array = Uint8Array::new_with_length(3);
        array.copy_from(&[1, 2, 3]);

        left.send(array).await.unwrap();

        let received = right.receive().await.unwrap();

        assert_eq!(received.length(), 3);
        assert_eq!(received.to_vec(), vec![1, 2, 3]);
    }

    #[wasm_bindgen_test]
    async fn test_uint8_array_into_transferable_in_derive() {
        #[derive(Debug, IntoTransferable)]
        struct Message {
            #[into_transferable]
            array: Uint8Array,
        }

        #[derive(Debug, IntoTransferable)]
        struct Empty {}

        let (mut left, mut right) = create_transports::<Empty, Message>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let array = Uint8Array::new_with_length(3);
        array.copy_from(&[1, 2, 3]);

        let message = Message { array };
        left.send(message).await.unwrap();

        let received = right.receive().await.unwrap();

        assert_eq!(received.array.length(), 3);
        assert_eq!(received.array.to_vec(), vec![1, 2, 3]);
    }

    #[wasm_bindgen_test]
    async fn test_int16_array_into_transferable() {
        let (mut left, mut right) = create_transports_simple::<Int16Array, Int16Array>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let array = Int16Array::new_with_length(3);
        array.copy_from(&[1, 2, 3]);

        left.send(array).await.unwrap();

        let received = right.receive().await.unwrap();

        assert_eq!(received.length(), 3);
        assert_eq!(received.to_vec(), vec![1, 2, 3]);
    }

    #[wasm_bindgen_test]
    async fn test_int16_array_into_transferable_in_derive() {
        #[derive(Debug, IntoTransferable)]
        struct Message {
            #[into_transferable]
            array: Int16Array,
        }

        #[derive(Debug, IntoTransferable)]
        struct Empty {}

        let (mut left, mut right) = create_transports::<Empty, Message>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let array = Int16Array::new_with_length(3);
        array.copy_from(&[1, 2, 3]);

        let message = Message { array };
        left.send(message).await.unwrap();

        let received = right.receive().await.unwrap();

        assert_eq!(received.array.length(), 3);
        assert_eq!(received.array.to_vec(), vec![1, 2, 3]);
    }

    #[wasm_bindgen_test]
    async fn test_uint16_array_into_transferable() {
        let (mut left, mut right) = create_transports_simple::<Uint16Array, Uint16Array>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let array = Uint16Array::new_with_length(3);
        array.copy_from(&[1, 2, 3]);

        left.send(array).await.unwrap();

        let received = right.receive().await.unwrap();

        assert_eq!(received.length(), 3);
        assert_eq!(received.to_vec(), vec![1, 2, 3]);
    }

    #[wasm_bindgen_test]
    async fn test_uint16_array_into_transferable_in_derive() {
        #[derive(Debug, IntoTransferable)]
        struct Message {
            #[into_transferable]
            array: Uint16Array,
        }

        #[derive(Debug, IntoTransferable)]
        struct Empty {}

        let (mut left, mut right) = create_transports::<Empty, Message>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let array = Uint16Array::new_with_length(3);
        array.copy_from(&[1, 2, 3]);

        let message = Message { array };
        left.send(message).await.unwrap();

        let received = right.receive().await.unwrap();

        assert_eq!(received.array.length(), 3);
        assert_eq!(received.array.to_vec(), vec![1, 2, 3]);
    }

    #[wasm_bindgen_test]
    async fn test_uint32_array_into_transferable() {
        let (mut left, mut right) = create_transports_simple::<Uint32Array, Uint32Array>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let array = Uint32Array::new_with_length(3);
        array.copy_from(&[1, 2, 3]);

        left.send(array).await.unwrap();

        let received = right.receive().await.unwrap();

        assert_eq!(received.length(), 3);
        assert_eq!(received.to_vec(), vec![1, 2, 3]);
    }

    #[wasm_bindgen_test]
    async fn test_uint32_array_into_transferable_in_derive() {
        #[derive(Debug, IntoTransferable)]
        struct Message {
            #[into_transferable]
            array: Uint32Array,
        }

        #[derive(Debug, IntoTransferable)]
        struct Empty {}

        let (mut left, mut right) = create_transports::<Empty, Message>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let array = Uint32Array::new_with_length(3);
        array.copy_from(&[1, 2, 3]);

        let message = Message { array };
        left.send(message).await.unwrap();

        let received = right.receive().await.unwrap();

        assert_eq!(received.array.length(), 3);
        assert_eq!(received.array.to_vec(), vec![1, 2, 3]);
    }

    #[wasm_bindgen_test]
    async fn test_int32_array_into_transferable() {
        let (mut left, mut right) = create_transports_simple::<Int32Array, Int32Array>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let array = Int32Array::new_with_length(3);
        array.copy_from(&[1, 2, 3]);

        left.send(array).await.unwrap();

        let received = right.receive().await.unwrap();

        assert_eq!(received.length(), 3);
        assert_eq!(received.to_vec(), vec![1, 2, 3]);
    }

    #[wasm_bindgen_test]
    async fn test_int32_array_into_transferable_in_derive() {
        #[derive(Debug, IntoTransferable)]
        struct Message {
            #[into_transferable]
            array: Int32Array,
        }

        #[derive(Debug, IntoTransferable)]
        struct Empty {}

        let (mut left, mut right) = create_transports::<Empty, Message>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let array = Int32Array::new_with_length(3);
        array.copy_from(&[1, 2, 3]);

        let message = Message { array };
        left.send(message).await.unwrap();

        let received = right.receive().await.unwrap();

        assert_eq!(received.array.length(), 3);
        assert_eq!(received.array.to_vec(), vec![1, 2, 3]);
    }

    #[wasm_bindgen_test]
    async fn test_float16_array_into_transferable() {
        let (mut left, mut right) = create_transports_simple::<Float16Array, Float16Array>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let array = Float16Array::new_with_length(3);
        array.set_index_from_f32(0, 1.0);
        array.set_index_from_f32(1, 2.0);
        array.set_index_from_f32(2, 3.0);

        left.send(array).await.unwrap();

        let received = right.receive().await.unwrap();

        assert_eq!(received.length(), 3);
        assert_eq!(received.get_index_as_f32(0), 1.0);
        assert_eq!(received.get_index_as_f32(1), 2.0);
        assert_eq!(received.get_index_as_f32(2), 3.0);
    }

    #[wasm_bindgen_test]
    async fn test_float16_array_into_transferable_in_derive() {
        #[derive(Debug, IntoTransferable)]
        struct Message {
            #[into_transferable]
            array: Float16Array,
        }

        #[derive(Debug, IntoTransferable)]
        struct Empty {}

        let (mut left, mut right) = create_transports::<Empty, Message>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let array = Float16Array::new_with_length(3);
        array.set_index_from_f32(0, 1.0);
        array.set_index_from_f32(1, 2.0);
        array.set_index_from_f32(2, 3.0);

        let message = Message { array };
        left.send(message).await.unwrap();

        let received = right.receive().await.unwrap();

        assert_eq!(received.array.length(), 3);
        assert_eq!(received.array.get_index_as_f32(0), 1.0);
        assert_eq!(received.array.get_index_as_f32(1), 2.0);
        assert_eq!(received.array.get_index_as_f32(2), 3.0);
    }

    #[wasm_bindgen_test]
    async fn test_float32_array_into_transferable() {
        let (mut left, mut right) = create_transports_simple::<Float32Array, Float32Array>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let array = Float32Array::new_with_length(3);
        array.copy_from(&[1.0, 2.0, 3.0]);

        left.send(array).await.unwrap();

        let received = right.receive().await.unwrap();

        assert_eq!(received.length(), 3);
        assert_eq!(received.to_vec(), vec![1.0, 2.0, 3.0]);
    }

    #[wasm_bindgen_test]
    async fn test_float32_array_into_transferable_in_derive() {
        #[derive(Debug, IntoTransferable)]
        struct Message {
            #[into_transferable]
            array: Float32Array,
        }

        #[derive(Debug, IntoTransferable)]
        struct Empty {}

        let (mut left, mut right) = create_transports::<Empty, Message>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let array = Float32Array::new_with_length(3);
        array.copy_from(&[1.0, 2.0, 3.0]);

        let message = Message { array };
        left.send(message).await.unwrap();

        let received = right.receive().await.unwrap();

        assert_eq!(received.array.length(), 3);
        assert_eq!(received.array.to_vec(), vec![1.0, 2.0, 3.0]);
    }

    #[wasm_bindgen_test]
    async fn test_float64_array_into_transferable() {
        let (mut left, mut right) = create_transports_simple::<Float64Array, Float64Array>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let array = Float64Array::new_with_length(3);
        array.copy_from(&[1.0, 2.0, 3.0]);

        left.send(array).await.unwrap();

        let received = right.receive().await.unwrap();

        assert_eq!(received.length(), 3);
        assert_eq!(received.to_vec(), vec![1.0, 2.0, 3.0]);
    }

    #[wasm_bindgen_test]
    async fn test_float64_array_into_transferable_in_derive() {
        #[derive(Debug, IntoTransferable)]
        struct Message {
            #[into_transferable]
            array: Float64Array,
        }

        #[derive(Debug, IntoTransferable)]
        struct Empty {}

        let (mut left, mut right) = create_transports::<Empty, Message>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let array = Float64Array::new_with_length(3);
        array.copy_from(&[1.0, 2.0, 3.0]);

        let message = Message { array };
        left.send(message).await.unwrap();

        let received = right.receive().await.unwrap();

        assert_eq!(received.array.length(), 3);
        assert_eq!(received.array.to_vec(), vec![1.0, 2.0, 3.0]);
    }

    #[wasm_bindgen_test]
    async fn test_bigint64_array_into_transferable() {
        let (mut left, mut right) =
            create_transports_simple::<BigInt64Array, BigInt64Array>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let array = BigInt64Array::new_with_length(3);
        array.copy_from(&[1, 2, 3]);

        left.send(array).await.unwrap();

        let received = right.receive().await.unwrap();

        assert_eq!(received.length(), 3);
        assert_eq!(received.to_vec(), vec![1, 2, 3]);
    }

    #[wasm_bindgen_test]
    async fn test_bigint64_array_into_transferable_in_derive() {
        #[derive(Debug, IntoTransferable)]
        struct Message {
            #[into_transferable]
            array: BigInt64Array,
        }

        #[derive(Debug, IntoTransferable)]
        struct Empty {}

        let (mut left, mut right) = create_transports::<Empty, Message>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let array = BigInt64Array::new_with_length(3);
        array.copy_from(&[1, 2, 3]);

        let message = Message { array };
        left.send(message).await.unwrap();

        let received = right.receive().await.unwrap();

        assert_eq!(received.array.length(), 3);
        assert_eq!(received.array.to_vec(), vec![1, 2, 3]);
    }

    #[wasm_bindgen_test]
    async fn test_biguint64_array_into_transferable() {
        let (mut left, mut right) =
            create_transports_simple::<BigUint64Array, BigUint64Array>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let array = BigUint64Array::new_with_length(3);
        array.copy_from(&[1, 2, 3]);

        left.send(array).await.unwrap();

        let received = right.receive().await.unwrap();

        assert_eq!(received.length(), 3);
        assert_eq!(received.to_vec(), vec![1, 2, 3]);
    }

    #[wasm_bindgen_test]
    async fn test_biguint64_array_into_transferable_in_derive() {
        #[derive(Debug, IntoTransferable)]
        struct Message {
            #[into_transferable]
            array: BigUint64Array,
        }

        #[derive(Debug, IntoTransferable)]
        struct Empty {}

        let (mut left, mut right) = create_transports::<Empty, Message>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let array = BigUint64Array::new_with_length(3);
        array.copy_from(&[1, 2, 3]);

        let message = Message { array };
        left.send(message).await.unwrap();

        let received = right.receive().await.unwrap();

        assert_eq!(received.array.length(), 3);
        assert_eq!(received.array.to_vec(), vec![1, 2, 3]);
    }
}
