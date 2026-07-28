//! Message wrapper for [Transport].
//!
//! [Transport]: super::Transport

use std::{
    fmt::{Debug, Display},
    marker::PhantomData,
};

use dnet_base::{Codec, Decode, Encode};
use dnet_utils::unwrap::Unwrap;
use js_sys::{Array, Uint8Array};
use serde::{Deserialize, Serialize};
use wasm_bindgen::{JsCast, JsValue};

use crate::transferable::{FromTransferable, IntoTransferable};

use super::{Transferable, WithTransferable};

/// Trait implemented by types using [Wrapper]-like implementation of
/// [IntoTransferable]/[FromTransferable].
///
/// This is a convenience trait for use with the `IntoTransferable` derive macro
/// to constrain generic `Into/FromTransferable` types.
pub trait WrapperLikeTransferable<C>:
    IntoTransferable<Context<C>, Error<<C as Encode>::Error, <C as Decode>::Error>>
    + FromTransferable<Context<C>, Error<<C as Encode>::Error, <C as Decode>::Error>>
where
    C: Codec,
{
}

impl<C, T> WrapperLikeTransferable<C> for T
where
    T: IntoTransferable<Context<C>, Error<<C as Encode>::Error, <C as Decode>::Error>>
        + FromTransferable<Context<C>, Error<<C as Encode>::Error, <C as Decode>::Error>>,
    C: Codec,
{
}

/// [Wrapper] error for [Transferable].
#[derive(Debug)]
pub enum Error<SerializationError, DeserializationError> {
    /// [JsValue] was not of an expected type.
    WrongType,

    /// Error occurred during serialization of a message.
    SerializationError(SerializationError),

    /// Error occurred during deserialization of a message.
    DeserializationError(DeserializationError),
}

impl<SerializationError, DeserializationError> Display
    for Error<SerializationError, DeserializationError>
where
    SerializationError: Display,
    DeserializationError: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::WrongType => write!(f, "unexpected JS value type"),
            Error::SerializationError(error) => write!(f, "failed to serialize message: {error}"),
            Error::DeserializationError(error) => {
                write!(f, "failed to deserialize message: {error}")
            }
        }
    }
}

impl<SerializationError, DeserializationError> std::error::Error
    for Error<SerializationError, DeserializationError>
where
    SerializationError: Debug + Display,
    DeserializationError: Debug + Display,
{
}

/// Context needed to prepare for transfer/reconstruct [Wrapper].
#[derive(Debug)]
pub struct Context<Codec>
where
    Codec: dnet_base::Codec,
{
    /// Codec used to serialize/deserialize message.
    pub codec: Codec,

    /// Work buffer.
    pub buffer: Vec<u8>,
}

impl<Codec> Context<Codec>
where
    Codec: dnet_base::Codec,
{
    /// Create new context using provided codec.
    pub fn new(codec: Codec) -> Self {
        Context {
            codec,
            buffer: vec![],
        }
    }
}

/// Message wrapper.
///
/// It uses codec from [Context] to serialize/deserialize message into/from [Uint8Array]
/// when preparing message for transfer/reconstructing it.
#[derive(Debug, Serialize, Deserialize)]
pub struct Wrapper<Codec, T>
where
    Codec: dnet_base::Codec,
{
    /// Message to transfer.
    pub message: T,
    _codec: PhantomData<Codec>,
}

impl<Codec, T> Wrapper<Codec, T>
where
    Codec: dnet_base::Codec,
{
    /// Wrap message into [Wrapper].
    pub fn new(message: T) -> Self {
        Wrapper {
            message,
            _codec: PhantomData,
        }
    }
}

impl<Codec, T> Unwrap for Wrapper<Codec, T>
where
    Codec: dnet_base::Codec,
{
    type Output = T;

    fn unwrap(self) -> Self::Output {
        self.message
    }
}

impl<Codec, T> Transferable for Wrapper<Codec, T>
where
    Codec: dnet_base::Codec,
    T: Serialize,
    for<'de> T: serde::de::Deserialize<'de>,
{
    type Context = Context<Codec>;
    type Error = Error<<Codec as Encode>::Error, <Codec as Decode>::Error>;

    fn prepare_for_transfer(
        self,
        context: &mut Self::Context,
    ) -> Result<WithTransferable, Self::Error> {
        context.buffer.clear();
        context
            .codec
            .encode(&mut context.buffer, &self)
            .map_err(Error::SerializationError)?;
        let data = Uint8Array::from(&context.buffer[..]);
        let transfer = Array::of1(&data.buffer());
        Ok(WithTransferable {
            data: data.into(),
            transfer,
        })
    }

    fn reconstruct(object: JsValue, context: &mut Self::Context) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let data = object
            .dyn_into::<Uint8Array>()
            .map_err(|_| Error::WrongType)?;
        context
            .codec
            .decode(&data.to_vec()[..])
            .map_err(Error::DeserializationError)
    }
}

impl<Codec, T>
    IntoTransferable<Context<Codec>, Error<<Codec as Encode>::Error, <Codec as Decode>::Error>>
    for Wrapper<Codec, T>
where
    Codec: dnet_base::Codec,
    T: Serialize,
    for<'de> T: serde::de::Deserialize<'de>,
{
    type Output = Wrapper<Codec, T>;

    fn into_transferable(self) -> Self::Output {
        self
    }
}

impl<Codec, T>
    FromTransferable<Context<Codec>, Error<<Codec as Encode>::Error, <Codec as Decode>::Error>>
    for Wrapper<Codec, T>
where
    Codec: dnet_base::Codec,
    T: Serialize,
    for<'de> T: serde::de::Deserialize<'de>,
{
    type Input = Wrapper<Codec, T>;

    fn from_transferable(input: Self::Input) -> Self {
        input
    }
}
