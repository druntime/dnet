use quote::quote;
use syn::{parse2, DeriveInput};

use crate::{
    tests::compare,
    transferable::{into_transferable_with_paths, tests::paths},
};

#[test]
fn test_into_transferable_enum() {
    let input = quote! {
        #[derive(Clone, IntoTransferable)]
        pub enum Message {
            WithTransferable {
                some_string: String,

                #[transferable]
                some_transferable: ArrayBuffer,
            },

            Simple {
                some_u32: u32,
            },

            NoFields,
        }
    };

    let expected = quote! {
        impl<
            C,
        > ::dnet::js::IntoTransferable<
            ::dnet::js::wrapper::Context<C>,
            ::dnet::js::wrapper::Error<
                <C as ::dnet::Encode>::Error,
                <C as ::dnet::Decode>::Error,
            >,
        > for Message
        where
            C: ::dnet::Codec,
        {
            type Output = MessageWrapper<C>;

            fn into_transferable(self) -> Self::Output {
                MessageWrapper::from(self)
            }
        }

        impl<
            C,
        > ::dnet::js::FromTransferable<
            ::dnet::js::wrapper::Context<C>,
            ::dnet::js::wrapper::Error<
                <C as ::dnet::Encode>::Error,
                <C as ::dnet::Decode>::Error,
            >,
        > for Message
        where
            C: ::dnet::Codec,
        {
            type Input = MessageWrapper<C>;

            fn from_transferable(input: Self::Input) -> Self {
                use ::dnet::utils::unwrap::Unwrap;
                input.unwrap()
            }
        }

        #[derive(::serde::Serialize, ::serde::Deserialize)]
        pub enum MessageStripped {
            WithTransferable {
                some_string: String,
                _phantom_data__: std::marker::PhantomData<()>,
            },
            Simple { some_u32: u32, _phantom_data__: std::marker::PhantomData<()> },
            NoFields { _phantom_data__: std::marker::PhantomData<()> },
        }

        pub struct MessageWrapper<C> {
            stripped: MessageStripped,
            transferables: Vec<::wasm_bindgen::JsValue>,
            _codec: std::marker::PhantomData<C>,
        }

        impl<C> From<Message> for MessageWrapper<C> {
            fn from(item: Message) -> Self {
                match item {
                    Message::WithTransferable { some_string, some_transferable } => {
                        let stripped__ = MessageStripped::WithTransferable {
                            some_string,
                            _phantom_data__: std::marker::PhantomData,
                        };
                        let transferables__ = vec![some_transferable.into(),];
                        Self {
                            stripped: stripped__,
                            transferables: transferables__,
                            _codec: std::marker::PhantomData,
                        }
                    }
                    Message::Simple { some_u32 } => {
                        let stripped__ = MessageStripped::Simple {
                            some_u32,
                            _phantom_data__: std::marker::PhantomData,
                        };
                        let transferables__ = vec![];
                        Self {
                            stripped: stripped__,
                            transferables: transferables__,
                            _codec: std::marker::PhantomData,
                        }
                    }
                    Message::NoFields {} => {
                        let stripped__ = MessageStripped::NoFields {
                            _phantom_data__: std::marker::PhantomData,
                        };
                        let transferables__ = vec![];
                        Self {
                            stripped: stripped__,
                            transferables: transferables__,
                            _codec: std::marker::PhantomData,
                        }
                    }
                }
            }
        }

        impl<C> ::dnet::utils::unwrap::Unwrap for MessageWrapper<C> {
            type Output = Message;

            fn unwrap(self) -> Self::Output {
                use ::wasm_bindgen::JsCast;
                let MessageWrapper {
                    stripped: stripped__,
                    transferables: mut transferables__,
                    ..
                } = self;
                match stripped__ {
                    MessageStripped::WithTransferable { some_string, .. } => {
                        let some_transferable = transferables__
                            .pop()
                            .expect("malformed transferables")
                            .dyn_into()
                            .expect("malformed transferables");
                        Message::WithTransferable {
                            some_string,
                            some_transferable,
                        }
                    }
                    MessageStripped::Simple { some_u32, .. } => Message::Simple { some_u32 },
                    MessageStripped::NoFields { .. } => Message::NoFields {},
                }
            }
        }

        impl<C> ::dnet::js::Transferable for MessageWrapper<C>
        where
            C: ::dnet::Codec,
        {
            type Context = ::dnet::js::wrapper::Context<C>;
            type Error = ::dnet::js::wrapper::Error<
                <C as ::dnet::Encode>::Error,
                <C as ::dnet::Decode>::Error,
            >;

            fn prepare_for_transfer(
                mut self,
                context: &mut Self::Context,
            ) -> Result<::dnet::js::WithTransferable, Self::Error> {
                use ::dnet::js::wrapper::Error;
                use ::wasm_bindgen::JsValue;
                use ::web_sys::js_sys::{Array, Uint8Array};

                context.buffer.clear();
                context
                    .codec
                    .encode(&mut context.buffer, &self.stripped)
                    .map_err(Error::SerializationError)?;

                let stripped = Uint8Array::from(&context.buffer[..]);
                let transferables = Array::of(&self.transferables);
                let mut transfer = self.transferables;
                transfer.push(stripped.buffer().into());
                let transfer = Array::of(&transfer);

                let into_transferables = None;

                let data = ::dnet::js::transferable::utils::construct_into_transferable_data_object(
                    &stripped,
                    &transferables,
                    into_transferables.as_ref(),
                );

                Ok(::dnet::js::WithTransferable {
                    data: data.into(),
                    transfer,
                })
            }

            fn reconstruct(
                object: ::wasm_bindgen::JsValue,
                context: &mut Self::Context,
            ) -> Result<Self, Self::Error>
            where
                Self: Sized,
            {
                use ::dnet::js::wrapper::Error;
                use ::wasm_bindgen::{JsCast, JsValue};
                use ::web_sys::js_sys::Array;

                let (stripped, transferables, into_transferables) = ::dnet::js::transferable::utils::destruct_into_transferable_data_object(
                    &object,
                )?;

                let stripped = context
                    .codec
                    .decode(&stripped.to_vec()[..])
                    .map_err(Error::DeserializationError)?;

                let _ = into_transferables;

                Ok(Self {
                    stripped,
                    transferables: transferables.to_vec(),
                    _codec: std::marker::PhantomData,
                })
            }
        }
    };

    let parsed = parse2::<DeriveInput>(input).unwrap();
    let item = into_transferable_with_paths(parsed, paths()).unwrap();

    let actual = quote! {
        #item
    };

    compare(expected, actual);
}

#[test]
fn test_into_transferable_nested_enum() {
    let input = quote! {
        #[derive(Clone, IntoTransferable)]
        pub enum Message2 {
            WithTransferable {
                some_string: String,

                #[transferable]
                some_transferable: ArrayBuffer,
            },

            WithIntoTransferable {
                some_other_string: String,

                #[into_transferable]
                some_into_transferable: Message,
            },

            Simple {
                some_u32: u32,
            },

            NoFields,
        }
    };

    let expected = quote! {
        impl<
            C,
        > ::dnet::js::IntoTransferable<
            ::dnet::js::wrapper::Context<C>,
            ::dnet::js::wrapper::Error<
                <C as ::dnet::Encode>::Error,
                <C as ::dnet::Decode>::Error,
            >,
        > for Message2
        where
            C: ::dnet::Codec,
        {
            type Output = Message2Wrapper<C>;

            fn into_transferable(self) -> Self::Output {
                Message2Wrapper::from(self)
            }
        }

        impl<
            C,
        > ::dnet::js::FromTransferable<
            ::dnet::js::wrapper::Context<C>,
            ::dnet::js::wrapper::Error<
                <C as ::dnet::Encode>::Error,
                <C as ::dnet::Decode>::Error,
            >,
        > for Message2
        where
            C: ::dnet::Codec,
        {
            type Input = Message2Wrapper<C>;

            fn from_transferable(input: Self::Input) -> Self {
                use ::dnet::utils::unwrap::Unwrap;
                input.unwrap()
            }
        }

        #[derive(::serde::Serialize, ::serde::Deserialize)]
        pub enum Message2Stripped {
            WithTransferable {
                some_string: String,
                _phantom_data__: std::marker::PhantomData<()>,
            },
            WithIntoTransferable {
                some_other_string: String,
                _phantom_data__: std::marker::PhantomData<()>,
            },
            Simple { some_u32: u32, _phantom_data__: std::marker::PhantomData<()> },
            NoFields { _phantom_data__: std::marker::PhantomData<()> },
        }

        pub enum Message2IntoTransferables<C> {
            WithTransferable { _phantom_data__: std::marker::PhantomData<(C,)> },
            WithIntoTransferable {
                some_into_transferable: Message,
                _phantom_data__: std::marker::PhantomData<(C,)>,
            },
            Simple { _phantom_data__: std::marker::PhantomData<(C,)> },
            NoFields { _phantom_data__: std::marker::PhantomData<(C,)> },
        }

        impl<C> Message2IntoTransferables<C> {
            fn pack(
                self,
                context__: &mut ::dnet::js::wrapper::Context<C>,
            ) -> Result<
                ::dnet::js::WithTransferable,
                ::dnet::js::wrapper::Error<
                    <C as ::dnet::Encode>::Error,
                    <C as ::dnet::Decode>::Error,
                >,
            >
            where
                C: ::dnet::Codec,
            {
                use ::dnet::js::{IntoTransferable, Transferable as _};
                let data__ = ::web_sys::js_sys::Array::new();
                let mut transfer__ = ::web_sys::js_sys::Array::new();
                match self {
                    Message2IntoTransferables::WithTransferable { .. } => {
                        data__.push(&::wasm_bindgen::JsValue::from_f64(0usize as f64));
                    }
                    Message2IntoTransferables::WithIntoTransferable {
                        some_into_transferable,
                        ..
                    } => {
                        {
                            let ::dnet::js::WithTransferable { data, transfer } = IntoTransferable::<
                                    ::dnet::js::wrapper::Context<C>,
                                    ::dnet::js::wrapper::Error<
                                        <C as ::dnet::Encode>::Error,
                                        <C as ::dnet::Decode>::Error,
                                    >,
                                >::into_transferable(some_into_transferable)
                                    .prepare_for_transfer(context__)?;
                            data__.push(&data);
                            transfer__ = transfer__.concat(&transfer);
                        }
                        data__.push(&::wasm_bindgen::JsValue::from_f64(1usize as f64));
                    }
                    Message2IntoTransferables::Simple { .. } => {
                        data__.push(&::wasm_bindgen::JsValue::from_f64(2usize as f64));
                    }
                    Message2IntoTransferables::NoFields { .. } => {
                        data__.push(&::wasm_bindgen::JsValue::from_f64(3usize as f64));
                    }
                }
                Ok(::dnet::js::WithTransferable {
                    data: data__.into(),
                    transfer: transfer__,
                })
            }

            fn unpack(
                value__: ::wasm_bindgen::JsValue,
                context__: &mut ::dnet::js::wrapper::Context<C>,
            ) -> Result<
                Self,
                ::dnet::js::wrapper::Error<
                    <C as ::dnet::Encode>::Error,
                    <C as ::dnet::Decode>::Error,
                >,
            >
            where
                C: ::dnet::Codec,
            {
                use ::dnet::js::{FromTransferable, Transferable};
                use ::wasm_bindgen::JsCast;
                let array__: ::web_sys::js_sys::Array = value__
                    .dyn_into()
                    .expect("malformed into_transferables");
                let variant_index__: usize = array__
                    .pop()
                    .as_f64()
                    .expect("malformed into_transferables") as usize;
                match variant_index__ {
                    0usize => {
                        Ok(Message2IntoTransferables::WithTransferable {
                            _phantom_data__: std::marker::PhantomData,
                        })
                    }
                    1usize => {
                        let some_into_transferable = FromTransferable::<
                            ::dnet::js::wrapper::Context<C>,
                            ::dnet::js::wrapper::Error<
                                <C as ::dnet::Encode>::Error,
                                <C as ::dnet::Decode>::Error,
                            >,
                        >::from_transferable(Transferable::reconstruct(array__.pop(), context__)?);
                        Ok(Message2IntoTransferables::WithIntoTransferable {
                            some_into_transferable,
                            _phantom_data__: std::marker::PhantomData,
                        })
                    }
                    2usize => {
                        Ok(Message2IntoTransferables::Simple {
                            _phantom_data__: std::marker::PhantomData,
                        })
                    }
                    3usize => {
                        Ok(Message2IntoTransferables::NoFields {
                            _phantom_data__: std::marker::PhantomData,
                        })
                    }
                    _ => unreachable!("invalid into_transferables variant received"),
                }
            }
        }

        pub struct Message2Wrapper<C> {
            stripped: Message2Stripped,
            transferables: Vec<::wasm_bindgen::JsValue>,
            into_transferables: Message2IntoTransferables<C>,
            _codec: std::marker::PhantomData<C>,
        }

        impl<C> From<Message2> for Message2Wrapper<C> {
            fn from(item: Message2) -> Self {
                match item {
                    Message2::WithTransferable { some_string, some_transferable } => {
                        let stripped__ = Message2Stripped::WithTransferable {
                            some_string,
                            _phantom_data__: std::marker::PhantomData,
                        };
                        let transferables__ = vec![some_transferable.into(),];
                        Self {
                            stripped: stripped__,
                            transferables: transferables__,
                            into_transferables: Message2IntoTransferables::WithTransferable {
                                _phantom_data__: std::marker::PhantomData,
                            },
                            _codec: std::marker::PhantomData,
                        }
                    }
                    Message2::WithIntoTransferable {
                        some_other_string,
                        some_into_transferable,
                    } => {
                        let stripped__ = Message2Stripped::WithIntoTransferable {
                            some_other_string,
                            _phantom_data__: std::marker::PhantomData,
                        };
                        let transferables__ = vec![];
                        Self {
                            stripped: stripped__,
                            transferables: transferables__,
                            into_transferables: Message2IntoTransferables::WithIntoTransferable {
                                some_into_transferable,
                                _phantom_data__: std::marker::PhantomData,
                            },
                            _codec: std::marker::PhantomData,
                        }
                    }
                    Message2::Simple { some_u32 } => {
                        let stripped__ = Message2Stripped::Simple {
                            some_u32,
                            _phantom_data__: std::marker::PhantomData,
                        };
                        let transferables__ = vec![];
                        Self {
                            stripped: stripped__,
                            transferables: transferables__,
                            into_transferables: Message2IntoTransferables::Simple {
                                _phantom_data__: std::marker::PhantomData,
                            },
                            _codec: std::marker::PhantomData,
                        }
                    }
                    Message2::NoFields {} => {
                        let stripped__ = Message2Stripped::NoFields {
                            _phantom_data__: std::marker::PhantomData,
                        };
                        let transferables__ = vec![];
                        Self {
                            stripped: stripped__,
                            transferables: transferables__,
                            into_transferables: Message2IntoTransferables::NoFields {
                                _phantom_data__: std::marker::PhantomData,
                            },
                            _codec: std::marker::PhantomData,
                        }
                    }
                }
            }
        }

        impl<C> ::dnet::utils::unwrap::Unwrap for Message2Wrapper<C> {
            type Output = Message2;

            fn unwrap(self) -> Self::Output {
                use ::wasm_bindgen::JsCast;
                let Message2Wrapper {
                    stripped: stripped__,
                    transferables: mut transferables__,
                    into_transferables: into_transferables__,
                    ..
                } = self;
                match stripped__ {
                    Message2Stripped::WithTransferable { some_string, .. } => {
                        let some_transferable = transferables__
                            .pop()
                            .expect("malformed transferables")
                            .dyn_into()
                            .expect("malformed transferables");
                        let Message2IntoTransferables::WithTransferable { .. } = into_transferables__
                        else { unreachable!("invalid into_transferables variant encountered") };
                        Message2::WithTransferable {
                            some_string,
                            some_transferable,
                        }
                    }
                    Message2Stripped::WithIntoTransferable { some_other_string, .. } => {
                        let Message2IntoTransferables::WithIntoTransferable {
                            some_into_transferable,
                            ..
                        } = into_transferables__ else {
                            unreachable!("invalid into_transferables variant encountered")
                        };
                        Message2::WithIntoTransferable {
                            some_other_string,
                            some_into_transferable,
                        }
                    }
                    Message2Stripped::Simple { some_u32, .. } => {
                        let Message2IntoTransferables::Simple { .. } = into_transferables__ else {
                            unreachable!("invalid into_transferables variant encountered")
                        };
                        Message2::Simple { some_u32 }
                    }
                    Message2Stripped::NoFields { .. } => {
                        let Message2IntoTransferables::NoFields { .. } = into_transferables__
                        else { unreachable!("invalid into_transferables variant encountered") };
                        Message2::NoFields {}
                    }
                }
            }
        }

        impl<C> ::dnet::js::Transferable for Message2Wrapper<C>
        where
            C: ::dnet::Codec,
        {
            type Context = ::dnet::js::wrapper::Context<C>;
            type Error = ::dnet::js::wrapper::Error<
                <C as ::dnet::Encode>::Error,
                <C as ::dnet::Decode>::Error,
            >;

            fn prepare_for_transfer(
                mut self,
                context: &mut Self::Context,
            ) -> Result<::dnet::js::WithTransferable, Self::Error> {
                use ::dnet::js::wrapper::Error;
                use ::wasm_bindgen::JsValue;
                use ::web_sys::js_sys::{Array, Uint8Array};

                context.buffer.clear();
                context
                    .codec
                    .encode(&mut context.buffer, &self.stripped)
                    .map_err(Error::SerializationError)?;

                let stripped = Uint8Array::from(&context.buffer[..]);
                let transferables = Array::of(&self.transferables);
                let mut transfer = self.transferables;
                transfer.push(stripped.buffer().into());
                let transfer = Array::of(&transfer);

                let into_transferables = self.into_transferables.pack(context)?;
                let transfer = transfer.concat(&into_transferables.transfer);
                let into_transferables = Some(into_transferables.data);

                let data = ::dnet::js::transferable::utils::construct_into_transferable_data_object(
                    &stripped,
                    &transferables,
                    into_transferables.as_ref(),
                );
                
                Ok(::dnet::js::WithTransferable {
                    data: data.into(),
                    transfer,
                })
            }

            fn reconstruct(
                object: ::wasm_bindgen::JsValue,
                context: &mut Self::Context,
            ) -> Result<Self, Self::Error>
            where
                Self: Sized,
            {
                use ::dnet::js::wrapper::Error;
                use ::wasm_bindgen::{JsCast, JsValue};
                use ::web_sys::js_sys::Array;

                let (stripped, transferables, into_transferables) = ::dnet::js::transferable::utils::destruct_into_transferable_data_object(
                    &object,
                )?;

                let stripped = context
                    .codec
                    .decode(&stripped.to_vec()[..])
                    .map_err(Error::DeserializationError)?;

                let into_transferables = Message2IntoTransferables::unpack(
                    into_transferables.expect("malformed transferables"),
                    context,
                )?;

                Ok(Self {
                    stripped,
                    transferables: transferables.to_vec(),
                    into_transferables,
                    _codec: std::marker::PhantomData,
                })
            }
        }
    };

    let parsed = parse2::<DeriveInput>(input).unwrap();
    let item = into_transferable_with_paths(parsed, paths()).unwrap();

    let actual = quote! {
        #item
    };

    compare(expected, actual);
}

#[test]
fn test_into_transferable_enum_unnamed_fields() {
    let input = quote! {
        #[derive(IntoTransferable)]
        pub enum Message {
            Variant(
                #[transferable]
                ArrayBuffer,

                u32,
            )
        }
    };

    let expected = quote! {
        impl<
            C,
        > ::dnet::js::IntoTransferable<
            ::dnet::js::wrapper::Context<C>,
            ::dnet::js::wrapper::Error<
                <C as ::dnet::Encode>::Error,
                <C as ::dnet::Decode>::Error,
            >,
        > for Message
        where
            C: ::dnet::Codec,
        {
            type Output = MessageWrapper<C>;
            fn into_transferable(self) -> Self::Output {
                MessageWrapper::from(self)
            }
        }

        impl<
            C,
        > ::dnet::js::FromTransferable<
            ::dnet::js::wrapper::Context<C>,
            ::dnet::js::wrapper::Error<
                <C as ::dnet::Encode>::Error,
                <C as ::dnet::Decode>::Error,
            >,
        > for Message
        where
            C: ::dnet::Codec,
        {
            type Input = MessageWrapper<C>;
            fn from_transferable(input: Self::Input) -> Self {
                use ::dnet::utils::unwrap::Unwrap;
                input.unwrap()
            }
        }

        #[derive(::serde::Serialize, ::serde::Deserialize)]
        pub enum MessageStripped {
            Variant { unnamed_field_1: u32, _phantom_data__: std::marker::PhantomData<()> },
        }

        pub struct MessageWrapper<C> {
            stripped: MessageStripped,
            transferables: Vec<::wasm_bindgen::JsValue>,
            _codec: std::marker::PhantomData<C>,
        }

        impl<C> From<Message> for MessageWrapper<C> {
            fn from(item: Message) -> Self {
                match item {
                    Message::Variant(unnamed_field_0, unnamed_field_1) => {
                        let stripped__ = MessageStripped::Variant {
                            unnamed_field_1,
                            _phantom_data__: std::marker::PhantomData,
                        };
                        let transferables__ = vec![unnamed_field_0.into(),];
                        Self {
                            stripped: stripped__,
                            transferables: transferables__,
                            _codec: std::marker::PhantomData,
                        }
                    }
                }
            }
        }

        impl<C> ::dnet::utils::unwrap::Unwrap for MessageWrapper<C> {
            type Output = Message;

            fn unwrap(self) -> Self::Output {
                use ::wasm_bindgen::JsCast;
                let MessageWrapper {
                    stripped: stripped__,
                    transferables: mut transferables__,
                    ..
                } = self;
                match stripped__ {
                    MessageStripped::Variant { unnamed_field_1, .. } => {
                        let unnamed_field_0 = transferables__
                            .pop()
                            .expect("malformed transferables")
                            .dyn_into()
                            .expect("malformed transferables");
                        Message::Variant(unnamed_field_0, unnamed_field_1)
                    }
                }
            }
        }

        impl<C> ::dnet::js::Transferable for MessageWrapper<C>
        where
            C: ::dnet::Codec,
        {
            type Context = ::dnet::js::wrapper::Context<C>;
            type Error = ::dnet::js::wrapper::Error<
                <C as ::dnet::Encode>::Error,
                <C as ::dnet::Decode>::Error,
            >;

            fn prepare_for_transfer(
                mut self,
                context: &mut Self::Context,
            ) -> Result<::dnet::js::WithTransferable, Self::Error> {
                use ::dnet::js::wrapper::Error;
                use ::wasm_bindgen::JsValue;
                use ::web_sys::js_sys::{Array, Uint8Array};

                context.buffer.clear();
                context
                    .codec
                    .encode(&mut context.buffer, &self.stripped)
                    .map_err(Error::SerializationError)?;

                let stripped = Uint8Array::from(&context.buffer[..]);
                let transferables = Array::of(&self.transferables);
                let mut transfer = self.transferables;
                transfer.push(stripped.buffer().into());
                let transfer = Array::of(&transfer);

                let into_transferables = None;

                let data = ::dnet::js::transferable::utils::construct_into_transferable_data_object(
                    &stripped,
                    &transferables,
                    into_transferables.as_ref(),
                );

                Ok(::dnet::js::WithTransferable {
                    data: data.into(),
                    transfer,
                })
            }

            fn reconstruct(
                object: ::wasm_bindgen::JsValue,
                context: &mut Self::Context,
            ) -> Result<Self, Self::Error>
            where
                Self: Sized,
            {
                use ::dnet::js::wrapper::Error;
                use ::wasm_bindgen::{JsCast, JsValue};
                use ::web_sys::js_sys::Array;

                let (stripped, transferables, into_transferables) = ::dnet::js::transferable::utils::destruct_into_transferable_data_object(
                    &object,
                )?;

                let stripped = context
                    .codec
                    .decode(&stripped.to_vec()[..])
                    .map_err(Error::DeserializationError)?;

                let _ = into_transferables;

                Ok(Self {
                    stripped,
                    transferables: transferables.to_vec(),
                    _codec: std::marker::PhantomData,
                })
            }
        }
    };

    let parsed = parse2::<DeriveInput>(input).unwrap();
    let item = into_transferable_with_paths(parsed, paths()).unwrap();

    let actual = quote! {
        #item
    };

    compare(expected, actual);
}
