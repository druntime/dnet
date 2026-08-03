use quote::quote;
use syn::{parse2, DeriveInput};

use crate::{
    tests::compare,
    transferable::{into_transferable_with_paths, tests::paths},
};

#[test]
fn test_into_transferable_struct() {
    let input = quote! {
        #[derive(IntoTransferable)]
        pub struct Message<G> {
            #[transferable]
            some_transferable: ArrayBuffer,

            _phantom: PhantomData<G>,
        }
    };

    let expected = quote! {
        impl<
            G,
            C,
        > ::dnet::js::IntoTransferable<
            ::dnet::js::wrapper::Context<C>,
            ::dnet::js::wrapper::Error<
                <C as ::dnet::Encode>::Error,
                <C as ::dnet::Decode>::Error,
            >,
        > for Message<G>
        where
            C: ::dnet::Codec,
        {
            type Output = MessageWrapper<G, C>;

            fn into_transferable(self) -> Self::Output {
                MessageWrapper::from(self)
            }
        }

        impl<
            G,
            C,
        > ::dnet::js::FromTransferable<
            ::dnet::js::wrapper::Context<C>,
            ::dnet::js::wrapper::Error<
                <C as ::dnet::Encode>::Error,
                <C as ::dnet::Decode>::Error,
            >,
        > for Message<G>
        where
            C: ::dnet::Codec,
        {
            type Input = MessageWrapper<G, C>;

            fn from_transferable(input: Self::Input) -> Self {
                use ::dnet::utils::unwrap::Unwrap;
                input.unwrap()
            }
        }

        #[derive(::serde::Serialize, ::serde::Deserialize)]
        pub struct MessageStripped<G> {
            _phantom: PhantomData<G>,
            _phantom_data__: std::marker::PhantomData<(G,)>,
        }

        pub struct MessageWrapper<G, C> {
            stripped: MessageStripped<G>,
            transferables: Vec<::wasm_bindgen::JsValue>,
            _codec: std::marker::PhantomData<C>,
        }

        impl<G, C> From<Message<G>> for MessageWrapper<G, C> {
            fn from(Message { some_transferable, _phantom }: Message<G>) -> Self {
                let stripped__ = MessageStripped {
                    _phantom,
                    _phantom_data__: std::marker::PhantomData,
                };
                let transferables__ = vec![some_transferable.into(),];
                Self {
                    stripped: stripped__,
                    transferables: transferables__,
                    _codec: std::marker::PhantomData,
                }
            }
        }

        impl<G, C> ::dnet::utils::unwrap::Unwrap for MessageWrapper<G, C> {
            type Output = Message<G>;

            fn unwrap(self) -> Self::Output {
                use ::wasm_bindgen::JsCast;
                let MessageWrapper {
                    stripped: stripped__,
                    transferables: mut transferables__,
                    ..
                } = self;
                let MessageStripped { _phantom, .. } = stripped__;
                let some_transferable = transferables__
                    .pop()
                    .expect("malformed transferables")
                    .dyn_into()
                    .expect("malformed transferables");
                Message {
                    some_transferable,
                    _phantom,
                }
            }
        }

        impl<G, C> ::dnet::js::Transferable for MessageWrapper<G, C>
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
fn test_into_transferable_nested_struct() {
    let input = quote! {
        #[derive(Clone, IntoTransferable)]
        pub struct Message2<G> where G: Clone + Copy {
            some_i32: i32,

            #[into_transferable]
            message: Message<G>,
        }
    };

    let expected = quote! {
        impl<
            G,
            C,
        > ::dnet::js::IntoTransferable<
            ::dnet::js::wrapper::Context<C>,
            ::dnet::js::wrapper::Error<
                <C as ::dnet::Encode>::Error,
                <C as ::dnet::Decode>::Error,
            >,
        > for Message2<G>
        where
            G: Clone + Copy,
            C: ::dnet::Codec,
        {
            type Output = Message2Wrapper<G, C>;

            fn into_transferable(self) -> Self::Output {
                Message2Wrapper::from(self)
            }
        }

        impl<
            G,
            C,
        > ::dnet::js::FromTransferable<
            ::dnet::js::wrapper::Context<C>,
            ::dnet::js::wrapper::Error<
                <C as ::dnet::Encode>::Error,
                <C as ::dnet::Decode>::Error,
            >,
        > for Message2<G>
        where
            G: Clone + Copy,
            C: ::dnet::Codec,
        {
            type Input = Message2Wrapper<G, C>;

            fn from_transferable(input: Self::Input) -> Self {
                use ::dnet::utils::unwrap::Unwrap;
                input.unwrap()
            }
        }

        #[derive(::serde::Serialize, ::serde::Deserialize)]
        pub struct Message2Stripped<G>
        where
            G: Clone + Copy,
        {
            some_i32: i32,
            _phantom_data__: std::marker::PhantomData<(G,)>,
        }

        pub struct Message2IntoTransferables<G, C>
        where
            G: Clone + Copy,
        {
            message: Message<G>,
            _phantom_data__: std::marker::PhantomData<(G, C)>,
        }

        impl<G, C> Message2IntoTransferables<G, C>
        where
            G: Clone + Copy,
        {
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
                let Message2IntoTransferables { message, .. } = self;
                let data__ = ::web_sys::js_sys::Array::new();
                let mut transfer__ = ::web_sys::js_sys::Array::new();
                {
                    let ::dnet::js::WithTransferable { data, transfer } = IntoTransferable::<
                        ::dnet::js::wrapper::Context<C>,
                        ::dnet::js::wrapper::Error<
                            <C as ::dnet::Encode>::Error,
                            <C as ::dnet::Decode>::Error,
                        >,
                    >::into_transferable(message)
                        .prepare_for_transfer(context__)?;
                    data__.push(&data);
                    transfer__ = transfer__.concat(&transfer);
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
                let message = FromTransferable::<
                    ::dnet::js::wrapper::Context<C>,
                    ::dnet::js::wrapper::Error<
                        <C as ::dnet::Encode>::Error,
                        <C as ::dnet::Decode>::Error,
                    >,
                >::from_transferable(Transferable::reconstruct(array__.pop(), context__)?);
                Ok(Message2IntoTransferables {
                    message,
                    _phantom_data__: std::marker::PhantomData,
                })
            }
        }

        pub struct Message2Wrapper<G, C>
        where
            G: Clone + Copy,
        {
            stripped: Message2Stripped<G>,
            transferables: Vec<::wasm_bindgen::JsValue>,
            into_transferables: Message2IntoTransferables<G, C>,
            _codec: std::marker::PhantomData<C>,
        }

        impl<G, C> From<Message2<G>> for Message2Wrapper<G, C>
        where
            G: Clone + Copy,
        {
            fn from(Message2 { some_i32, message }: Message2<G>) -> Self {
                let stripped__ = Message2Stripped {
                    some_i32,
                    _phantom_data__: std::marker::PhantomData,
                };
                let transferables__ = vec![];
                Self {
                    stripped: stripped__,
                    transferables: transferables__,
                    into_transferables: Message2IntoTransferables {
                        message,
                        _phantom_data__: std::marker::PhantomData,
                    },
                    _codec: std::marker::PhantomData,
                }
            }
        }

        impl<G, C> ::dnet::utils::unwrap::Unwrap for Message2Wrapper<G, C>
        where
            G: Clone + Copy,
        {
            type Output = Message2<G>;

            fn unwrap(self) -> Self::Output {
                use ::wasm_bindgen::JsCast;
                let Message2Wrapper {
                    stripped: stripped__,
                    transferables: mut transferables__,
                    into_transferables: into_transferables__,
                    ..
                } = self;
                let Message2Stripped { some_i32, .. } = stripped__;
                let Message2IntoTransferables { message, .. } = into_transferables__;
                Message2 { some_i32, message }
            }
        }

        impl<G, C> ::dnet::js::Transferable for Message2Wrapper<G, C>
        where
            G: Clone + Copy,
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
fn test_into_transferable_struct_unnamed_fields() {
    let input = quote! {
        #[derive(IntoTransferable)]
        pub struct Message (
            #[transferable]
            ArrayBuffer,

            u32,
        );
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
        pub struct MessageStripped {
            unnamed_field_1: u32,
            _phantom_data__: std::marker::PhantomData<()>,
        }

        pub struct MessageWrapper<C> {
            stripped: MessageStripped,
            transferables: Vec<::wasm_bindgen::JsValue>,
            _codec: std::marker::PhantomData<C>,
        }

        impl<C> From<Message> for MessageWrapper<C> {
            fn from(Message(unnamed_field_0, unnamed_field_1): Message) -> Self {
                let stripped__ = MessageStripped {
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

        impl<C> ::dnet::utils::unwrap::Unwrap for MessageWrapper<C> {
            type Output = Message;

            fn unwrap(self) -> Self::Output {
                use ::wasm_bindgen::JsCast;
                let MessageWrapper {
                    stripped: stripped__,
                    transferables: mut transferables__,
                    ..
                } = self;
                let MessageStripped { unnamed_field_1, .. } = stripped__;
                let unnamed_field_0 = transferables__
                    .pop()
                    .expect("malformed transferables")
                    .dyn_into()
                    .expect("malformed transferables");
                Message(unnamed_field_0, unnamed_field_1)
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
