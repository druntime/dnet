use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use syn::{
    parse2, punctuated::Punctuated, token::Bracket, AttrStyle, Attribute, FnArg, ItemTrait, Meta,
    Path, PathSegment, ReturnType, Token, TraitItem, Type,
};
use uuid::Uuid;

use crate::{
    rpc::{
        consumer::consumer, producer::impl_produce, request::request, response::response, Paths,
        SerdeConfig, SerializationConfig,
    },
    utils::{
        extract_stream_item_type, has_methods_with_transferable, is_transferable_attribute,
        no_serde_attribute, remove_transferable_attributes,
    },
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ApiSerializationConfig {
    pub request: SerializationConfig,
    pub response: SerializationConfig,
}

impl ApiSerializationConfig {
    pub fn from_no_serde_attribute(attribute: &Attribute) -> syn::Result<Self> {
        match &attribute.meta {
            // bare `#[no_serde]` - disable both request and response fully
            Meta::Path(_) => Ok(ApiSerializationConfig {
                request: SerializationConfig::Serde(SerdeConfig::none()),
                response: SerializationConfig::Serde(SerdeConfig::none()),
            }),

            // `#[no_serde(a, b, ...)]`
            Meta::List(list) => {
                let idents =
                    list.parse_args_with(Punctuated::<Path, Token![,]>::parse_terminated)?;

                // start with all enabled, selectively disable
                let mut request = SerdeConfig::all();
                let mut response = SerdeConfig::all();

                for path in &idents {
                    let ident = path
                        .get_ident()
                        .ok_or_else(|| syn::Error::new_spanned(path, "expected identifier"))?;

                    match ident.to_string().as_str() {
                        "request" => request = SerdeConfig::none(),
                        "response" => response = SerdeConfig::none(),
                        "request_serialize" => request.serialize = false,
                        "request_deserialize" => request.deserialize = false,
                        "response_serialize" => response.serialize = false,
                        "response_deserialize" => response.deserialize = false,
                        other => {
                            return Err(syn::Error::new_spanned(
                                ident,
                                format!("unknown no_serde parameter `{other}`"),
                            ))
                        }
                    }
                }

                let request = SerializationConfig::Serde(request);
                let response = SerializationConfig::Serde(response);
                Ok(ApiSerializationConfig { request, response })
            }

            Meta::NameValue(name_value) => Err(syn::Error::new_spanned(
                name_value,
                "#[no_serde] does not support `key = value` syntax",
            )),
        }
    }
}

pub fn api(item: TokenStream) -> syn::Result<TokenStream> {
    let paths = Paths::default()?;
    api_with_paths(item, paths)
}

fn api_with_paths(item: TokenStream, paths: Paths) -> syn::Result<TokenStream> {
    let mut item = parse2::<ItemTrait>(item)?;

    let no_serde_attribute = no_serde_attribute(&item.attrs)?;
    let mut config = if let Some(attribute) = no_serde_attribute {
        ApiSerializationConfig::from_no_serde_attribute(attribute)?
    } else {
        ApiSerializationConfig::default()
    };

    let is_transferable = item.attrs.iter().any(is_transferable_attribute);

    if no_serde_attribute.is_some() && is_transferable {
        return Err(syn::Error::new_spanned(
            item,
            "the #[transferable] attribute automatically implies #[no_serde] - remove the #[no_serde] attribute",
        ));
    }

    let is_transferable_implied_from_methods = has_methods_with_transferable(&item);
    if no_serde_attribute.is_some() && is_transferable_implied_from_methods {
        return Err(syn::Error::new_spanned(
            item,
            "use of the #[transferable]/#[into_transferable] attribute(s) inside API implies #[no_serde] - remove the #[no_serde] attribute",
        ));
    }

    let is_transferable = is_transferable || is_transferable_implied_from_methods;
    if is_transferable {
        config.request = SerializationConfig::IntoTransferable;
        config.response = SerializationConfig::IntoTransferable;
    }

    let request = request(&paths, config.request, &item);
    let response = response(&paths, config.response, &item)?;

    for method in item.items.iter_mut().filter_map(|item| {
        if let TraitItem::Fn(func) = item {
            Some(func)
        } else {
            None
        }
    }) {
        remove_transferable_attributes(&mut method.attrs);

        for input in method.sig.inputs.iter_mut() {
            if let FnArg::Typed(input) = input {
                remove_transferable_attributes(&mut input.attrs);
            }
        }
    }

    let item_modified = modify_trait(&paths, item.clone())?;

    let consumer = consumer(&paths, &item)?;
    let impl_produce = impl_produce(&paths, &item, Uuid::new_v4())?;

    Ok(quote! {
        #item_modified

        #request

        #response

        #consumer

        #impl_produce
    })
}

fn modify_trait(paths: &Paths, mut item: ItemTrait) -> syn::Result<ItemTrait> {
    let Paths {
        dnet_rpc: rpc_path, ..
    } = paths;

    if item.generics.lt_token.is_some() {
        Err(syn::Error::new_spanned(
            &item.generics,
            "generic traits are not supported in #[api]",
        ))?;
    }

    item.items.push(
        parse2::<TraitItem>(quote! {
            /// Request arguments.
            type Request;
        })
        .unwrap(),
    );

    let must_use = format_ident!("must_use");
    let must_use = Attribute {
        pound_token: Token!(#)(Span::call_site()),
        style: AttrStyle::Outer,
        bracket_token: Bracket(Span::call_site()),
        meta: Meta::Path(Path {
            leading_colon: None,
            segments: Punctuated::from_iter([PathSegment::from(must_use)]),
        }),
    };

    for method in item.items.iter_mut().filter_map(|item| {
        if let TraitItem::Fn(func) = item {
            Some(func)
        } else {
            None
        }
    }) {
        if method.sig.asyncness.is_none() {
            Err(syn::Error::new_spanned(
                &method.sig,
                "all api methods must be marked as \"async\"",
            ))?;
        }

        if method.sig.generics.lt_token.is_some() {
            Err(syn::Error::new_spanned(
                &method.sig,
                "generic methods are not supported",
            ))?;
        }

        method.sig.asyncness = None;
        method.attrs.push(must_use.clone());

        match &method.sig.output {
            ReturnType::Default => {
                method.sig.output =
                    parse2::<ReturnType>(quote!(-> #rpc_path::ValueRequest<Request, ()>)).unwrap();
            }
            ReturnType::Type(_, return_type) => match *return_type.to_owned() {
                Type::ImplTrait(impl_trait) => {
                    let return_type = extract_stream_item_type(&impl_trait)?;
                    method.sig.output = parse2::<ReturnType>(
                        quote!(-> #rpc_path::StreamRequest<Request, #return_type>),
                    )
                    .unwrap();
                }
                _ => {
                    method.sig.output = parse2::<ReturnType>(
                        quote!(-> #rpc_path::ValueRequest<Request, #return_type>),
                    )
                    .unwrap();
                }
            },
        };
    }

    Ok(item)
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use crate::{
        rpc::tests::{parsed_api, paths},
        tests::compare,
    };

    use super::modify_trait;

    #[test]
    fn test_modify_trait() {
        let expected = quote! {
            /// Public api.
            pub trait Api {
                /// Print "Hello World!" message on the server.
                #[no_ack]
                #[must_use]
                fn hello_world(&self)  -> ::dnet_rpc::ValueRequest<Request, ()>;

                /// Wait for an acknowledgement from the producer.
                #[must_use]
                fn wait_for_ack(&self) -> ::dnet_rpc::ValueRequest<Request, ()>;

                /// Add two integers.
                #[must_use]
                fn add_numbers(&self, a: i32, b: i32) -> ::dnet_rpc::ValueRequest<Request, i32>;

                /// Concatenate two strings.
                #[must_use]
                fn concatenate_strings(
                    &self,
                    a: String,
                    b: String,
                ) -> ::dnet_rpc::ValueRequest<Request, String>;

                /// Create stream of consequent natural numbers.
                #[must_use]
                fn stream_natural_numbers(&self) -> ::dnet_rpc::StreamRequest<Request, usize>;

                /// Keep sending server time at given interval.
                #[must_use]
                fn stream_time(&self, interval: Duration) -> ::dnet_rpc::StreamRequest<Request, NaiveTime>;

                /// Abortable long-running task.
                #[abortable]
                #[must_use]
                fn long_running_task(&self, input: u32) -> ::dnet_rpc::ValueRequest<Request, String>;

                /// Request arguments.
                type Request;
            }
        };

        let item = modify_trait(&paths(), parsed_api()).unwrap();
        let actual = quote! {
            #item
        };

        compare(expected, actual);
    }
}
