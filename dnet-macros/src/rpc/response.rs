use convert_case::{Case, Casing};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Error, ItemTrait, TraitItem, TraitItemFn};

use crate::{
    rpc::{Paths, SerializationConfig},
    utils::{
        extract_return_type, is_into_transferable_attribute, is_no_ack, is_stream,
        is_transferable_attribute, request_response_derive,
    },
};

pub fn response(
    paths: &Paths,
    config: SerializationConfig,
    item: &ItemTrait,
) -> syn::Result<TokenStream> {
    let Paths {
        dnet_rpc: rpc_path, ..
    } = paths;

    let items = item.items.iter().try_fold::<_, _, Result<Vec<_>, Error>>(
        Vec::new(),
        |mut out, item| match item {
            TraitItem::Fn(method) => {
                let ident = format_ident!("{}", method.sig.ident.to_string().to_case(Case::Pascal));

                let return_type = extract_return_type(&method.sig.output)?;

                if is_stream(&method.sig.output) {
                    let transferable = transferable_attribute(config, method, true)?;
                    out.push(quote! { #ident(#transferable #rpc_path::producer::StreamResponse<#return_type>), });
                    Ok(out)
                } else if is_no_ack(method)? {
                    Ok(out)
                } else {
                    let transferable = transferable_attribute(config, method, false)?;
                    out.push(quote! { #ident(#transferable #return_type), });
                    Ok(out)
                }
            }
            _ => Ok(out),
        },
    )?;

    let derive = request_response_derive(paths, config);

    let output = quote! {
        #derive
        pub enum Response {
            #(#items)*
        }
    };
    Ok(output)
}

fn transferable_attribute(
    config: SerializationConfig,
    item: &TraitItemFn,
    stream: bool,
) -> syn::Result<TokenStream> {
    let is_transferable = item.attrs.iter().any(is_transferable_attribute);
    let is_into_transferable = item.attrs.iter().any(is_into_transferable_attribute);
    let is_both = is_transferable && is_into_transferable;
    let is_either = is_transferable || is_into_transferable;

    if matches!(config, SerializationConfig::Serde(_)) && is_either {
        return Err(Error::new_spanned(
            item,
            "the #[transferable] and #[into_transferable] attributes are only supported when using SerializationConfig::IntoTransferable",
        ));
    }

    if is_both {
        Err(Error::new_spanned(
            item,
            "a function cannot be marked with both #[transferable] and #[into_transferable]",
        ))
    } else if is_transferable {
        if stream {
            Err(Error::new_spanned(
                item,
                "the #[transferable] attribute is not supported on stream responses, use #[into_transferable] instead",
            ))
        } else {
            Ok(quote! { #[transferable] })
        }
    } else if is_into_transferable {
        Ok(quote! { #[into_transferable] })
    } else {
        Ok(quote! {})
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use crate::{
        rpc::{
            tests::{parsed_api, parsed_api_with_transferable, paths},
            SerdeConfig, SerializationConfig,
        },
        tests::compare,
    };

    use super::response;

    #[test]
    fn test_response() {
        let expected = quote! {
            #[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize)]
            pub enum Response {
                WaitForAck(()),
                AddNumbers(i32),
                ConcatenateStrings(String),
                StreamNaturalNumbers(::dnet_rpc::producer::StreamResponse<usize>),
                StreamTime(::dnet_rpc::producer::StreamResponse<NaiveTime>),
                LongRunningTask(String),
            }

        };

        let item = response(&paths(), Default::default(), &parsed_api()).unwrap();
        let actual = quote! {
            #item
        };

        compare(expected, actual);
    }

    #[test]
    fn test_response_no_serde() {
        let expected = quote! {
            #[derive(Debug, Clone)]
            pub enum Response {
                WaitForAck(()),
                AddNumbers(i32),
                ConcatenateStrings(String),
                StreamNaturalNumbers(::dnet_rpc::producer::StreamResponse<usize>),
                StreamTime(::dnet_rpc::producer::StreamResponse<NaiveTime>),
                LongRunningTask(String),
            }

        };

        let config = SerializationConfig::Serde(SerdeConfig::none());
        let item = response(&paths(), config, &parsed_api()).unwrap();
        let actual = quote! {
            #item
        };

        compare(expected, actual);
    }

    #[test]
    fn test_response_no_serde_serialization() {
        let expected = quote! {
            #[derive(Debug, Clone, ::serde::Deserialize)]
            pub enum Response {
                WaitForAck(()),
                AddNumbers(i32),
                ConcatenateStrings(String),
                StreamNaturalNumbers(::dnet_rpc::producer::StreamResponse<usize>),
                StreamTime(::dnet_rpc::producer::StreamResponse<NaiveTime>),
                LongRunningTask(String),
            }

        };

        let config = SerializationConfig::Serde(SerdeConfig {
            serialize: false,
            deserialize: true,
        });
        let item = response(&paths(), config, &parsed_api()).unwrap();
        let actual = quote! {
            #item
        };

        compare(expected, actual);
    }

    #[test]
    fn test_response_no_serde_deserialization() {
        let expected = quote! {
            #[derive(Debug, Clone, ::serde::Serialize)]
            pub enum Response {
                WaitForAck(()),
                AddNumbers(i32),
                ConcatenateStrings(String),
                StreamNaturalNumbers(::dnet_rpc::producer::StreamResponse<usize>),
                StreamTime(::dnet_rpc::producer::StreamResponse<NaiveTime>),
                LongRunningTask(String),
            }

        };

        let config = SerializationConfig::Serde(SerdeConfig {
            serialize: true,
            deserialize: false,
        });
        let item = response(&paths(), config, &parsed_api()).unwrap();
        let actual = quote! {
            #item
        };

        compare(expected, actual);
    }

    #[test]
    fn test_response_with_transferable() {
        let expected = quote! {
            #[derive(Debug, Clone, ::dnet_js::IntoTransferable)]
            pub enum Response {
                SendTransferable(()),
                GetData(#[into_transferable] Data),
            }

        };

        let config = SerializationConfig::IntoTransferable;
        let item = response(&paths(), config, &parsed_api_with_transferable()).unwrap();
        let actual = quote! {
            #item
        };

        compare(expected, actual);
    }
}
