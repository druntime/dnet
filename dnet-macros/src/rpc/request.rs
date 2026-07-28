use convert_case::{Case, Casing};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemTrait, TraitItem};

use crate::{
    rpc::{Paths, SerializationConfig},
    utils::{request_response_derive, skip_self},
};

pub fn request(paths: &Paths, config: SerializationConfig, item: &ItemTrait) -> TokenStream {
    let items = item.items.iter().filter_map(|item| match item {
        TraitItem::Fn(method) => {
            let ident = format_ident!("{}", method.sig.ident.to_string().to_case(Case::Pascal));
            let args = skip_self(&method.sig.inputs);
            let item = quote! {
                #ident { #args },
            };
            Some(item)
        }
        _ => None,
    });

    let derive = request_response_derive(paths, config);

    let output = quote! {
        #derive
        pub enum Request {
           #(#items)*
        }
    };
    output
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

    use super::request;

    #[test]
    fn test_request() {
        let expected = quote! {
            #[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize)]
            pub enum Request {
                HelloWorld {},
                WaitForAck {},
                AddNumbers { a: i32, b: i32 },
                ConcatenateStrings { a: String, b: String },
                StreamNaturalNumbers {},
                StreamTime { interval: Duration },
                LongRunningTask { input: u32 },
            }
        };

        let item = request(&paths(), Default::default(), &parsed_api());
        let actual = quote! {
            #item
        };

        compare(expected, actual);
    }

    #[test]
    fn test_request_no_serde() {
        let expected = quote! {
            #[derive(Debug, Clone)]
            pub enum Request {
                HelloWorld {},
                WaitForAck {},
                AddNumbers { a: i32, b: i32 },
                ConcatenateStrings { a: String, b: String },
                StreamNaturalNumbers {},
                StreamTime { interval: Duration },
                LongRunningTask { input: u32 },
            }
        };

        let config = SerializationConfig::Serde(SerdeConfig::none());
        let item = request(&paths(), config, &parsed_api());
        let actual = quote! {
            #item
        };

        compare(expected, actual);
    }

    #[test]
    fn test_request_no_serde_serialization() {
        let expected = quote! {
            #[derive(Debug, Clone, ::serde::Deserialize)]
            pub enum Request {
                HelloWorld {},
                WaitForAck {},
                AddNumbers { a: i32, b: i32 },
                ConcatenateStrings { a: String, b: String },
                StreamNaturalNumbers {},
                StreamTime { interval: Duration },
                LongRunningTask { input: u32 },
            }
        };

        let config = SerializationConfig::Serde(SerdeConfig {
            serialize: false,
            deserialize: true,
        });
        let item = request(&paths(), config, &parsed_api());
        let actual = quote! {
            #item
        };

        compare(expected, actual);
    }

    #[test]
    fn test_request_no_serde_deserialization() {
        let expected = quote! {
            #[derive(Debug, Clone, ::serde::Serialize)]
            pub enum Request {
                HelloWorld {},
                WaitForAck {},
                AddNumbers { a: i32, b: i32 },
                ConcatenateStrings { a: String, b: String },
                StreamNaturalNumbers {},
                StreamTime { interval: Duration },
                LongRunningTask { input: u32 },
            }
        };

        let config = SerializationConfig::Serde(SerdeConfig {
            serialize: true,
            deserialize: false,
        });
        let item = request(&paths(), config, &parsed_api());
        let actual = quote! {
            #item
        };

        compare(expected, actual);
    }

    #[test]
    fn test_request_with_transferable() {
        let expected = quote! {
            #[derive(Debug, Clone, ::dnet_js::IntoTransferable)]
            pub enum Request {
                SendTransferable { #[transferable] data: OffscreenCanvas },
                GetData {},
            }
        };

        let config = SerializationConfig::IntoTransferable;
        let item = request(&paths(), config, &parsed_api_with_transferable());
        let actual = quote! {
            #item
        };

        compare(expected, actual);
    }
}
