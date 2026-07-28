use proc_macro2::TokenStream;
use quote::quote;
use syn::{punctuated::Punctuated, Error, Generics, Ident, Token, Visibility};

use crate::transferable::{
    impl_into_and_from_transferable, impl_transferable_for_wrapper, pack_transferables,
    process_fields, unpack_transferables, wrapper, CommonParams, Fields, Idents, Paths, Wrapper,
};

struct Item {
    pub ident: Ident,
    pub generics: Generics,
    pub variants: Vec<Variant>,
}

impl Item {
    fn has_into_transferables(&self) -> bool {
        self.variants.iter().any(Variant::has_into_transferables)
    }
}

struct Variant {
    ident: Ident,
    fields: Fields,
}

impl Variant {
    fn has_into_transferables(&self) -> bool {
        self.fields.into_transferables.len() > 1
    }
}

pub fn expand_enum(
    params: CommonParams,
    variants: Punctuated<syn::Variant, Token![,]>,
) -> syn::Result<TokenStream> {
    let CommonParams {
        paths,
        visibility,
        idents,
        generics,
        phantom,
        wrapper: wrapper_params,
        codec: codec_param,
    } = params;

    let Idents {
        item: item_ident,
        into_transferables: into_transferables_ident,
        stripped: stripped_ident,
        wrapper: _wrapper_ident,
    } = idents;

    let variants: Result<_, Error> = variants
        .into_iter()
        .map(
            |variant| match process_fields(variant.fields, &phantom, &codec_param) {
                Ok(fields) => Ok(Variant {
                    ident: variant.ident,
                    fields,
                }),
                Err(err) => Err(err),
            },
        )
        .collect();

    let item_params = Item {
        ident: item_ident,
        generics,
        variants: variants?,
    };

    let impl_into_and_from_transferable = impl_into_and_from_transferable(
        &paths,
        &item_params.ident,
        &item_params.generics,
        &wrapper_params,
        &codec_param,
    );

    let stripped = stripped(&paths, &visibility, &item_params, &stripped_ident);

    let into_transferables = into_transferables(
        &paths,
        &visibility,
        &into_transferables_ident,
        &item_params,
        &wrapper_params.generics,
        &codec_param,
    );

    let into_transferables_ident = if item_params.has_into_transferables() {
        Some(&into_transferables_ident)
    } else {
        None
    };

    let (_, item_generics, item_where_clause) = item_params.generics.split_for_impl();

    let wrapper = wrapper(
        &paths,
        &visibility,
        &item_generics,
        &item_where_clause,
        &stripped_ident,
        &into_transferables_ident,
        &wrapper_params,
        &codec_param,
    );

    let impl_from_for_wrapper = impl_from_for_wrapper(
        &item_params,
        &stripped_ident,
        &into_transferables_ident,
        &wrapper_params,
    );

    let impl_unwrap_for_wrapper = impl_unwrap_for_wrapper(
        &paths,
        &item_params,
        &stripped_ident,
        &into_transferables_ident,
        &wrapper_params,
    );

    let impl_transferable_for_wrapper = impl_transferable_for_wrapper(
        &paths,
        &wrapper_params,
        &codec_param,
        &into_transferables_ident,
    );

    Ok(quote! {
        #impl_into_and_from_transferable

        #stripped

        #into_transferables

        #wrapper

        #impl_from_for_wrapper

        #impl_unwrap_for_wrapper

        #impl_transferable_for_wrapper
    })
}

fn stripped(
    paths: &Paths,
    visibility: &Visibility,
    item: &Item,
    stripped_ident: &Ident,
) -> TokenStream {
    let Paths { serde, .. } = paths;
    let Item {
        generics, variants, ..
    } = item;

    let (_, item_generics, item_where_clause) = generics.split_for_impl();

    let stripped_variants = variants.iter().map(
        |Variant {
             ident,
             fields:
                 Fields {
                     stripped: stripped_fields,
                     ..
                 },
         }| {
            quote! {
                #ident {
                    #stripped_fields,
                }
            }
        },
    );

    quote! {
        #[derive(#serde::Serialize, #serde::Deserialize)]
        #visibility enum #stripped_ident #item_generics #item_where_clause {
            #(#stripped_variants,)*
        }
    }
}

fn into_transferables(
    paths: &Paths,
    visibility: &Visibility,
    ident: &Ident,
    item: &Item,
    wrapper_generics: &Generics,
    codec_param: &Ident,
) -> TokenStream {
    if item.has_into_transferables() {
        let Paths {
            dnet_base,
            dnet_js,
            wasm_bindgen,
            web_sys,
            ..
        } = paths;

        let Item {
            generics, variants, ..
        } = item;

        let (_, _, where_clause) = generics.split_for_impl();

        let (impl_generics, generics, _) = wrapper_generics.split_for_impl();

        let enum_variants = variants.iter().map(
            |Variant {
                 ident,
                 fields: Fields {
                     into_transferables, ..
                 },
             }| {
                quote! {
                    #ident {
                       #into_transferables
                    }
                }
            },
        );

        let pack_variants = variants.iter().enumerate().map(
            |(
                index,
                Variant {
                    ident: variant_ident,
                    fields:
                        Fields {
                            into_transferables_names,
                            ..
                        },
                },
            )| {
                let pushers = into_transferables_names.iter().map(|name| {
                    quote! {
                        {
                            let #dnet_js::WithTransferable { data, transfer } = IntoTransferable::<
                                #dnet_js::wrapper::Context<#codec_param>,
                                #dnet_js::wrapper::Error<
                                    <#codec_param as #dnet_base::Encode>::Error,
                                    <#codec_param as #dnet_base::Decode>::Error
                                >
                            >::into_transferable(#name).prepare_for_transfer(context__)?;
                            data__.push(&data);
                            transfer__ = transfer__.concat(&transfer);
                        }
                    }
                });

                quote! {
                    #ident::#variant_ident {
                       #(#into_transferables_names,)*
                       ..
                    } => {
                        #(#pushers)*

                        // push variant index
                        data__.push(&#wasm_bindgen::JsValue::from_f64(#index as f64));
                    }
                }
            },
        );

        let unpack_variants = variants.iter().enumerate().map(
            |(
                index,
                Variant {
                    ident: variant_ident,
                    fields:
                        Fields {
                            into_transferables_names,
                            ..
                        },
                },
            )| {
                // note: we are popping from array - so reconstruct in reverse order
                let poppers = into_transferables_names.iter().rev().map(|name| {
                    quote! {
                        let #name = FromTransferable::<
                            #dnet_js::wrapper::Context<#codec_param>,
                            #dnet_js::wrapper::Error<
                                <#codec_param as #dnet_base::Encode>::Error,
                                <#codec_param as #dnet_base::Decode>::Error
                            >
                        >::from_transferable(Transferable::reconstruct(array__.pop(), context__)?);
                    }
                });

                quote! {
                    #index => {
                        #(#poppers)*

                        Ok(#ident::#variant_ident {
                            #(#into_transferables_names,)*
                            _phantom_data__: std::marker::PhantomData,
                        })
                    }
                }
            },
        );

        quote! {
            #visibility enum #ident #generics #where_clause {
                #(#enum_variants,)*
            }

            impl #impl_generics #ident #generics #where_clause {
                fn pack(
                    self,
                    context__: &mut #dnet_js::wrapper::Context<#codec_param>
                ) -> Result<
                    #dnet_js::WithTransferable,
                    #dnet_js::wrapper::Error<
                        <#codec_param as #dnet_base::Encode>::Error,
                        <#codec_param as #dnet_base::Decode>::Error
                    >
                >
                where
                    #codec_param: #dnet_base::Codec,
                {
                    use #dnet_js::{IntoTransferable, Transferable as _};

                    let data__ = #web_sys::js_sys::Array::new();
                    let mut transfer__ = #web_sys::js_sys::Array::new();

                    match self {
                        #(#pack_variants)*
                    }

                    Ok(#dnet_js::WithTransferable {
                        data: data__.into(),
                        transfer: transfer__,
                    })
                }

                fn unpack(
                    value__: #wasm_bindgen::JsValue,
                    context__: &mut #dnet_js::wrapper::Context<#codec_param>,
                ) -> Result<
                    Self,
                    #dnet_js::wrapper::Error<
                        <#codec_param as #dnet_base::Encode>::Error,
                        <#codec_param as #dnet_base::Decode>::Error
                    >
                >
                where
                    #codec_param: #dnet_base::Codec,
                {
                    use #dnet_js::{FromTransferable, Transferable};
                    use #wasm_bindgen::JsCast;

                    let array__: #web_sys::js_sys::Array =
                        value__.dyn_into().expect("malformed into_transferables");

                    // pop variant index
                    let variant_index__: usize =
                        array__.pop().as_f64().expect("malformed into_transferables") as usize;

                    match variant_index__ {
                        #(#unpack_variants)*
                        _ => unreachable!("invalid into_transferables variant received"),
                    }
                }
            }
        }
    } else {
        quote! {}
    }
}

fn impl_from_for_wrapper(
    item: &Item,
    stripped_ident: &Ident,
    into_transferables_ident: &Option<&Ident>,
    wrapper: &Wrapper,
) -> TokenStream {
    let Item {
        ident: item_ident,
        generics,
        variants,
    } = item;
    let Wrapper {
        ident: wrapper_ident,
        generics: wrapper_generics,
    } = wrapper;

    let (_, item_generics, item_where_clause) = generics.split_for_impl();

    let (wrapper_impl_generics, wrapper_generics, _) = wrapper_generics.split_for_impl();

    let has_into_transferables = item.has_into_transferables();

    let variants = variants.iter().map(
        |Variant {
             ident,
             fields:
                 Fields {
                     names: fields_names,
                     stripped_names: stripped_fields_names,
                     transferables_names,
                     into_transferables_names,
                     were_unnamed,
                     ..
                 },
         }| {
            let transferables = pack_transferables(transferables_names);

            let into_transferables = if has_into_transferables {
                quote! {
                    into_transferables: #into_transferables_ident::#ident {
                        #(#into_transferables_names,)*
                        _phantom_data__: std::marker::PhantomData,
                    },
                }
            } else {
                quote! {}
            };

            let destructor = if *were_unnamed {
                quote! {
                    #item_ident::#ident (
                        #(#fields_names,)*
                    )
                }
            } else {
                quote! {
                    #item_ident::#ident {
                        #(#fields_names,)*
                    }
                }
            };

            quote! {
                #destructor => {
                    let stripped__ = #stripped_ident::#ident {
                        #(#stripped_fields_names,)*
                        _phantom_data__: std::marker::PhantomData,
                    };

                    #transferables

                    Self {
                        stripped: stripped__,
                        transferables: transferables__,
                        #into_transferables
                        _codec: std::marker::PhantomData,
                    }
                }
            }
        },
    );

    quote! {
        impl #wrapper_impl_generics From<#item_ident #item_generics> for #wrapper_ident #wrapper_generics
        #item_where_clause
        {
            fn from(item: #item_ident #item_generics) -> Self {
                match item {
                    #(#variants,)*
                }
            }
        }
    }
}

fn impl_unwrap_for_wrapper(
    paths: &Paths,
    item: &Item,
    stripped_ident: &Ident,
    into_transferables_ident: &Option<&Ident>,
    wrapper: &Wrapper,
) -> TokenStream {
    let Paths {
        dnet_utils,
        wasm_bindgen,
        ..
    } = paths;
    let Item {
        ident: item_ident,
        generics,
        variants,
    } = item;
    let Wrapper {
        ident: wrapper_ident,
        generics: wrapper_generics,
    } = wrapper;

    let (_, item_generics, item_where_clause) = generics.split_for_impl();

    let (wrapper_impl_generics, wrapper_generics, _) = wrapper_generics.split_for_impl();

    let has_into_transferables = item.has_into_transferables();

    let variants = variants.iter().map(
        |Variant {
             ident,
             fields:
                 Fields {
                     names: fields_names,
                     stripped_names: stripped_fields_names,
                     transferables_names,
                     into_transferables_names,
                     were_unnamed,
                     ..
                 },
         }| {
            let transferables = unpack_transferables(transferables_names);

            let into_transferables_destructor = if has_into_transferables {
                quote! {
                    let #into_transferables_ident::#ident {
                        #(#into_transferables_names,)*
                        ..
                    } = into_transferables__ else {
                        unreachable!("invalid into_transferables variant encountered")
                    };
                }
            } else {
                quote! {}
            };

            let constructor = if *were_unnamed {
                quote! {
                    #item_ident::#ident (
                        #(#fields_names,)*
                    )
                }
            } else {
                quote! {
                    #item_ident::#ident {
                        #(#fields_names,)*
                    }
                }
            };

            quote! {
                #stripped_ident::#ident {
                    #(#stripped_fields_names,)*
                    ..
                } => {
                    #transferables

                    #into_transferables_destructor

                    #constructor
                }
            }
        },
    );

    let into_transferables_field = if has_into_transferables {
        quote! {
            into_transferables: into_transferables__,
        }
    } else {
        quote! {}
    };

    quote! {
        impl #wrapper_impl_generics #dnet_utils::unwrap::Unwrap for #wrapper_ident #wrapper_generics
        #item_where_clause
        {
            type Output = #item_ident #item_generics;

            fn unwrap(self) -> Self::Output {
                use #wasm_bindgen::JsCast;

                let #wrapper_ident {
                    stripped: stripped__,
                    transferables: mut transferables__,
                    #into_transferables_field
                    ..
                } = self;

                match stripped__ {
                    #(#variants,)*
                }
            }
        }
    }
}
