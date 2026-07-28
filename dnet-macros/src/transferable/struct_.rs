use proc_macro2::TokenStream;
use quote::quote;
use syn::{punctuated::Punctuated, Field, Generics, Ident, Token, Visibility};

use crate::transferable::{
    impl_into_and_from_transferable, impl_transferable_for_wrapper, pack_transferables,
    process_fields, unpack_transferables, wrapper, CommonParams, Fields, Idents, Paths, Wrapper,
};

struct Item {
    pub ident: Ident,
    pub generics: Generics,
    pub fields_names: Vec<Ident>,
}

struct IntoTransferables {
    pub ident: Ident,
    pub generics: Generics,
    pub fields: Punctuated<Field, Token![,]>,
    pub fields_names: Vec<Ident>,
}

impl IntoTransferables {
    fn has_into_transferables(&self) -> bool {
        self.fields.len() > 1
    }
}

struct Stripped {
    pub ident: Ident,
    pub fields: Punctuated<Field, Token![,]>,
    pub fields_names: Vec<Ident>,
}

pub fn expand_struct(params: CommonParams, fields: syn::Fields) -> syn::Result<TokenStream> {
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

    let Fields {
        names: fields_names,
        stripped: stripped_fields,
        stripped_names: stripped_fields_names,
        transferables_names,
        into_transferables: into_transferables_fields,
        into_transferables_names,
        were_unnamed: were_fields_unnamed,
    } = process_fields(fields, &phantom, &codec_param)?;

    let item_params = Item {
        ident: item_ident,
        generics,
        fields_names,
    };

    let into_transferable_params = IntoTransferables {
        ident: into_transferables_ident,
        generics: item_params.generics.clone(),
        fields: into_transferables_fields,
        fields_names: into_transferables_names,
    };

    let stripped_params = Stripped {
        ident: stripped_ident,
        fields: stripped_fields,
        fields_names: stripped_fields_names,
    };

    let impl_into_and_from_transferable = impl_into_and_from_transferable(
        &paths,
        &item_params.ident,
        &item_params.generics,
        &wrapper_params,
        &codec_param,
    );

    let stripped = stripped(&paths, &visibility, &item_params, &stripped_params);

    let into_transferables = into_transferables(
        &paths,
        &visibility,
        &into_transferable_params,
        &wrapper_params.generics,
        &codec_param,
    );

    let into_transferables_ident = if into_transferable_params.has_into_transferables() {
        Some(&into_transferable_params.ident)
    } else {
        None
    };

    let (_, item_generics, item_where_clause) = item_params.generics.split_for_impl();

    let wrapper = wrapper(
        &paths,
        &visibility,
        &item_generics,
        &item_where_clause,
        &stripped_params.ident,
        &into_transferables_ident,
        &wrapper_params,
        &codec_param,
    );

    let impl_from_for_wrapper = impl_from_for_wrapper(
        &item_params,
        &stripped_params,
        &wrapper_params,
        &transferables_names,
        &into_transferable_params,
        were_fields_unnamed,
    );

    let impl_unwrap_for_wrapper = impl_unwrap_for_wrapper(
        &paths,
        &item_params,
        &stripped_params,
        &wrapper_params,
        &transferables_names,
        &into_transferable_params,
        were_fields_unnamed,
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
    stripped: &Stripped,
) -> TokenStream {
    let Paths { serde, .. } = paths;

    let Item {
        generics: item_generics,
        ..
    } = item;

    let Stripped {
        ident: stripped_ident,
        fields: stripped_fields,
        ..
    } = stripped;

    let (_, item_generics, item_where_clause) = item_generics.split_for_impl();

    quote! {
        #[derive(#serde::Serialize, #serde::Deserialize)]
        #visibility struct #stripped_ident #item_generics #item_where_clause {
            #stripped_fields
        }
    }
}

fn into_transferables(
    paths: &Paths,
    visibility: &Visibility,
    into_transferables: &IntoTransferables,
    wrapper_generics: &Generics,
    codec_param: &Ident,
) -> TokenStream {
    if into_transferables.has_into_transferables() {
        let Paths {
            dnet_base,
            dnet_js,
            wasm_bindgen,
            web_sys,
            ..
        } = paths;

        let IntoTransferables {
            ident,
            generics,
            fields,
            fields_names,
        } = into_transferables;

        let (_, _, where_clause) = generics.split_for_impl();

        let (impl_generics, generics, _) = wrapper_generics.split_for_impl();

        let pushers = fields_names.iter().map(|name| {
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

        // note: we are popping from array - so reconstruct in reverse order
        let poppers = fields_names.iter().rev().map(|name| {
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
            #visibility struct #ident #generics #where_clause {
                #fields
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

                    let #ident { #(#fields_names,)* .. } = self;

                    let data__ = #web_sys::js_sys::Array::new();
                    let mut transfer__ = #web_sys::js_sys::Array::new();

                    #(#pushers)*

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
                    C: #dnet_base::Codec,
                {
                    use #dnet_js::{FromTransferable, Transferable};
                    use #wasm_bindgen::JsCast;

                    let array__: #web_sys::js_sys::Array =
                        value__.dyn_into().expect("malformed into_transferables");

                    #(#poppers)*

                    Ok(#ident {
                        #(#fields_names,)*
                        _phantom_data__: std::marker::PhantomData,
                    })
                }
            }
        }
    } else {
        quote! {}
    }
}

fn impl_from_for_wrapper(
    item: &Item,
    stripped: &Stripped,
    wrapper: &Wrapper,
    transferables_names: &[Ident],
    into_transferables: &IntoTransferables,
    were_fields_unnamed: bool,
) -> TokenStream {
    let Item {
        ident: item_ident,
        generics: item_generics,
        fields_names: item_fields_names,
    } = item;

    let Stripped {
        ident: stripped_ident,
        fields_names: stripped_fields_names,
        ..
    } = stripped;

    let Wrapper {
        ident: wrapper_ident,
        generics: wrapper_generics,
    } = wrapper;

    let (_, item_generics, item_where_clause) = item_generics.split_for_impl();

    let (wrapper_impl_generics, wrapper_generics, _) = wrapper_generics.split_for_impl();

    let transferables = pack_transferables(transferables_names);

    let into_transferables = if into_transferables.has_into_transferables() {
        let IntoTransferables {
            ident,
            fields_names,
            ..
        } = into_transferables;
        quote! {
            into_transferables: #ident {
                #(#fields_names,)*
                _phantom_data__: std::marker::PhantomData,
            },
        }
    } else {
        quote! {}
    };

    let destructor = if were_fields_unnamed {
        quote! {
            #item_ident (
                #(#item_fields_names,)*
            )
        }
    } else {
        quote! {
            #item_ident {
                #(#item_fields_names,)*
            }
        }
    };

    quote! {
        impl #wrapper_impl_generics From<#item_ident #item_generics> for #wrapper_ident #wrapper_generics
        #item_where_clause
        {
            fn from(#destructor: #item_ident #item_generics) -> Self {
                let stripped__ = #stripped_ident {
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
    }
}

fn impl_unwrap_for_wrapper(
    paths: &Paths,
    item: &Item,
    stripped: &Stripped,
    wrapper: &Wrapper,
    transferables_names: &[Ident],
    into_transferables: &IntoTransferables,
    were_fields_unnamed: bool,
) -> TokenStream {
    let Paths {
        dnet_utils,
        wasm_bindgen,
        ..
    } = paths;

    let Item {
        ident: item_ident,
        generics: item_generics,
        fields_names: item_fields_names,
    } = item;

    let Stripped {
        ident: stripped_ident,
        fields_names: stripped_fields_names,
        ..
    } = stripped;

    let Wrapper {
        ident: wrapper_ident,
        generics: wrapper_generics,
    } = wrapper;

    let (_, item_generics, item_where_clause) = item_generics.split_for_impl();

    let (wrapper_impl_generics, wrapper_generics, _) = wrapper_generics.split_for_impl();

    let transferables = unpack_transferables(transferables_names);

    let (into_transferables_field, into_transferables_destructor) =
        if into_transferables.has_into_transferables() {
            let IntoTransferables {
                ident,
                fields_names,
                ..
            } = into_transferables;
            (
                quote! {
                    into_transferables: into_transferables__,
                },
                quote! {
                    let #ident {
                        #(#fields_names,)*
                        ..
                    } = into_transferables__;
                },
            )
        } else {
            (quote! {}, quote! {})
        };

    let constructor = if were_fields_unnamed {
        quote! {
            #item_ident (
                #(#item_fields_names,)*
            )
        }
    } else {
        quote! {
            #item_ident {
                #(#item_fields_names,)*
            }
        }
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
                let #stripped_ident {
                    #(#stripped_fields_names,)*
                    ..
                } = stripped__;

                #transferables

                #into_transferables_destructor

                #constructor
            }
        }
    }
}
