use convert_case::{Case, Casing};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{parse2, Error, ItemTrait, Path, ReturnType, TraitItem, Type};

use crate::{
    rpc::Paths,
    utils::{
        extract_return_type, extract_stream_item_type, is_no_ack, is_stream, patternize_arguments,
    },
};

pub fn consumer(paths: &Paths, item: &ItemTrait) -> syn::Result<TokenStream> {
    let Paths {
        dnet_rpc: rpc_path, ..
    } = paths;

    let senders = consumer_senders(rpc_path, item)?;
    let state = consumer_state(rpc_path, item)?;

    let impl_consume = impl_consume(rpc_path, item)?;
    let impl_api = impl_api(rpc_path, item)?;

    let output = quote! {
        #senders

        #state

        #[derive(Debug)]
        pub struct Consumer {
            id_generator: #rpc_path::parts::consumer::RequestIdGenerator,
            senders: RequestSenders,
            drop_sender: Option<#rpc_path::futures::channel::oneshot::Sender<()>>,
        }

        #impl_consume

        #impl_api

        impl Drop for Consumer {
            fn drop(&mut self) {
                if let Some(sender) = self.drop_sender.take() {
                    let _ = sender.send(());
                }
            }
        }
    };
    Ok(output)
}

fn consumer_senders(rpc_path: &Path, item: &ItemTrait) -> syn::Result<TokenStream> {
    let items = item.items.iter().try_fold::<_, _, Result<Vec<_>, Error>>(
        Vec::new(),
        |mut out, item| match item {
            TraitItem::Fn(method) => {
                let ident = format_ident!(
                    "{}__sender",
                    method.sig.ident.to_string().to_case(Case::Snake)
                );
                let return_type = extract_return_type(&method.sig.output)?;
                let item = quote! {
                    #ident: #rpc_path::parts::consumer::RequestSender<Request, #return_type>,
                };
                out.push(item);
                Ok(out)
            }
            _ => Ok(out),
        },
    )?;

    let output = quote! {
        #[derive(Debug)]
        struct RequestSenders {
            #(#items)*
        }
    };
    Ok(output)
}

#[rustfmt::skip]
fn consumer_state(rpc_path: &Path, item: &ItemTrait) -> syn::Result<TokenStream> {
    let items = item.items.iter().try_fold::<_, _, Result<Vec<_>, Error>>(
        Vec::new(),
        |mut out, item| match item {
            TraitItem::Fn(method) => {
                let ident_requests = format_ident!("{}__requests", method.sig.ident.to_string().to_case(Case::Snake));
                let ident_receiver = format_ident!("{}__receiver", method.sig.ident.to_string().to_case(Case::Snake));
                let return_type = extract_return_type(&method.sig.output)?;

            let sender = if is_stream(&method.sig.output) {
                quote! {
                    (Option<#rpc_path::parts::consumer::StreamResultSender>,
                     #rpc_path::parts::consumer::StreamValuesSender<#return_type>)
                }
            } else {
                quote! {
                    #rpc_path::parts::consumer::ValueResultSender<#return_type>
                }
            };

            let requests = if is_no_ack(method)? {
                quote! { }
            } else { 
                quote! {
                    #ident_requests: std::collections::HashMap<#rpc_path::consumer::RequestId, #sender>,
                }
            };

            let receiver = quote! {
                #ident_receiver: #rpc_path::futures::channel::mpsc::UnboundedReceiver<
                    #rpc_path::parts::consumer::FullRequest<Request, #return_type>
                >,
            };

            let item = quote! {
                #requests
                #receiver
            };
            out.push(item);
            Ok(out)
        }
        _ => Ok(out),
    })?;

    let impl_consumer_state = impl_consumer_state(rpc_path, item)?;

    let output = quote! {
        #[derive(Debug)]
        struct ConsumerState {
            pending: usize,
            #(#items)*
        }

        #impl_consumer_state
    };
    Ok(output)
}

fn impl_consumer_state(rpc_path: &Path, item: &ItemTrait) -> syn::Result<TokenStream> {
    let mut channels = vec![];
    let mut senders = vec![];
    let mut items = vec![];
    let mut handlers = vec![];
    let mut drainers = vec![];

    for method in item.items.iter().filter_map(|item| match item {
        TraitItem::Fn(method) => Some(method),
        _ => None,
    }) {
        let ident = format_ident!("{}", method.sig.ident.to_string().to_case(Case::Pascal));
        let ident_requests = format_ident!(
            "{}__requests",
            method.sig.ident.to_string().to_case(Case::Snake)
        );
        let ident_sender = format_ident!(
            "{}__sender",
            method.sig.ident.to_string().to_case(Case::Snake)
        );
        let ident_receiver = format_ident!(
            "{}__receiver",
            method.sig.ident.to_string().to_case(Case::Snake)
        );

        channels.push(quote! {
            let (#ident_sender, #ident_receiver) = #rpc_path::parts::consumer::RequestSender::pair();
        });

        senders.push(quote! {
            #ident_sender,
        });

        let is_no_ack = is_no_ack(method)?;

        if is_no_ack {
            items.push(quote! {
                #ident_receiver,
            });
        } else {
            items.push(quote! {
                #ident_requests: std::collections::HashMap::new(),
                #ident_receiver,
            });
        }

        if !is_no_ack {
            handlers.push(if is_stream(&method.sig.output) {
                quote! {
                    Response::#ident(result) => {
                        #rpc_path::parts::consumer::handle_stream_response(result, id, &mut self.#ident_requests, &mut self.pending);
                    }
                }
            } else {
                quote! {
                    Response::#ident(result) => {
                        #rpc_path::parts::consumer::handle_response(result, id, &mut self.#ident_requests, &mut self.pending);
                    }
                }
            });
        }

        if is_stream(&method.sig.output) {
            drainers.push(quote! {
                for (_id, (mut sender, _)) in self.#ident_requests.drain() {
                    if let Some(sender) = sender.take() {
                        let _ = sender.send(Err(shutdown_type.into()));
                    }
                }
            });
        } else if !is_no_ack {
            drainers.push(quote! {
                for (_id, sender) in self.#ident_requests.drain() {
                    let _ = sender.send(Err(shutdown_type.into()));
                }
            });
        }
    }

    let output = quote! {
        impl ConsumerState {
            fn new() -> (RequestSenders, Self) {
                #(#channels)*

                let senders = RequestSenders {
                    #(#senders)*
                };

                let state = ConsumerState {
                    pending: 0,
                    #(#items)*
                };

                (senders, state)
            }
        }

        impl #rpc_path::parts::consumer::State<Response> for ConsumerState {
            fn handle_message(
                &mut self,
                message: #rpc_path::producer::Message<Response>,
            ) -> Result<(), #rpc_path::ShutdownType> {
                match message {
                    #rpc_path::producer::Message::Response { id, response } => {
                        match response {
                            #(#handlers)*
                        }
                        Ok(())
                    }
                    #rpc_path::producer::Message::Aborted => Err(#rpc_path::ShutdownType::Aborted),
                    #rpc_path::producer::Message::Shutdown => Err(#rpc_path::ShutdownType::Shutdown),
                }
            }

            fn idle(&self) -> bool {
                self.pending == 0
            }

            fn shutdown(&mut self, shutdown_type: #rpc_path::ShutdownType) {
                #(#drainers)*
            }
        }
    };
    Ok(output)
}

#[rustfmt::skip]
fn impl_consume(rpc_path: &Path, item: &ItemTrait) -> syn::Result<TokenStream> {
    let items = item.items.iter().try_fold::<_, _, Result<Vec<_>, Error>>(
        Vec::new(),
        |mut out, item| match item {
            TraitItem::Fn(method) => {
                let ident_option = format_ident!(
                    "{}__option",
                    method.sig.ident.to_string().to_case(Case::Snake)
                );
            let ident_receiver = format_ident!(
                "{}__receiver",
                method.sig.ident.to_string().to_case(Case::Snake)
            );
            let ident_requests = format_ident!(
                "{}__requests",
                method.sig.ident.to_string().to_case(Case::Snake)
            );

            let handler = if is_stream(&method.sig.output) {
                quote! {
                    #rpc_path::parts::consumer::handle_new_stream_request(
                        #ident_option, 
                        timeout.is_some(),
                        &mut timeout_future,
                        &mut state.#ident_requests,
                        &mut sender,
                        &mut state.pending,
                    ).await;
                }
            } else if is_no_ack(method)? {
                quote! {
                    #rpc_path::parts::consumer::handle_new_no_ack_request(
                        #ident_option, 
                        timeout.is_some(),
                        &mut timeout_future,
                        &mut sender,
                    ).await;
                }
            } else {
                quote! {
                    #rpc_path::parts::consumer::handle_new_request(
                        #ident_option, 
                        timeout.is_some(),
                        &mut timeout_future,
                        &mut state.#ident_requests,
                        &mut sender,
                        &mut state.pending,
                    ).await;
                }
            };

            let item = quote! {
                #ident_option = #rpc_path::futures::StreamExt::next(&mut state.#ident_receiver) => {
                    #handler
                },
            };
            out.push(item);
            Ok(out)
        }
        _ => Ok(out),
    })?;

    let implementation = quote! {
        use #rpc_path::futures::{FutureExt, SinkExt, StreamExt};
        use #rpc_path::parts::consumer::State;

        let #rpc_path::consumer::Configuration {
            shutdown,
            timeout,
            ..
        } = configuration;

        let (drop_sender, mut drop_receiver) = #rpc_path::futures::channel::oneshot::channel::<()>();
        let drop_sender = Some(drop_sender);

        let (senders, mut state) = ConsumerState::new();

        #rpc_path::spawn(async move {
            let (to_other_side, mine) = #rpc_path::parts::transports();
            let _pipe = #rpc_path::parts::Pipe::new(to_other_side, transport, Default::default(), error_handler);
            let transport = mine;

            let (mut sender, receiver) = transport.split();
            let receiver = #rpc_path::futures::StreamExt::fuse(receiver);

            let timeout_future = if let Some(duration) = timeout {
                #rpc_path::Timeout::new(duration)
            } else {
                #rpc_path::Timeout::never()
            };
            let shutdown = #rpc_path::futures::FutureExt::fuse(shutdown);
            #rpc_path::futures::pin_mut!(receiver, timeout_future, shutdown);

            loop {
                let mut should_break = false;
                #rpc_path::futures::select! {
                    receive_option = #rpc_path::futures::StreamExt::next(&mut receiver) => {
                        #rpc_path::parts::consumer::handle_producer_message(
                            receive_option,
                            timeout.is_some(),
                            &mut timeout_future,
                            &mut state,
                            &mut should_break
                        );
                    },
                    #(#items)*
                    _ = &mut timeout_future => {
                        if timeout.is_some() && !state.idle() {
                            state.shutdown(#rpc_path::ShutdownType::Timeout);
                            should_break = true;
                        }
                    },
                    shutdown_type = &mut shutdown => {
                        state.shutdown(shutdown_type);
                        should_break = true;
                    }
                    _ = &mut drop_receiver => { should_break = true; }
                }
                if should_break {
                    break;
                }
            }
        });

        Consumer {
            id_generator: #rpc_path::parts::consumer::RequestIdGenerator::new(),
            senders,
            drop_sender,
        }
    };

    let output = quote! {
        impl #rpc_path::consumer::Consume<Consumer> for Consumer {
            type Request = Request;
            type Response = Response;

            fn consume<Transport, Error>(
                transport: Transport,
                configuration: #rpc_path::consumer::Configuration,
                error_handler: #rpc_path::consumer::ErrorHandler<Self::Request, Error>,
            ) -> Consumer
            where
                Transport: #rpc_path::consumer::Transport<Self::Request, Self::Response, Error>,
                Error: #rpc_path::TransportError,
            {
                #implementation
            }
        }
    };
    Ok(output)
}

fn impl_api(rpc_path: &Path, item: &ItemTrait) -> syn::Result<TokenStream> {
    let ident = &item.ident;

    let items = item.items.iter().try_fold::<_, _, Result<Vec<_>, Error>>(
        Vec::new(),
        |mut out, item| match item {
            TraitItem::Fn(method) => {
                let ident_request =
                    format_ident!("{}", method.sig.ident.to_string().to_case(Case::Pascal));
                let ident_sender = format_ident!(
                    "{}__sender",
                    method.sig.ident.to_string().to_case(Case::Snake)
                );

                let mut signature = method.sig.clone();
                signature.asyncness = None;
                match &signature.output {
                    ReturnType::Default => {
                        signature.output =
                            parse2::<ReturnType>(quote!(-> #rpc_path::ValueRequest<Request, ()>))
                                .unwrap();
                    }
                    ReturnType::Type(_, return_type) => match *return_type.to_owned() {
                        Type::ImplTrait(impl_trait) => {
                            let return_type = extract_stream_item_type(&impl_trait)?;
                            signature.output = parse2::<ReturnType>(
                                quote!(-> #rpc_path::StreamRequest<Request, #return_type>),
                            )
                            .unwrap();
                        }
                        _ => {
                            signature.output = parse2::<ReturnType>(
                                quote!(-> #rpc_path::ValueRequest<Request, #return_type>),
                            )
                            .unwrap();
                        }
                    },
                };

                let ident_request_future = if is_stream(&method.sig.output) {
                    quote!(#rpc_path::StreamRequest)
                } else {
                    quote!(#rpc_path::ValueRequest)
                };

                let arguments = patternize_arguments(&method.sig.inputs);

                let item = quote! {
                    #signature {
                        let request = Request::#ident_request { #arguments };
                        #ident_request_future::new(
                            self.senders.#ident_sender.clone(),
                            self.id_generator.id(),
                            request,
                        )
                    }
                };
                out.push(item);
                Ok(out)
            }
            _ => Ok(out),
        },
    )?;

    let output = quote! {
        impl #ident for Consumer {
            type Request = Request;

            #(#items)*
        }
    };
    Ok(output)
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use crate::{rpc::tests::parsed_api, tests::compare, utils::make_path};

    use super::{consumer_senders, consumer_state, impl_api, impl_consume};

    #[test]
    fn test_consumer_senders() {
        let expected = quote! {
            #[derive(Debug)]
            struct RequestSenders {
                hello_world__sender: ::dnet_rpc::parts::consumer::RequestSender<Request, ()>,
                wait_for_ack__sender: ::dnet_rpc::parts::consumer::RequestSender<Request, ()>,
                add_numbers__sender: ::dnet_rpc::parts::consumer::RequestSender<Request, i32>,
                concatenate_strings__sender: ::dnet_rpc::parts::consumer::RequestSender<Request, String>,
                stream_natural_numbers__sender: ::dnet_rpc::parts::consumer::RequestSender<Request, usize>,
                stream_time__sender: ::dnet_rpc::parts::consumer::RequestSender<Request, NaiveTime>,
                long_running_task__sender: ::dnet_rpc::parts::consumer::RequestSender<Request, String>,
            }
        };

        let rpc_path = make_path(&["dnet_rpc"]);
        let item = consumer_senders(&rpc_path, &parsed_api()).unwrap();
        let actual = quote! {
            #item
        };

        compare(expected, actual);
    }

    #[test]
    fn test_consumer_state() {
        let expected = quote! {
            #[derive(Debug)]
            struct ConsumerState {
                pending: usize,
                hello_world__receiver: ::dnet_rpc::futures::channel::mpsc::UnboundedReceiver<
                    ::dnet_rpc::parts::consumer::FullRequest<Request, ()>,
                >,
                wait_for_ack__requests: std::collections::HashMap<
                    ::dnet_rpc::consumer::RequestId,
                    ::dnet_rpc::parts::consumer::ValueResultSender<()>,
                >,
                wait_for_ack__receiver: ::dnet_rpc::futures::channel::mpsc::UnboundedReceiver<
                    ::dnet_rpc::parts::consumer::FullRequest<Request, ()>,
                >,
                add_numbers__requests: std::collections::HashMap<
                    ::dnet_rpc::consumer::RequestId,
                    ::dnet_rpc::parts::consumer::ValueResultSender<i32>,
                >,
                add_numbers__receiver: ::dnet_rpc::futures::channel::mpsc::UnboundedReceiver<
                    ::dnet_rpc::parts::consumer::FullRequest<Request, i32>,
                >,
                concatenate_strings__requests: std::collections::HashMap<
                    ::dnet_rpc::consumer::RequestId,
                    ::dnet_rpc::parts::consumer::ValueResultSender<String>,
                >,
                concatenate_strings__receiver: ::dnet_rpc::futures::channel::mpsc::UnboundedReceiver<
                    ::dnet_rpc::parts::consumer::FullRequest<Request, String>,
                >,
                stream_natural_numbers__requests: std::collections::HashMap<
                    ::dnet_rpc::consumer::RequestId,
                    (
                        Option<::dnet_rpc::parts::consumer::StreamResultSender>,
                        ::dnet_rpc::parts::consumer::StreamValuesSender<usize>,
                    ),
                >,
                stream_natural_numbers__receiver: ::dnet_rpc::futures::channel::mpsc::UnboundedReceiver<
                    ::dnet_rpc::parts::consumer::FullRequest<Request, usize>,
                >,
                stream_time__requests: std::collections::HashMap<
                    ::dnet_rpc::consumer::RequestId,
                    (
                        Option<::dnet_rpc::parts::consumer::StreamResultSender>,
                        ::dnet_rpc::parts::consumer::StreamValuesSender<NaiveTime>,
                    ),
                >,
                stream_time__receiver: ::dnet_rpc::futures::channel::mpsc::UnboundedReceiver<
                    ::dnet_rpc::parts::consumer::FullRequest<Request, NaiveTime>,
                >,
                long_running_task__requests: std::collections::HashMap<
                    ::dnet_rpc::consumer::RequestId,
                    ::dnet_rpc::parts::consumer::ValueResultSender<String>,
                >,
                long_running_task__receiver: ::dnet_rpc::futures::channel::mpsc::UnboundedReceiver<
                    ::dnet_rpc::parts::consumer::FullRequest<Request, String>,
                >,
            }
            impl ConsumerState {
                fn new() -> (RequestSenders, Self) {
                    let (hello_world__sender, hello_world__receiver) = ::dnet_rpc::parts::consumer::RequestSender::pair();
                    let (wait_for_ack__sender, wait_for_ack__receiver) = ::dnet_rpc::parts::consumer::RequestSender::pair();
                    let (add_numbers__sender, add_numbers__receiver) = ::dnet_rpc::parts::consumer::RequestSender::pair();
                    let (concatenate_strings__sender, concatenate_strings__receiver) = ::dnet_rpc::parts::consumer::RequestSender::pair();
                    let (stream_natural_numbers__sender, stream_natural_numbers__receiver) = ::dnet_rpc::parts::consumer::RequestSender::pair();
                    let (stream_time__sender, stream_time__receiver) = ::dnet_rpc::parts::consumer::RequestSender::pair();
                    let (long_running_task__sender, long_running_task__receiver) = ::dnet_rpc::parts::consumer::RequestSender::pair();
                    let senders = RequestSenders {
                        hello_world__sender,
                        wait_for_ack__sender,
                        add_numbers__sender,
                        concatenate_strings__sender,
                        stream_natural_numbers__sender,
                        stream_time__sender,
                        long_running_task__sender,
                    };
                    let state = ConsumerState {
                        pending: 0,
                        hello_world__receiver,
                        wait_for_ack__requests: std::collections::HashMap::new(),
                        wait_for_ack__receiver,
                        add_numbers__requests: std::collections::HashMap::new(),
                        add_numbers__receiver,
                        concatenate_strings__requests: std::collections::HashMap::new(),
                        concatenate_strings__receiver,
                        stream_natural_numbers__requests: std::collections::HashMap::new(),
                        stream_natural_numbers__receiver,
                        stream_time__requests: std::collections::HashMap::new(),
                        stream_time__receiver,
                        long_running_task__requests: std::collections::HashMap::new(),
                        long_running_task__receiver,
                    };
                    (senders, state)
                }
            }

            impl ::dnet_rpc::parts::consumer::State<Response> for ConsumerState {
                fn handle_message(
                    &mut self,
                    message: ::dnet_rpc::producer::Message<Response>,
                ) -> Result<(), ::dnet_rpc::ShutdownType> {
                    match message {
                        ::dnet_rpc::producer::Message::Response { id, response } => {
                            match response {
                                Response::WaitForAck(result) => {
                                    ::dnet_rpc::parts::consumer::handle_response(
                                        result,
                                        id,
                                        &mut self.wait_for_ack__requests,
                                        &mut self.pending,
                                    );
                                }
                                Response::AddNumbers(result) => {
                                    ::dnet_rpc::parts::consumer::handle_response(
                                        result,
                                        id,
                                        &mut self.add_numbers__requests,
                                        &mut self.pending,
                                    );
                                }
                                Response::ConcatenateStrings(result) => {
                                    ::dnet_rpc::parts::consumer::handle_response(
                                        result,
                                        id,
                                        &mut self.concatenate_strings__requests,
                                        &mut self.pending,
                                    );
                                }
                                Response::StreamNaturalNumbers(result) => {
                                    ::dnet_rpc::parts::consumer::handle_stream_response(
                                        result,
                                        id,
                                        &mut self.stream_natural_numbers__requests,
                                        &mut self.pending,
                                    );
                                }
                                Response::StreamTime(result) => {
                                    ::dnet_rpc::parts::consumer::handle_stream_response(
                                        result,
                                        id,
                                        &mut self.stream_time__requests,
                                        &mut self.pending,
                                    );
                                }
                                Response::LongRunningTask(result) => {
                                    ::dnet_rpc::parts::consumer::handle_response(
                                        result,
                                        id,
                                        &mut self.long_running_task__requests,
                                        &mut self.pending,
                                    );
                                }
                            }
                            Ok(())
                        }
                        ::dnet_rpc::producer::Message::Aborted => {
                            Err(::dnet_rpc::ShutdownType::Aborted)
                        }
                        ::dnet_rpc::producer::Message::Shutdown => {
                            Err(::dnet_rpc::ShutdownType::Shutdown)
                        }
                    }
                }
                fn idle(&self) -> bool {
                    self.pending == 0
                }
                fn shutdown(&mut self, shutdown_type: ::dnet_rpc::ShutdownType) {
                    for (_id, sender) in self.wait_for_ack__requests.drain() {
                        let _ = sender.send(Err(shutdown_type.into()));
                    }
                    for (_id, sender) in self.add_numbers__requests.drain() {
                        let _ = sender.send(Err(shutdown_type.into()));
                    }
                    for (_id, sender) in self.concatenate_strings__requests.drain() {
                        let _ = sender.send(Err(shutdown_type.into()));
                    }
                    for (_id, (mut sender, _)) in self.stream_natural_numbers__requests.drain() {
                        if let Some(sender) = sender.take() {
                            let _ = sender.send(Err(shutdown_type.into()));
                        }
                    }
                    for (_id, (mut sender, _)) in self.stream_time__requests.drain() {
                        if let Some(sender) = sender.take() {
                            let _ = sender.send(Err(shutdown_type.into()));
                        }
                    }
                    for (_id, sender) in self.long_running_task__requests.drain() {
                        let _ = sender.send(Err(shutdown_type.into()));
                    }
                }
            }
        };

        let rpc_path = make_path(&["dnet_rpc"]);
        let item = consumer_state(&rpc_path, &parsed_api()).unwrap();
        let actual = quote! {
            #item
        };

        compare(expected, actual);
    }

    #[test]
    fn test_impl_consume() {
        let select = quote! {
            receive_option = ::dnet_rpc::futures::StreamExt::next(& mut receiver) => {
                ::dnet_rpc::parts::consumer::handle_producer_message(
                    receive_option,
                    timeout.is_some(),
                    &mut timeout_future,
                    &mut state,
                    &mut should_break
                );
            },
            hello_world__option = ::dnet_rpc::futures::StreamExt::next(& mut state.hello_world__receiver) => {
                ::dnet_rpc::parts::consumer::handle_new_no_ack_request(
                    hello_world__option,
                    timeout.is_some(),
                    &mut timeout_future,
                    &mut sender,
                ).await;
            },
            wait_for_ack__option = ::dnet_rpc::futures::StreamExt::next(& mut state.wait_for_ack__receiver) => {
                ::dnet_rpc::parts::consumer::handle_new_request(
                    wait_for_ack__option,
                    timeout.is_some(),
                    &mut timeout_future,
                    &mut state.wait_for_ack__requests,
                    &mut sender,
                    &mut state.pending,
                ).await;
            },
            add_numbers__option = ::dnet_rpc::futures::StreamExt::next(& mut state.add_numbers__receiver) => {
                ::dnet_rpc::parts::consumer::handle_new_request(
                    add_numbers__option,
                    timeout.is_some(),
                    &mut timeout_future,
                    &mut state.add_numbers__requests,
                    &mut sender,
                    &mut state.pending,
                ).await;
            },
            concatenate_strings__option = ::dnet_rpc::futures::StreamExt::next(& mut state.concatenate_strings__receiver) => {
                ::dnet_rpc::parts::consumer::handle_new_request(
                    concatenate_strings__option,
                    timeout.is_some(),
                    &mut timeout_future,
                    &mut state.concatenate_strings__requests,
                    &mut sender,
                    &mut state.pending,
                ).await;
            },
            stream_natural_numbers__option = ::dnet_rpc::futures::StreamExt::next(&mut state.stream_natural_numbers__receiver) => {
                ::dnet_rpc::parts::consumer::handle_new_stream_request(
                    stream_natural_numbers__option,
                    timeout.is_some(),
                    &mut timeout_future,
                    &mut state.stream_natural_numbers__requests,
                    &mut sender,
                    &mut state.pending,
                ).await;
            },
            stream_time__option = ::dnet_rpc::futures::StreamExt::next(&mut state.stream_time__receiver) => {
                ::dnet_rpc::parts::consumer::handle_new_stream_request(
                    stream_time__option,
                    timeout.is_some(),
                    &mut timeout_future,
                    &mut state.stream_time__requests,
                    &mut sender,
                    &mut state.pending,
                ).await;
            },
            long_running_task__option = ::dnet_rpc::futures::StreamExt::next(&mut state.long_running_task__receiver) => {
                ::dnet_rpc::parts::consumer::handle_new_request(
                    long_running_task__option,
                    timeout.is_some(),
                    &mut timeout_future,
                    &mut state.long_running_task__requests,
                    &mut sender,
                    &mut state.pending,
                ).await;
            },
            _ = & mut timeout_future => {
                if timeout.is_some() && ! state.idle() {
                    state.shutdown(::dnet_rpc::ShutdownType::Timeout);
                    should_break = true;
                }
            },
            shutdown_type = &mut shutdown => {
                state.shutdown(shutdown_type);
                should_break = true;
            }
            _ = &mut drop_receiver => { should_break = true; }
        };

        let expected = quote! {
            impl ::dnet_rpc::consumer::Consume<Consumer> for Consumer {
                type Request = Request;
                type Response = Response;
                fn consume<Transport, Error>(
                    transport: Transport,
                    configuration: ::dnet_rpc::consumer::Configuration,
                    error_handler: ::dnet_rpc::consumer::ErrorHandler<Self::Request, Error>,
                ) -> Consumer
                where
                    Transport: ::dnet_rpc::consumer::Transport<Self::Request, Self::Response, Error>,
                    Error: ::dnet_rpc::TransportError,
                {
                    use ::dnet_rpc::futures::{FutureExt, SinkExt, StreamExt};
                    use ::dnet_rpc::parts::consumer::State;
                    let ::dnet_rpc::consumer::Configuration {
                        shutdown,
                        timeout,
                        ..
                    } = configuration;
                    let (drop_sender, mut drop_receiver) =
                        ::dnet_rpc::futures::channel::oneshot::channel::<()>();
                    let drop_sender = Some(drop_sender);
                    let (senders, mut state) = ConsumerState::new();
                    ::dnet_rpc::spawn(async move {
                        let (to_other_side, mine) = ::dnet_rpc::parts::transports();
                        let _pipe = ::dnet_rpc::parts::Pipe::new(
                            to_other_side,
                            transport,
                            Default::default(),
                            error_handler,
                        );
                        let transport = mine;
                        let (mut sender, receiver) = transport.split();
                        let receiver = ::dnet_rpc::futures::StreamExt::fuse(receiver);
                        let timeout_future = if let Some(duration) = timeout {
                            ::dnet_rpc::Timeout::new(duration)
                        } else {
                            ::dnet_rpc::Timeout::never()
                        };
                        let shutdown = ::dnet_rpc::futures::FutureExt::fuse(shutdown);
                        ::dnet_rpc::futures::pin_mut!(receiver, timeout_future, shutdown);
                        loop {
                            let mut should_break = false;
                            ::dnet_rpc::futures::select! {
                                #select
                            }
                            if should_break {
                                break;
                            }
                        }
                    });
                    Consumer {
                        id_generator: ::dnet_rpc::parts::consumer::RequestIdGenerator::new(),
                        senders,
                        drop_sender,
                    }
                }
            }

        };

        let rpc_path = make_path(&["dnet_rpc"]);
        let item = impl_consume(&rpc_path, &parsed_api()).unwrap();
        let actual = quote! {
            #item
        };

        compare(expected, actual);
    }

    #[test]
    fn test_impl_api() {
        let expected = quote! {
            impl Api for Consumer {
                type Request = Request;
                fn hello_world(&self) -> ::dnet_rpc::ValueRequest<Request, ()> {
                    let request = Request::HelloWorld {};
                    ::dnet_rpc::ValueRequest::new(
                        self.senders.hello_world__sender.clone(),
                        self.id_generator.id(),
                        request,
                    )
                }
                fn wait_for_ack(&self) -> ::dnet_rpc::ValueRequest<Request, ()> {
                    let request = Request::WaitForAck {};
                    ::dnet_rpc::ValueRequest::new(
                        self.senders.wait_for_ack__sender.clone(),
                        self.id_generator.id(),
                        request,
                    )
                }
                fn add_numbers(
                    &self,
                    a: i32,
                    b: i32,
                ) -> ::dnet_rpc::ValueRequest<Request, i32> {
                    let request = Request::AddNumbers { a, b };
                    ::dnet_rpc::ValueRequest::new(
                        self.senders.add_numbers__sender.clone(),
                        self.id_generator.id(),
                        request,
                    )
                }
                fn concatenate_strings(
                    &self,
                    a: String,
                    b: String,
                ) -> ::dnet_rpc::ValueRequest<Request, String> {
                    let request = Request::ConcatenateStrings {
                        a,
                        b,
                    };
                    ::dnet_rpc::ValueRequest::new(
                        self.senders.concatenate_strings__sender.clone(),
                        self.id_generator.id(),
                        request,
                    )
                }
                fn stream_natural_numbers(
                    &self,
                ) -> ::dnet_rpc::StreamRequest<Request, usize> {
                    let request = Request::StreamNaturalNumbers {};
                    ::dnet_rpc::StreamRequest::new(
                        self.senders.stream_natural_numbers__sender.clone(),
                        self.id_generator.id(),
                        request,
                    )
                }
                fn stream_time(
                    &self,
                    interval: Duration,
                ) -> ::dnet_rpc::StreamRequest<Request, NaiveTime> {
                    let request = Request::StreamTime { interval };
                    ::dnet_rpc::StreamRequest::new(
                        self.senders.stream_time__sender.clone(),
                        self.id_generator.id(),
                        request,
                    )
                }
                fn long_running_task(
                    &self,
                    input: u32
                ) -> ::dnet_rpc::ValueRequest<Request, String> {
                    let request = Request::LongRunningTask { input };
                    ::dnet_rpc::ValueRequest::new(
                        self.senders.long_running_task__sender.clone(),
                        self.id_generator.id(),
                        request,
                    )
                }
            }
        };

        let rpc_path = make_path(&["dnet_rpc"]);
        let item = impl_api(&rpc_path, &parsed_api()).unwrap();
        let actual = quote! {
            #item
        };

        compare(expected, actual);
    }
}
