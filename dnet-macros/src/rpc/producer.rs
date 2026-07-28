use convert_case::{Case, Casing};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{DeriveInput, Error, ItemTrait, TraitItem};
use uuid::Uuid;

use crate::{
    rpc::Paths,
    utils::{is_abortable, is_no_ack, is_stream, patternize_arguments, rpc_path},
};

pub fn produce(input: DeriveInput) -> syn::Result<TokenStream> {
    let rpc_path = rpc_path()?;
    let DeriveInput { ident, .. } = input;
    Ok(quote! {
        impl #rpc_path::producer::Produce for #ident {
            type Request = Request;
            type Response = Response;

            fn produce<Transport, Error>(
                self,
                transport: Transport,
                configuration: #rpc_path::producer::Configuration,
                error_handler: #rpc_path::producer::ErrorHandler<Self::Response, Error>,
            ) -> #rpc_path::JoinHandle<#rpc_path::ShutdownType>
            where
                Transport: #rpc_path::producer::Transport<Self::Request, Self::Response, Error>,
                Error: #rpc_path::TransportError
            {
                impl_produce!(self, transport, configuration, error_handler)
            }
        }
    })
}

#[rustfmt::skip]
pub fn impl_produce(paths: &Paths, item: &ItemTrait, random_id: Uuid) -> syn::Result<TokenStream> {
    let Paths { dnet_rpc: rpc_path, .. } = paths;

    let items = item.items.iter().try_fold::<_, _, Result<Vec<_>, Error>>(
        Vec::new(),
        |mut out, item| match item {
            TraitItem::Fn(method) => {
                let ident = method.sig.ident.clone();
                let ident_request =
                    format_ident!("{}", method.sig.ident.to_string().to_case(Case::Pascal));

            let is_abortable = is_abortable(method);

            let pattern = patternize_arguments(&method.sig.inputs);
            let mut arguments = pattern.clone();
            let abortion_token = if is_abortable {
                arguments.push(format_ident!("abortion_token"));
                quote! {
                    let aborter_token = #rpc_path::producer::abortable::AborterToken::new();
                    let abortion_token = #rpc_path::producer::abortable::AbortionToken::new(aborter_token.clone());
                    let task_aborter = Some(aborter_token);
                }
            } else {
                quote! {
                    let task_aborter = None;
                }
            };

            let task = if is_abortable {
                quote! {
                    let task = async move{ me.#ident(#arguments).await };
                }
            } else {
                quote! {
                    let task = async move{ Result::<_, std::convert::Infallible>::Ok(me.#ident(#arguments).await) };
                }
            };

            let item = if is_stream(&method.sig.output) {
                quote! {
                    Request::#ident_request { #pattern } => {
                        let me = me.clone();
                        #abortion_token
                        #task
                        #rpc_path::parts::producer::handle_stream_request(
                            id,
                            task,
                            Response::#ident_request,
                            reply_sender.clone(),
                            remove_aborter_sender.clone(),
                            abort_receiver,
                            task_aborter,
                        );
                    },
                }
            } else if is_no_ack(method)? {
                quote! {
                    Request::#ident_request { #pattern } => {
                        let me = me.clone();
                        #abortion_token
                        #task
                        #rpc_path::parts::producer::handle_no_ack_request(
                            id,
                            task,
                            remove_aborter_sender.clone(),
                            abort_receiver,
                            task_aborter,
                        );
                    },
                }
            } else {
                quote! {
                    Request::#ident_request { #pattern } => {
                        let me = me.clone();
                        #abortion_token
                        #task
                        #rpc_path::parts::producer::handle_request(
                            id,
                            task,
                            Response::#ident_request,
                            reply_sender.clone(),
                            remove_aborter_sender.clone(),
                            abort_receiver,
                            task_aborter,
                        );
                    },
                }
            };

            out.push(item);
            Ok(out)
        }
        _ => Ok(out),
    })?;

    let ident = format_ident!("__impl_produce_{}", random_id.simple().to_string());

    let output = quote! {
        #[doc(hidden)]
        #[macro_export]
        macro_rules! #ident {
            ($self:ident, $transport:ident, $configuration:ident, $error_handler:ident) => {
                #rpc_path::spawn(async move {
                    use std::sync::Arc;
                    use std::collections::HashMap;
                    use futures::{
                        channel::{
                            mpsc::unbounded,
                            oneshot,
                        },
                        pin_mut, select, StreamExt,
                    };
                    use #rpc_path::{ShutdownType, Timeout};

                    let (to_other_side, mine) = #rpc_path::parts::transports();
                    let _pipe = #rpc_path::parts::Pipe::new(to_other_side, $transport, Default::default(), $error_handler);
                    let transport = mine;

                    let me = Arc::new($self);

                    let (mut sender, receiver) = transport.split();
                    let mut receiver = #rpc_path::futures::StreamExt::fuse(receiver);

                    let (reply_sender, mut reply_receiver) = unbounded::<#rpc_path::producer::Message<Response>>();

                    let mut aborters: HashMap<u64, oneshot::Sender<()>> = HashMap::new();
                    let (remove_aborter_sender, mut remove_aborter_receiver) = unbounded::<u64>();

                    let (stop_sender, stop_receiver) = oneshot::channel::<ShutdownType>();
                    let mut stop_sender = Some(stop_sender);

                    let #rpc_path::producer::Configuration {
                        shutdown,
                        timeout,
                        ..
                    } = $configuration;

                    #rpc_path::spawn(async move {
                        while let Some(message) = #rpc_path::futures::StreamExt::next(&mut reply_receiver).await {
                            #rpc_path::parts::producer::handle_response(message, &mut sender, &mut stop_sender).await;
                        }
                    });

                    let handle_message = |aborters: &mut HashMap<u64, oneshot::Sender<()>>, message: #rpc_path::consumer::Message<Request>| {
                        let #rpc_path::consumer::Message { id, payload } = message;
                        match payload {
                            #rpc_path::consumer::Payload::Request(request) => {
                                let (abort_sender, abort_receiver) = oneshot::channel::<()>();
                                aborters.insert(id, abort_sender);
                                match request {
                                     #(#items)*
                                }
                            }
                            #rpc_path::consumer::Payload::Abort => {
                                if let Some(abort_sender) = aborters.remove(&id) {
                                    let _ = abort_sender.send(());
                                }
                            }
                        }
                    };

                    let mut return_value = ShutdownType::Closed;
                    let mut handle_shutdown = |shutdown_type: ShutdownType| {
                        return_value = shutdown_type;
                        let message = match shutdown_type {
                            ShutdownType::Shutdown => #rpc_path::producer::Message::Shutdown,
                            ShutdownType::Aborted => #rpc_path::producer::Message::Aborted,
                            _ => {
                                return;
                            }
                        };
                        let _ = reply_sender.unbounded_send(message);
                    };

                    let timeout_future = if let Some(duration) = timeout {
                        Timeout::new(duration)
                    } else {
                        Timeout::never()
                    };
                    let shutdown = #rpc_path::futures::FutureExt::fuse(shutdown);
                    pin_mut!(shutdown, timeout_future, stop_receiver);
                    loop {
                        select! {
                            receive_option = #rpc_path::futures::StreamExt::next(&mut receiver) => {
                                if timeout.is_some() {
                                    timeout_future.reset();
                                }

                                if let Some(receive_result) = receive_option {
                                    match receive_result {
                                        Ok(message) => handle_message(&mut aborters, message),
                                        Err(_) => { 
                                            handle_shutdown(ShutdownType::Closed);
                                            break; 
                                        },
                                    }
                                } else {
                                    handle_shutdown(ShutdownType::Closed);
                                    break;
                                }
                            },
                            id_option = #rpc_path::futures::StreamExt::next(&mut remove_aborter_receiver) => {
                                if let Some(id) = id_option {
                                    aborters.remove(&id);
                                }
                            },
                            _ = &mut timeout_future => {
                                if timeout.is_some() {
                                    if aborters.is_empty() {
                                        handle_shutdown(ShutdownType::Timeout);
                                        break;
                                    } else {
                                        timeout_future.reset();
                                    }
                                }
                            },
                            shutdown_type = &mut stop_receiver => {
                                if let Ok(shutdown_type) = shutdown_type {
                                    handle_shutdown(shutdown_type);
                                    break;
                                }
                            },
                            shutdown_type = &mut shutdown => {
                                handle_shutdown(shutdown_type);
                                break;
                            },
                        }
                    }

                    for (_, aborter) in aborters.drain() {
                        let _ = aborter.send(());
                    }

                    return_value
                })
            }
        }

        pub use #ident as impl_produce;
    };
    Ok(output)
}

#[cfg(test)]
mod tests {
    use quote::quote;
    use uuid::Uuid;

    use crate::{
        rpc::tests::{parsed_api, paths},
        tests::compare,
    };

    use super::impl_produce;

    #[test]
    fn test_impl_produce() {
        let expected = quote! {
            #[doc(hidden)]
            #[macro_export]
            macro_rules! __impl_produce_00000000000000000000000000000000  {
                ($self:ident, $transport:ident, $configuration:ident, $error_handler:ident) => {
                    ::dnet_rpc::spawn(async move {
                        use std::sync::Arc;
                        use std::collections::HashMap;
                        use futures::{channel::{mpsc::unbounded, oneshot,}, pin_mut, select, StreamExt,};
                        use ::dnet_rpc::{ShutdownType, Timeout};

                        let (to_other_side, mine) = ::dnet_rpc::parts::transports();
                        let _pipe = ::dnet_rpc::parts::Pipe::new(to_other_side, $transport, Default::default(), $error_handler);
                        let transport = mine;

                        let me = Arc::new($self);
                        let (mut sender, receiver) = transport.split();
                        let mut receiver = ::dnet_rpc::futures::StreamExt::fuse(receiver);
                        let (reply_sender, mut reply_receiver) = unbounded::<::dnet_rpc::producer::Message<Response>>();
                        let mut aborters: HashMap<u64, oneshot::Sender<()>> = HashMap::new();
                        let (remove_aborter_sender, mut remove_aborter_receiver) = unbounded::<u64>();
                        let (stop_sender, stop_receiver) = oneshot::channel::<ShutdownType>();
                        let mut stop_sender = Some(stop_sender);
                        let ::dnet_rpc::producer::Configuration { shutdown, timeout, .. } = $configuration;
                        ::dnet_rpc::spawn(async move {
                            while let Some(message) = ::dnet_rpc::futures::StreamExt::next(&mut reply_receiver).await {
                                ::dnet_rpc::parts::producer::handle_response(message, &mut sender, &mut stop_sender).await;
                            }
                        });
                        let handle_message = |aborters: &mut HashMap<u64, oneshot::Sender<()>>, message: ::dnet_rpc::consumer::Message<Request>| {
                            let ::dnet_rpc::consumer::Message { id, payload } = message;
                            match payload {
                                ::dnet_rpc::consumer::Payload::Request(request) => {
                                    let (abort_sender, abort_receiver) = oneshot::channel::<()>();
                                    aborters.insert(id, abort_sender);
                                    match request {
                                        Request::HelloWorld {} => {
                                            let me = me.clone();
                                            let task_aborter = None;
                                            let task = async move { Result::<_, std::convert::Infallible>::Ok(me.hello_world().await) };
                                            ::dnet_rpc::parts::producer::handle_no_ack_request(
                                                id,
                                                task,
                                                remove_aborter_sender.clone(),
                                                abort_receiver,
                                                task_aborter,
                                            );
                                        },
                                        Request::WaitForAck {} => {
                                            let me = me.clone();
                                            let task_aborter = None;
                                            let task = async move { Result::<_, std::convert::Infallible>::Ok(me.wait_for_ack().await) };
                                            ::dnet_rpc::parts::producer::handle_request(
                                                id,
                                                task,
                                                Response::WaitForAck,
                                                reply_sender.clone(),
                                                remove_aborter_sender.clone(),
                                                abort_receiver,
                                                task_aborter,
                                            );
                                        },
                                        Request::AddNumbers { a, b } => {
                                            let me = me.clone();
                                            let task_aborter = None;
                                            let task = async move { Result::<_, std::convert::Infallible>::Ok(me.add_numbers(a, b).await) };
                                            ::dnet_rpc::parts::producer::handle_request(
                                                id,
                                                task,
                                                Response::AddNumbers,
                                                reply_sender.clone(),
                                                remove_aborter_sender.clone(),
                                                abort_receiver,
                                                task_aborter,
                                            );
                                        },
                                        Request::ConcatenateStrings { a, b } => {
                                            let me = me.clone();
                                            let task_aborter = None;
                                            let task = async move { Result::<_, std::convert::Infallible>::Ok(me.concatenate_strings(a, b).await) };
                                            ::dnet_rpc::parts::producer::handle_request(
                                                id,
                                                task,
                                                Response::ConcatenateStrings,
                                                reply_sender.clone(),
                                                remove_aborter_sender.clone(),
                                                abort_receiver,
                                                task_aborter,
                                            );
                                        },
                                        Request::StreamNaturalNumbers {} => {
                                            let me = me.clone();
                                            let task_aborter = None;
                                            let task = async move{ Result::<_, std::convert::Infallible>::Ok(me.stream_natural_numbers().await) };
                                            ::dnet_rpc::parts::producer::handle_stream_request(
                                                id,
                                                task,
                                                Response::StreamNaturalNumbers,
                                                reply_sender.clone(),
                                                remove_aborter_sender.clone(),
                                                abort_receiver,
                                                task_aborter,
                                            );
                                        },
                                        Request::StreamTime { interval } => {
                                            let me = me.clone();
                                            let task_aborter = None;
                                            let task = async move{ Result::<_, std::convert::Infallible>::Ok(me.stream_time(interval).await) };
                                            ::dnet_rpc::parts::producer::handle_stream_request(
                                                id,
                                                task,
                                                Response::StreamTime,
                                                reply_sender.clone(),
                                                remove_aborter_sender.clone(),
                                                abort_receiver,
                                                task_aborter,
                                            );
                                        },
                                        Request::LongRunningTask { input } => {
                                            let me = me.clone();
                                            let aborter_token = ::dnet_rpc::producer::abortable::AborterToken::new();
                                            let abortion_token = ::dnet_rpc::producer::abortable::AbortionToken::new(aborter_token.clone());
                                            let task_aborter = Some(aborter_token);
                                            let task = async move { me.long_running_task(input, abortion_token).await };
                                            ::dnet_rpc::parts::producer::handle_request(
                                                id,
                                                task,
                                                Response::LongRunningTask,
                                                reply_sender.clone(),
                                                remove_aborter_sender.clone(),
                                                abort_receiver,
                                                task_aborter,
                                            );
                                        },
                                    }
                                }
                                ::dnet_rpc::consumer::Payload::Abort => {
                                    if let Some(abort_sender) = aborters.remove(&id) {
                                        let _ = abort_sender.send(());
                                    }
                                }
                            }
                        };
                        let mut return_value = ShutdownType::Closed;
                        let mut handle_shutdown = |shutdown_type: ShutdownType| {
                            return_value = shutdown_type;
                            let message = match shutdown_type {
                                ShutdownType::Shutdown => ::dnet_rpc::producer::Message::Shutdown,
                                ShutdownType::Aborted => ::dnet_rpc::producer::Message::Aborted,
                                _ => { return; }
                            };
                            let _ = reply_sender.unbounded_send(message);
                        };
                        let timeout_future = if let Some(duration) = timeout {
                            Timeout::new(duration)
                        } else {
                            Timeout::never()
                        };
                        let shutdown = ::dnet_rpc::futures::FutureExt::fuse(shutdown);
                        pin_mut!(shutdown, timeout_future, stop_receiver);
                        loop {
                            select! {
                                receive_option = ::dnet_rpc::futures::StreamExt::next(&mut receiver) => {
                                    if timeout.is_some() {
                                        timeout_future.reset();
                                    }
                                    if let Some(receive_result) = receive_option {
                                        match receive_result {
                                            Ok(message) => handle_message(&mut aborters, message),
                                            Err(_) => {
                                                handle_shutdown(ShutdownType::Closed);
                                                break;
                                            },
                                        }
                                    } else {
                                        handle_shutdown(ShutdownType::Closed);
                                        break;
                                    }
                                },
                                id_option = ::dnet_rpc::futures::StreamExt::next(&mut remove_aborter_receiver) => {
                                    if let Some(id) = id_option {
                                        aborters.remove(&id);
                                    }
                                },
                                _ = & mut timeout_future => {
                                    if timeout.is_some() {
                                        if aborters.is_empty() {
                                            handle_shutdown(ShutdownType::Timeout);
                                            break;
                                        } else {
                                            timeout_future.reset();
                                        }
                                    }
                                },
                                shutdown_type = & mut stop_receiver => {
                                    if let Ok(shutdown_type) = shutdown_type {
                                        handle_shutdown(shutdown_type);
                                        break;
                                    }
                                },
                                shutdown_type = & mut shutdown => {
                                    handle_shutdown(shutdown_type);
                                    break;
                                },
                            }
                        }
                        for (_, aborter) in aborters.drain() {
                            let _ = aborter.send(());
                        }
                        return_value
                    })
                };
            }
            pub use __impl_produce_00000000000000000000000000000000 as impl_produce;
        };

        let item = impl_produce(&paths(), &parsed_api(), Uuid::nil()).unwrap();
        let actual = quote! {
            #item
        };

        compare(expected, actual);
    }
}
