#![cfg(not(target_arch = "wasm32"))]

use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{bail, Result};
use device_query::{DeviceQuery, DeviceState};
use dnet::{
    codecs::BincodeCodec,
    quic::{QuicTransport, QuicUnreliableTransport},
    utils::{
        latest::OnlyLatest,
        number::{NumberMessagesU128, Wrapper},
        unwrap::Unwrapping,
    },
    Receive,
};
use futures::{pin_mut, FutureExt, SinkExt, StreamExt};
use quinn::{Endpoint, Incoming, ServerConfig};
use rcgen::generate_simple_self_signed;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use serde::{Deserialize, Serialize};
use tokio::{
    select, spawn,
    sync::{
        mpsc::{unbounded_channel, UnboundedSender},
        RwLock,
    },
    time::interval,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{error, info, Level};

use crate::client;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    Init { name_already_taken: bool },
    UserConnected { user_name: String },
    UserDisconnected { user_name: String },
    Message { user_name: String, content: String },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MousePosition {
    pub x: i32,
    pub y: i32,
}

#[derive(Debug)]
struct User {
    reliable_sender: UnboundedSender<Message>,
    unreliable_sender: UnboundedSender<MousePosition>,
}

impl User {
    fn new(
        reliable_sender: UnboundedSender<Message>,
        unreliable_sender: UnboundedSender<MousePosition>,
    ) -> Self {
        User {
            reliable_sender,
            unreliable_sender,
        }
    }
}

#[derive(Debug, Default)]
struct State {
    users: HashMap<String, User>,
}

pub async fn run(address: &str) -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("Server running!");
    let state = Arc::new(RwLock::new(State::default()));

    let break_signal = tokio::signal::ctrl_c().fuse();
    pin_mut!(break_signal);

    let server_address = address.parse()?;

    // You'd likely need to tweak this certificate-related code in your application.
    // Here we use a simple setup to just make example work.
    //
    // See rustls (https://github.com/rustls/rustls) and quinn (https://github.com/quinn-rs/quinn)
    // documentation for more info.
    let certificate = generate_simple_self_signed(vec!["localhost".into()])?;
    let certificate_der = CertificateDer::from(certificate.cert.clone());

    let private_key = PrivatePkcs8KeyDer::from(certificate.signing_key.serialize_der());

    let mut server_config =
        ServerConfig::with_single_cert(vec![certificate_der], private_key.into())?;
    let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();
    transport_config.max_concurrent_uni_streams(0_u8.into());
    transport_config.keep_alive_interval(Some(Duration::from_secs(1)));

    let endpoint = Endpoint::server(server_config, server_address)?;
    info!("Listening at {}...", address);

    let device_state = DeviceState::new();
    let mut interval = interval(Duration::from_millis(50));

    loop {
        select! {
            endpoint_option = endpoint.accept() => {
                if let Some(incoming) = endpoint_option {
                    let state = state.clone();
                    spawn(async move {
                        if let Err(error) = user_connected(incoming, state).await {
                            error!("Error occurred: {error}");
                        }
                    });
                } else {
                    break;
                }
            },
            _tick = interval.tick() => {
                let mouse_state = device_state.get_mouse();
                let (x, y) = mouse_state.coords;
                let message = MousePosition { x, y };
                for user in state.write().await.users.values() {
                    let _ = user.unreliable_sender.send(message);
                }
            },
            break_result = &mut break_signal => {
                break_result.expect("failed to listen for event");
                break;
            }
        }
    }
    info!("Shutting down...");

    Ok(())
}

async fn user_connected(incoming: Incoming, state: Arc<RwLock<State>>) -> Result<()> {
    let connection = incoming.await?;

    let (send_stream, recv_stream) = connection.accept_bi().await?;

    let mut unreliable = QuicUnreliableTransport::<
        _,
        Wrapper<u128, ()>,
        Wrapper<u128, MousePosition>,
    >::new(&connection, BincodeCodec::default(), Default::default())
    .number_messages_u128()
    .only_latest()
    .unwrapping();

    let (mut sender, mut receiver) = QuicTransport::<_, _, client::Message, Message>::new(
        send_stream,
        recv_stream,
        BincodeCodec::default(),
    )
    .await?
    .split();

    let init_message = receiver.receive().await?;
    match init_message {
        client::Message::Init { user_name } => {
            let (unreliable_sender, unreliable_receiver) = unbounded_channel();
            let mut unreliable_receiver = UnboundedReceiverStream::new(unreliable_receiver);

            let (user_sender, user_receiver) = unbounded_channel();
            let mut user_receiver = UnboundedReceiverStream::new(user_receiver);

            let user = User::new(user_sender, unreliable_sender);
            {
                let mut state = state.write().await;
                if state.users.contains_key(&user_name) {
                    sender
                        .send(Message::Init {
                            name_already_taken: true,
                        })
                        .await?;
                    bail!("user with that name already exists");
                } else {
                    state.users.insert(user_name.clone(), user);
                    sender
                        .send(Message::Init {
                            name_already_taken: false,
                        })
                        .await?;
                }
            }

            info!("User <{user_name}> connected.");

            let message = Message::UserConnected {
                user_name: user_name.clone(),
            };
            for (name, user) in state.read().await.users.iter() {
                if &user_name != name {
                    let _ = user.reliable_sender.send(message.clone());
                }
            }

            /*let user_name_clone = user_name.clone();
            spawn(async move {
                while let Some(message) = unreliable_user_receiver.next().await {
                    let result = unreliable_sender.send(message).await;
                    if let Err(error) = result {
                        error!("Failed to send mouse position to user: name: {user_name_clone}, error: {error}.");
                    }
                }
            });*/

            let user_name_clone = user_name.clone();
            spawn(async move {
                while let Some(message) = user_receiver.next().await {
                    let result = sender.send(message).await;
                    if let Err(error) = result {
                        error!("Failed to send message to user: name: {user_name_clone}, error: {error}.");
                    }
                }
            });

            loop {
                select! {
                    receiver_result = receiver.next() => {
                        let Some(result) = receiver_result else {
                            break
                        };
                        let msg = match result {
                            Ok(msg) => msg,
                            Err(error) => {
                                error!("Failed to receive message from user: {user_name}, error: {error}.");
                                break;
                            }
                        };
                        user_message(user_name.clone(), msg, &state).await?;
                    }
                    unreliable_result = unreliable_receiver.next() => {
                        let Some(mouse_position) = unreliable_result else {
                            break;
                        };
                        let _ = unreliable.send(mouse_position).await?;
                    }
                }
            }

            user_disconnected(user_name, &state).await;
        }
        _ => bail!("unexpected message received"),
    }

    Ok(())
}

async fn user_message(
    name: String,
    message: client::Message,
    state: &Arc<RwLock<State>>,
) -> Result<()> {
    match message {
        client::Message::Message { content } => {
            let message = Message::Message {
                user_name: name,
                content,
            };
            for user in state.read().await.users.values() {
                let _ = user.reliable_sender.send(message.clone());
            }
        }
        _ => bail!("unexpected message received"),
    }

    Ok(())
}

async fn user_disconnected(name: String, state: &Arc<RwLock<State>>) {
    info!("User <{name}> disconnected.");

    let message = Message::UserDisconnected {
        user_name: name.clone(),
    };
    for (user_name, user) in state.read().await.users.iter() {
        if &name != user_name {
            let _ = user.reliable_sender.send(message.clone());
        }
    }

    state.write().await.users.remove(&name);
}
