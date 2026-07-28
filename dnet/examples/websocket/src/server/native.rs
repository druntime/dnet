use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use anyhow::{bail, Result};
use axum::{
    extract::{ws::WebSocket, ConnectInfo, State, WebSocketUpgrade},
    response::IntoResponse,
    routing::any,
    Router,
};
use axum_extra::{headers, TypedHeader};
use dnet::{codecs::BincodeCodec, websocket::axum::WebSocketTransport, Receive};
use futures::{SinkExt, StreamExt};
use tokio::{
    signal, spawn,
    sync::{
        mpsc::{unbounded_channel, UnboundedSender},
        RwLock,
    },
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tower_http::{
    services::ServeDir,
    trace::{DefaultMakeSpan, TraceLayer},
};
use tracing::{error, info};

use crate::{client, server::Message};

#[derive(Clone)]
struct AppState {
    state: Arc<RwLock<MyState>>,
}

#[derive(Debug)]
struct User {
    sender: UnboundedSender<Message>,
}

impl User {
    fn new(sender: UnboundedSender<Message>) -> Self {
        User { sender }
    }
}

#[derive(Debug, Default)]
struct MyState {
    users: HashMap<String, User>,
}

pub async fn run(address: &str) -> Result<()> {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!("{}=debug,tower_http=debug", env!("CARGO_CRATE_NAME")).into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let state = AppState {
        state: Arc::new(RwLock::new(MyState {
            users: HashMap::new(),
        })),
    };

    let static_files = ServeDir::new("./www").append_index_html_on_directories(true);

    let routes = Router::new()
        .fallback_service(static_files)
        .route("/ws", any(ws_handler))
        .with_state(state)
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::default().include_headers(true)),
        );

    let listener = tokio::net::TcpListener::bind(&address).await.unwrap();
    let address = listener.local_addr().unwrap();
    println!("Server listening on {address}...",);
    println!("Web client available at http://{address}/");
    axum::serve(
        listener,
        routes.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await
    .unwrap();

    Ok(())
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    user_agent: Option<TypedHeader<headers::UserAgent>>,
    ConnectInfo(address): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let user_agent = if let Some(TypedHeader(user_agent)) = user_agent {
        user_agent.to_string()
    } else {
        String::from("Unknown browser")
    };
    info!("`{user_agent}` at {address} connected.");
    ws.on_upgrade(move |socket| handle_socket(state, socket, address))
}

async fn handle_socket(state: AppState, socket: WebSocket, _address: SocketAddr) {
    if let Err(error) = user_connected(socket, state.state).await {
        error!("Error occurred: {error}");
    }
}

async fn user_connected(socket: WebSocket, state: Arc<RwLock<MyState>>) -> Result<()> {
    let (mut sender, mut receiver) =
        WebSocketTransport::<_, _, client::Message, Message>::new(socket, BincodeCodec::default())
            .split();

    let init_message = receiver.receive().await?;
    match init_message {
        client::Message::Init { user_name } => {
            let (user_sender, user_receiver) = unbounded_channel();
            let mut user_receiver = UnboundedReceiverStream::new(user_receiver);

            let user = User::new(user_sender);
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
                    let _ = user.sender.send(message.clone());
                }
            }

            let user_name_clone = user_name.clone();
            spawn(async move {
                while let Some(message) = user_receiver.next().await {
                    let result = sender.send(message).await;
                    if let Err(error) = result {
                        error!("Failed to send message to user: name: {user_name_clone}, error: {error}.");
                    }
                }
            });

            while let Some(result) = receiver.next().await {
                let msg = match result {
                    Ok(msg) => msg,
                    Err(error) => {
                        error!("Failed to receive message from user: {user_name}, error: {error}.");
                        break;
                    }
                };
                user_message(user_name.clone(), msg, &state).await?;
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
    state: &Arc<RwLock<MyState>>,
) -> Result<()> {
    match message {
        client::Message::Message { content } => {
            let message = Message::Message {
                user_name: name,
                content,
            };
            for user in state.read().await.users.values() {
                let _ = user.sender.send(message.clone());
            }
        }
        _ => bail!("unexpected message received"),
    }

    Ok(())
}

async fn user_disconnected(name: String, state: &Arc<RwLock<MyState>>) {
    info!("User <{name}> disconnected.");

    let message = Message::UserDisconnected {
        user_name: name.clone(),
    };
    for (user_name, user) in state.read().await.users.iter() {
        if &name != user_name {
            let _ = user.sender.send(message.clone());
        }
    }

    state.write().await.users.remove(&name);
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
