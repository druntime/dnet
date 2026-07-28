use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    response::IntoResponse,
    routing::any,
    Router,
};
use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tower_http::trace::{DefaultMakeSpan, TraceLayer};

type SenderAndReceiver = (
    async_channel::Sender<UnboundedReceiver<Message>>,
    async_channel::Receiver<UnboundedReceiver<Message>>,
);

#[derive(Clone)]
struct AppState {
    left: SenderAndReceiver,
    right: SenderAndReceiver,
}

impl AppState {
    fn new() -> Self {
        AppState {
            left: async_channel::unbounded(),
            right: async_channel::unbounded(),
        }
    }

    async fn left_sender(&self) -> UnboundedSender<Message> {
        let (sender, receiver) = unbounded_channel();
        self.right.0.send(receiver).await.unwrap();
        sender
    }

    async fn left_receiver(&self) -> UnboundedReceiver<Message> {
        self.left.1.recv().await.unwrap()
    }

    async fn right_sender(&self) -> UnboundedSender<Message> {
        let (sender, receiver) = unbounded_channel();
        self.left.0.send(receiver).await.unwrap();
        sender
    }

    async fn right_receiver(&self) -> UnboundedReceiver<Message> {
        self.right.1.recv().await.unwrap()
    }
}

#[tokio::main]
async fn main() {
    /*
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!("{}=debug,tower_http=debug", env!("CARGO_CRATE_NAME")).into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
    */

    let state = AppState::new();

    let routes = Router::new()
        .route("/left", any(left_handler))
        .route("/right", any(right_handler))
        .with_state(state)
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::default().include_headers(true)),
        );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    println!(
        "Tests server listening on {}",
        listener.local_addr().unwrap()
    );
    axum::serve(listener, routes).await.unwrap();
}

async fn left_handler(State(state): State<AppState>, ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket_left(state, socket))
}

async fn right_handler(State(state): State<AppState>, ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket_right(state, socket))
}

async fn handle_socket_left(state: AppState, socket: WebSocket) {
    let to_other = state.left_sender().await;
    let from_other = state.left_receiver().await;
    handle_socket(socket, to_other, from_other).await;
}

async fn handle_socket_right(state: AppState, socket: WebSocket) {
    let to_other = state.right_sender().await;
    let from_other = state.right_receiver().await;
    handle_socket(socket, to_other, from_other).await;
}

async fn handle_socket(
    socket: WebSocket,
    to_other: UnboundedSender<Message>,
    mut from_other: UnboundedReceiver<Message>,
) {
    let (mut to_socket, mut from_socket) = socket.split();
    loop {
        tokio::select! {
            message = from_other.recv() => {
                if let Some(message) = message {
                    let _ = to_socket.send(message).await;
                } else {
                    break
                }
            },
            message = from_socket.next() => {
                if let Some(Ok(message)) = message {
                    let _ = to_other.send(message);
                } else {
                    break
                }
            },
        }
    }
}
