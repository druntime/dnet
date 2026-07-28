#![cfg(not(target_arch = "wasm32"))]

use anyhow::Result;
use dnet::{
    codecs::BincodeCodec,
    quic::{QuicTransport, QuicUnreliableTransport},
    Messages, Receive,
};
use dnet_utils::{
    latest::OnlyLatest,
    number::{NumberMessagesU128, Wrapper},
    unwrap::Unwrapping,
};
use futures::{FutureExt, SinkExt, StreamExt};
use quinn::{crypto::rustls::QuicClientConfig, ClientConfig, Endpoint, TransportConfig};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustyline_async::{Readline, ReadlineEvent};
use serde::{Deserialize, Serialize};
use std::{io::Write, sync::Arc, time::Duration};
use tokio::select;

use crate::server::{self, MousePosition};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    Init { user_name: String },
    Message { content: String },
}

pub async fn run(address: &str) -> Result<()> {
    println!("Connecting to server...");

    // Note we are skipping server certificate verification here.
    // You should NOT use setup like this in your application
    // unless you understand the consequences of doing so.
    let mut client_config = ClientConfig::new(Arc::new(QuicClientConfig::try_from(
        rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(SkipServerVerification::new())
            .with_no_client_auth(),
    )?));
    let mut transport_config = TransportConfig::default();
    transport_config.max_idle_timeout(Some(Duration::from_secs(3).try_into()?));
    client_config.transport_config(Arc::new(transport_config));

    let mut endpoint = Endpoint::client("0.0.0.0:0".parse()?)?;
    endpoint.set_default_client_config(client_config);

    let connecting = endpoint.connect(address.parse()?, "localhost")?;

    let connection = connecting.await?;

    let (send_stream, recv_stream) = connection.open_bi().await?;

    let mut unreliable = QuicUnreliableTransport::<
        _,
        Wrapper<u128, MousePosition>,
        Wrapper<u128, ()>,
    >::new(&connection, BincodeCodec::default(), Default::default())
    .number_messages_u128()
    .only_latest()
    .unwrapping();

    let (mut sender, mut receiver) = QuicTransport::<_, _, server::Message, Message>::new(
        send_stream,
        recv_stream,
        BincodeCodec::default(),
    )
    .await?
    .split();
    println!("Connected.");

    let (mut readline, mut stdout) = Readline::new("> ".to_string())?;
    readline.should_print_line_on(false, true);
    writeln!(stdout, "Type your name:")?;

    let name = if let ReadlineEvent::Line(name) = readline.readline().await? {
        name
    } else {
        println!("Exiting...");
        return Ok(());
    };
    sender
        .send(Message::Init {
            user_name: name.clone(),
        })
        .await?;

    let init_message = receiver.receive().await?;
    match init_message {
        server::Message::Init { name_already_taken } => {
            if name_already_taken {
                panic!("name already taken");
            }
        }
        _ => panic!("unexpected message received"),
    }
    writeln!(stdout, "Hello {name}.")?;
    writeln!(stdout, "Server mouse position is displayed in prompt.")?;

    readline.flush()?;

    let (mut readline, mut stdout) = Readline::new("> ".to_string())?;
    let mut stdout_clone = stdout.clone();
    let mut message_stream = receiver.messages_with_error_callback(move |error| {
        let _ = writeln!(
            stdout_clone,
            "Error occurred while receiving message: {error}."
        );
    });

    loop {
        readline.should_print_line_on(false, true);
        select! {
            message = message_stream.next() => {
                if let Some(message) = message {
                    match message {
                        server::Message::UserConnected { user_name } => {
                            writeln!(stdout, "New user connected: <{user_name}>.")?;
                        },
                        server::Message::UserDisconnected { user_name } => {
                            writeln!(stdout, "User <{user_name}> left.")?;
                        },
                        server::Message::Message { user_name, content } => {
                            writeln!(stdout, "<{user_name}> {content}")?;
                        },
                        _ => panic!("unexpected message received"),
                    }
                } else {
                    writeln!(stdout, "Server disconnected.")?;
                    writeln!(stdout, "Exiting...")?;
                    break;
                }
            },
            mouse_position = unreliable.next() => {
                if let Some(Ok(MousePosition { x, y })) = mouse_position {
                    readline.update_prompt(&format!("[ x: {x}, y: {y} ] > "))?;
                }
            },
            command = readline.readline().fuse() => match command {
                Ok(event) => {
                    match event {
                        ReadlineEvent::Line(line) => {
                            let message = Message::Message { content: line.to_string() };
                            sender.send(message).await?;
                        },
                        ReadlineEvent::Eof | ReadlineEvent::Interrupted => {
                            writeln!(stdout, "Exiting...")?;
                            break;
                        }
                    }
                },
                Err(error) => {
                    writeln!(stdout, "Error occurred while handling command: {error}")?;
                    writeln!(stdout, "Exiting...")?;
                    break;
                },
            },
        }
    }
    readline.flush()?;

    Ok(())
}

// Code below is copied from https://github.com/quinn-rs/quinn/blob/main/quinn/examples/insecure_connection.rs
// Again: Note we are skipping server certificate verification in this example.
// You should NOT use setup like this in your application
// unless you understand the consequences of doing so.

/// Dummy certificate verifier that treats any certificate as valid.
/// NOTE, such verification is vulnerable to MITM attacks, but convenient for testing.
#[derive(Debug)]
struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self(Arc::new(rustls::crypto::ring::default_provider())))
    }
}

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
