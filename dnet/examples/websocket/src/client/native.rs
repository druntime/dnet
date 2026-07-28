use anyhow::Result;
use dnet::{codecs::BincodeCodec, websocket::WebSocketTransport, Messages, Receive};
use futures::{FutureExt, SinkExt, StreamExt};
use rustyline_async::{Readline, ReadlineEvent};
use std::io::Write;
use tokio::select;

use crate::{client::Message, server};

pub async fn run(address: &str) -> Result<()> {
    println!("Connecting to server at {address}...");
    let (stream, _) = tokio_tungstenite::connect_async(format!("ws://{address}/ws")).await?;
    let (mut sender, mut receiver) =
        WebSocketTransport::<_, _, server::Message, Message>::new(stream, BincodeCodec::default())
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
