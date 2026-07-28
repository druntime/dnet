use std::{cell::RefCell, rc::Rc};

use dnet::{codecs::BincodeCodec, websocket::WebSocketTransport, Messages, Receive};
use futures::{SinkExt, StreamExt};
use js_utils::{
    console_log, document,
    event::{EventListener, When},
    spawn, window,
};
use wasm_bindgen::JsCast;
use web_sys::{
    Element, HtmlInputElement, HtmlTextAreaElement, KeyboardEvent, MouseEvent, WebSocket,
};

use crate::{client::Message, server};

#[allow(dead_code)]
pub async fn run() {
    js_utils::set_panic_hook();

    console_log!("WASM initialized.");

    let document = document();

    let text_area = Rc::new(
        document
            .get_element_by_id("text")
            .unwrap()
            .dyn_into::<HtmlTextAreaElement>()
            .unwrap(),
    );

    let input = Rc::new(
        document
            .get_element_by_id("input")
            .unwrap()
            .dyn_into::<HtmlInputElement>()
            .unwrap(),
    );

    let send_button = Rc::new(document.get_element_by_id("send").unwrap());

    let write_line = move |line: &str| {
        let mut value = text_area.value();
        value += line;
        value += "\n";
        text_area.set_value(&value);
        text_area.set_scroll_top(text_area.scroll_height());
    };

    let host = window()
        .location()
        .host()
        .expect("couldn't extract host from location");

    write_line(&format!("Connecting to server at {host}..."));
    let address = format!("ws://{host}/ws");
    let web_socket = WebSocket::new(&address).unwrap();
    let (sender, mut receiver) =
        WebSocketTransport::<_, server::Message, Message>::new(web_socket, BincodeCodec::default())
            .await
            .unwrap()
            .split();
    write_line("Connected.");

    let sender = Rc::new(RefCell::new(sender));

    {
        write_line("Type your name.");
        let name = Rc::new(RefCell::new(String::default()));
        let sender = sender.clone();
        let write = write_line.clone();
        let name_clone = name.clone();
        let _handlers = set_input_handlers(&input, &send_button, move |input| {
            let sender = sender.clone();
            let write = write.clone();
            let input = input.to_string();
            *name_clone.borrow_mut() = input.clone();
            spawn(async move {
                let mut sender = sender.borrow_mut();
                match sender.send(Message::Init { user_name: input }).await {
                    Ok(_) => (),
                    Err(error) => {
                        write(&format!("Error occurred while sending message: {error}."));
                    }
                };
            });
        });
        let init_message = receiver.receive().await.unwrap();
        match init_message {
            server::Message::Init { name_already_taken } => {
                if name_already_taken {
                    write_line("Name already taken.");
                    write_line("Reload the page to try again.");
                    return;
                }
            }
            _ => {
                write_line("Unexpected message received.");
                write_line("Reload the page to try again.");
                return;
            }
        }
        write_line(&format!("Hello {}.", name.borrow()));
    }

    let write = write_line.clone();
    let mut message_stream = receiver.messages_with_error_callback(move |error| {
        write(&format!("Error occurred while receiving message: {error}."));
    });

    let write = write_line.clone();
    let _handlers = set_input_handlers(&input, &send_button, move |input| {
        let input = input.to_string();
        let sender = sender.clone();
        let write = write.clone();
        spawn(async move {
            let message = Message::Message { content: input };
            if let Err(error) = sender.borrow_mut().send(message).await {
                write(&format!("Error occurred while sending message: {error}."));
            }
        });
    });

    while let Some(message) = message_stream.next().await {
        match message {
            server::Message::UserConnected { user_name } => {
                write_line(&format!("New user connected: <{user_name}>."));
            }
            server::Message::UserDisconnected { user_name } => {
                write_line(&format!("User <{user_name}> left."));
            }
            server::Message::Message { user_name, content } => {
                write_line(&format!("<{user_name}> {content}"));
            }
            _ => write_line("Unexpected message received."),
        }
    }
    write_line("Server disconnected.");
}

fn set_input_handlers<F>(
    input: &Rc<HtmlInputElement>,
    button: &Rc<Element>,
    mut callback: F,
) -> (
    EventListener<HtmlInputElement, KeyboardEvent>,
    EventListener<Element, MouseEvent>,
)
where
    F: FnMut(&str) + Clone + 'static,
{
    let input_clone = input.clone();
    let mut callback_clone = callback.clone();
    let enter_handler = input
        .when("keyup", move |event: KeyboardEvent| {
            let value = input_clone.value();
            if event.key() == "Enter" {
                callback_clone(&value);
            }
        })
        .unwrap();

    let input_clone = input.clone();
    let click_handler = button
        .when("click", move |_event: MouseEvent| {
            let value = input_clone.value();
            callback(&value);
        })
        .unwrap();

    (enter_handler, click_handler)
}
