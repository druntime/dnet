//! Utilities used by transferable transport and transferable RPC features implementations.

use std::sync::LazyLock;

use js_sys::{Array, JsString, Object, Reflect, Uint8Array};
use wasm_bindgen::{JsCast, JsValue};

use crate::wrapper::Error;

static STRINGS: LazyLock<Strings> = LazyLock::new(Strings::new);

struct Strings {
    type_: JsString,
    name: JsString,
    transport_message: JsString,
    payload: JsString,
    open: JsString,
    close: JsString,
    stripped: JsString,
    transferables: JsString,
    into_transferables: JsString,
}

impl Strings {
    fn new() -> Self {
        Strings {
            type_: JsString::from("type"),
            name: JsString::from("name"),
            transport_message: JsString::from("transport-message"),
            payload: JsString::from("payload"),
            open: JsString::from("open"),
            close: JsString::from("close"),
            stripped: JsString::from("stripped"),
            transferables: JsString::from("transferables"),
            into_transferables: JsString::from("into-transferables"),
        }
    }
}

/// Represents the payload of a transferable transport message.
pub enum Payload {
    /// Indicates that the remote transport is open.
    Open,

    /// Indicates that the remote transport is closed.
    Close,

    /// Payload with message data.
    Object(JsValue),
}

/// Returns the message type of the given data, if it is a transferable transport message.
pub fn message_type(data: &JsValue) -> Option<JsValue> {
    Reflect::get(data, &STRINGS.type_).ok()
}

/// Returns `true` if the given data is a transferable transport message.
pub fn is_transport_message(data: &JsValue) -> bool {
    let Some(message_type) = message_type(data) else {
        return false;
    };

    Object::is(&message_type, &STRINGS.transport_message)
}

/// Returns the transport name of the given message.
pub fn message_transport_name(data: &JsValue) -> Option<JsValue> {
    if let Ok(name) = Reflect::get(data, &STRINGS.name) {
        if name.is_string() {
            Some(name)
        } else {
            None
        }
    } else {
        None
    }
}

/// Returns `true` if the transport name of the given message matches the given name.
pub fn transport_name_matches(data: &JsValue, name: Option<&JsString>) -> bool {
    match (message_transport_name(data), name) {
        (Some(left), Some(right)) => Object::is(&left, right),
        (None, None) => true,
        _ => false,
    }
}

/// Returns the payload of the given message.
pub fn message_payload(data: &JsValue) -> Option<Payload> {
    let payload = Reflect::get(data, &STRINGS.payload).ok()?;

    if let Some(payload) = payload.dyn_ref::<JsString>() {
        if Object::is(payload, &STRINGS.open) {
            Some(Payload::Open)
        } else if Object::is(payload, &STRINGS.close) {
            Some(Payload::Close)
        } else {
            None
        }
    } else {
        Some(Payload::Object(payload))
    }
}

/// Constructs a transferable transport message object with the given transport name and payload.
pub fn construct_message_object(transport_name: Option<&JsString>, payload: Payload) -> JsValue {
    let message = Object::new();

    Reflect::set(&message, &STRINGS.type_, &STRINGS.transport_message).unwrap();
    if let Some(name) = transport_name {
        Reflect::set(&message, &STRINGS.name, name).unwrap();
    }
    let value = match &payload {
        Payload::Open => &STRINGS.open,
        Payload::Close => &STRINGS.close,
        Payload::Object(payload) => payload,
    };
    Reflect::set(&message, &STRINGS.payload, value).unwrap();

    message.into()
}

/// Constructs data object used internally by the `IntoTransferable` derive macro implementation.
pub fn construct_into_transferable_data_object(
    stripped: &JsValue,
    transferables: &JsValue,
    into_transferables: Option<&JsValue>,
) -> JsValue {
    let data = Object::new();

    Reflect::set(&data, &STRINGS.stripped, stripped).unwrap();
    Reflect::set(&data, &STRINGS.transferables, transferables).unwrap();
    if let Some(into_transferables) = into_transferables {
        Reflect::set(&data, &STRINGS.into_transferables, into_transferables).unwrap();
    }

    data.into()
}

/// Destructs data object used internally by the `IntoTransferable` derive macro implementation.
pub fn destruct_into_transferable_data_object<S, D>(
    data: &JsValue,
) -> Result<(Uint8Array, Array, Option<JsValue>), Error<S, D>> {
    let stripped = Reflect::get(data, &STRINGS.stripped)
        .map_err(|_| Error::WrongType)?
        .dyn_into::<Uint8Array>()
        .map_err(|_| Error::WrongType)?;
    let transferables = Reflect::get(data, &STRINGS.transferables)
        .map_err(|_| Error::WrongType)?
        .dyn_into::<Array>()
        .map_err(|_| Error::WrongType)?;
    let into_transferables = Reflect::get(data, &STRINGS.into_transferables).ok();

    Ok((stripped, transferables, into_transferables))
}
