//! Transport of data with [Transferable] data over underlying transports
//! implementing [PostMessage].

pub mod typed_array;

use std::{
    cell::RefCell,
    fmt::{Debug, Display},
    marker::PhantomData,
    pin::Pin,
    rc::Rc,
    task::{self, Poll},
};

use futures::{channel::oneshot, stream::FusedStream, Sink, Stream};
use js_sys::{Array, Object, Reflect};
use js_utils::{
    event::{EventListener, When},
    JsError,
};
use wasm_bindgen::{JsCast, JsValue};
use web_sys::{Event, EventTarget, MessageEvent};

use super::{state::State, PostMessage};

/// Data with [transferable](https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API/Transferable_objects).
pub struct WithTransferable {
    /// Data to transfer.
    pub data: JsValue,

    /// Array of [transferable](https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API/Transferable_objects) objects.
    pub transfer: Array,
}

/// Trait that needs to be implemented for data to be able to be
/// transferred over [TransferableTransport].
pub trait Transferable {
    /// Additional "context" needed to perform conversion.
    type Context;

    /// Conversion error.
    type Error;

    /// Convert into [WithTransferable].
    fn prepare_for_transfer(
        self,
        context: &mut Self::Context,
    ) -> Result<WithTransferable, Self::Error>;

    /// Reconstruct from [JsValue].
    fn reconstruct(object: JsValue, context: &mut Self::Context) -> Result<Self, Self::Error>
    where
        Self: Sized;
}

/// Trait for types that can be converted into [Transferable] data.
pub trait IntoTransferable<Context, Error> {
    /// Type of output ([Transferable]) data.
    type Output: Transferable<Context = Context, Error = Error>;

    /// Convert into [Transferable] data.
    fn into_transferable(self) -> Self::Output;
}

/// Trait for types that can be reconstructed from [Transferable] data.
pub trait FromTransferable<Context, Error> {
    /// Type of input ([Transferable]) data.
    type Input: Transferable<Context = Context, Error = Error>;

    /// Reconstruct from [Transferable] data.
    fn from_transferable(input: Self::Input) -> Self;
}

/// [TransferableTransport] error.
#[derive(Debug)]
pub enum Error<TransferableError> {
    /// Ran not inside a web worker.
    NotInWorker,

    /// Error occurred during sending a message.
    SendingError(JsError),

    /// Malformed message received.
    MalformedMessage,

    /// Failed to prepare data for transfer/reconstruct the data.
    TransferableError(TransferableError),

    /// Error occurred in worker.
    WorkerError(Event),

    /// Message error occurred.
    MessageError(MessageEvent),

    /// Other error occurred.
    Other(JsError),
}

impl<TransferableError> Display for Error<TransferableError>
where
    TransferableError: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotInWorker => write!(f, "not inside a (right type of) worker"),
            Error::SendingError(error) => write!(f, "failed to send message: {error}"),
            Error::MalformedMessage => write!(f, "malformed message received"),
            Error::TransferableError(error) => {
                write!(f, "error occurred  while converting transferable: {error}")
            }
            Error::WorkerError(error) => write!(f, "error occurred in worker: {error:?}"),
            Error::MessageError(error) => write!(f, "message error occurred: {error:?}"),
            Error::Other(error) => write!(f, "other error occurred: {error}"),
        }
    }
}

impl<TransferableError> std::error::Error for Error<TransferableError> where
    TransferableError: Debug + Display
{
}

/// Transport of data with [Transferable] data over underlying transports
/// implementing [PostMessage].
pub struct TransferableTransport<T, Context, Incoming, Outgoing, Error>
where
    T: JsCast + AsRef<EventTarget> + PostMessage,
    Incoming: FromTransferable<Context, Error>,
    Outgoing: IntoTransferable<Context, Error>,
    Error: std::error::Error + 'static,
{
    target: Rc<T>,
    name: Option<String>,
    state: Rc<RefCell<State<JsValue, self::Error<Error>>>>,
    context: RefCell<Context>,
    open_receiver: Option<oneshot::Receiver<()>>,

    #[cfg(feature = "logging")]
    logger: Rc<RefCell<dnet_base::Logger>>,

    _message_listener: EventListener<T, MessageEvent>,
    _error_listener: EventListener<T, Event>,
    _message_error_listener: EventListener<T, MessageEvent>,
    _incoming: PhantomData<Incoming>,
    _outgoing: PhantomData<Outgoing>,
}

impl<T, Context, Incoming, Outgoing, Error>
    TransferableTransport<T, Context, Incoming, Outgoing, Error>
where
    T: JsCast + AsRef<EventTarget> + PostMessage,
    Incoming: FromTransferable<Context, Error>,
    Outgoing: IntoTransferable<Context, Error>,
    Error: std::error::Error + 'static,
{
    /// Create new [TransferableTransport] wrapping provided target implementing [PostMessage].
    pub async fn new(
        target: &Rc<T>,
        name: Option<String>,
        context: Context,
        wait_for_open: bool,
    ) -> Result<Self, self::Error<Error>> {
        let (open_sender, open_receiver) = oneshot::channel();
        let open_sender = Rc::new(RefCell::new(Some(open_sender)));

        let state = Rc::new(RefCell::new(State::new()));

        #[cfg(feature = "logging")]
        let logger = Rc::new(RefCell::new(if let Some(name) = name.as_ref() {
            dnet_base::Logger::new_for_already_named::<Self>(&name)
        } else {
            dnet_base::Logger::new::<Self>()
        }));

        let target = target.clone();

        #[cfg(feature = "logging")]
        let logger_clone = logger.clone();

        let state_clone = state.clone();
        let name_clone = name.clone();
        let open_sender_clone = open_sender.clone();
        let message_listener = target
            .when("message", move |event: MessageEvent| {
                let data = event.data();
                if !(Reflect::get(&data, &"type".into()).ok() == Some("transport-message".into())
                    && Reflect::get(&data, &"name".into())
                        .ok()
                        .and_then(|value| value.as_string())
                        == name_clone)
                {
                    return;
                }

                if let Ok(payload) = Reflect::get(&data, &"payload".into()) {
                    if let Some(payload) = payload.as_string() {
                        match payload.as_str() {
                            "open" => {
                                if let Some(notifier) = open_sender_clone.take() {
                                    let _ = notifier.send(());
                                } else {
                                    unreachable!("open message received twice!")
                                }
                            }
                            "close" => {
                                state_clone.borrow_mut().close();
                            }
                            _ => {
                                let error = self::Error::MalformedMessage;

                                #[cfg(feature = "logging")]
                                logger_clone.borrow().log_message_arrived_failure(&error);

                                state_clone.borrow_mut().error(error);
                            }
                        }
                    } else if payload.is_object() {
                        #[cfg(feature = "logging")]
                        logger_clone.borrow().log_message_arrived_unknown(None);

                        state_clone.borrow_mut().message(payload);
                    } else {
                        let error = self::Error::MalformedMessage;

                        #[cfg(feature = "logging")]
                        logger_clone.borrow().log_message_arrived_failure(&error);

                        state_clone.borrow_mut().error(error);
                    }
                } else {
                    let error = self::Error::MalformedMessage;

                    #[cfg(feature = "logging")]
                    logger_clone.borrow().log_message_arrived_failure(&error);

                    state_clone.borrow_mut().error(error);
                }
            })
            .map_err(self::Error::Other)?;

        let state_clone = state.clone();
        let error_listener = target
            .when("error", move |event: Event| {
                state_clone
                    .borrow_mut()
                    .error(self::Error::WorkerError(event));
            })
            .map_err(self::Error::Other)?;

        let state_clone = state.clone();
        let message_error_listener = target
            .when("messageerror", move |event: MessageEvent| {
                state_clone
                    .borrow_mut()
                    .error(self::Error::MessageError(event));
            })
            .map_err(self::Error::Other)?;

        let context = RefCell::new(context);

        let mut transport = TransferableTransport {
            target,
            name: name.clone(),
            state,
            context,
            open_receiver: Some(open_receiver),

            #[cfg(feature = "logging")]
            logger,

            _message_listener: message_listener,
            _error_listener: error_listener,
            _message_error_listener: message_error_listener,
            _incoming: PhantomData,
            _outgoing: PhantomData,
        };
        transport.send_open();
        if wait_for_open {
            transport.wait_for_open().await;
        }

        Ok(transport)
    }

    /// Name of the transport.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Wait for transport open.
    pub async fn wait_for_open(&mut self) {
        if let Some(open_receiver) = self.open_receiver.take() {
            let _ = open_receiver.await;

            #[cfg(feature = "logging")]
            self.logger.borrow().log_open_success();
        }
    }

    fn send_message(&self, message: Outgoing) -> Result<(), self::Error<Error>> {
        let WithTransferable { data, transfer } = message
            .into_transferable()
            .prepare_for_transfer(&mut self.context.borrow_mut())
            .map_err(self::Error::TransferableError)?;

        let message = Object::new();
        Reflect::set(&message, &"type".into(), &"transport-message".into()).unwrap();
        if let Some(name) = &self.name {
            Reflect::set(&message, &"name".into(), &name.into()).unwrap();
        }
        Reflect::set(&message, &"payload".into(), &data).unwrap();
        self.target
            .post_message_with_transfer(&message, &transfer)
            .map_err(|error| self::Error::SendingError(error.into()))?;
        Ok(())
    }

    fn send_open(&self) {
        let message = Object::new();
        Reflect::set(&message, &"type".into(), &"transport-message".into()).unwrap();
        if let Some(name) = &self.name {
            Reflect::set(&message, &"name".into(), &name.into()).unwrap();
        }
        Reflect::set(&message, &"payload".into(), &"open".into()).unwrap();
        let _ = self.target.post_message(&message);
    }

    fn send_close(&self) {
        let message = Object::new();
        Reflect::set(&message, &"type".into(), &"transport-message".into()).unwrap();
        if let Some(name) = &self.name {
            Reflect::set(&message, &"name".into(), &name.into()).unwrap();
        }
        Reflect::set(&message, &"payload".into(), &"close".into()).unwrap();
        let _ = self.target.post_message(&message);
    }
}

impl<T, Context, Incoming, Outgoing, Error> Drop
    for TransferableTransport<T, Context, Incoming, Outgoing, Error>
where
    T: JsCast + AsRef<EventTarget> + PostMessage,
    Incoming: FromTransferable<Context, Error>,
    Outgoing: IntoTransferable<Context, Error>,
    Error: std::error::Error + 'static,
{
    fn drop(&mut self) {
        self.send_close();
    }
}

impl<T, Context, Incoming, Outgoing, Error> Sink<Outgoing>
    for TransferableTransport<T, Context, Incoming, Outgoing, Error>
where
    T: JsCast + AsRef<EventTarget> + PostMessage,
    Incoming: FromTransferable<Context, Error>,
    Outgoing: IntoTransferable<Context, Error>,
    Error: std::error::Error + 'static,
{
    type Error = dnet_base::Error<self::Error<Error>>;

    fn poll_ready(
        self: Pin<&mut Self>,
        _cx: &mut task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let result = Poll::Ready(Ok(()));

        #[cfg(feature = "logging")]
        self.logger.borrow().log_ready(&result);

        result
    }

    fn start_send(self: Pin<&mut Self>, item: Outgoing) -> Result<(), Self::Error> {
        let result = if self.state.borrow().closed {
            Err(dnet_base::Error::Closed)
        } else {
            self.send_message(item).map_err(dnet_base::Error::Other)
        };

        #[cfg(feature = "logging")]
        self.logger
            .borrow()
            .log_sending::<Outgoing, _>(&result, None);

        result
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let result = if self.state.borrow().closed {
            Poll::Ready(Err(dnet_base::Error::Closed))
        } else {
            Poll::Ready(Ok(()))
        };

        #[cfg(feature = "logging")]
        self.logger.borrow().log_flush(&result);

        result
    }

    fn poll_close(
        self: Pin<&mut Self>,
        _cx: &mut task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let result = if self.state.borrow().closed {
            Poll::Ready(Err(dnet_base::Error::Closed))
        } else {
            self.send_close();
            self.state.borrow_mut().close();
            Poll::Ready(Ok(()))
        };

        #[cfg(feature = "logging")]
        self.logger.borrow().log_close(&result);

        result
    }
}

impl<T, Context, Incoming, Outgoing, Error> Stream
    for TransferableTransport<T, Context, Incoming, Outgoing, Error>
where
    T: JsCast + AsRef<EventTarget> + PostMessage,
    Incoming: FromTransferable<Context, Error>,
    Outgoing: IntoTransferable<Context, Error>,
    Error: std::error::Error + 'static,
{
    type Item = Result<Incoming, self::Error<Error>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        let mut state = self.state.borrow_mut();
        let result = if state.is_terminated() {
            Poll::Ready(None)
        } else if let Some(item) = state.incoming.pop_front() {
            Poll::Ready(Some(item.and_then(|item| {
                <Incoming::Input>::reconstruct(item, &mut self.context.borrow_mut())
                    .map_err(self::Error::TransferableError)
                    .map(|item| Incoming::from_transferable(item))
            })))
        } else {
            state.update_waker_with(cx.waker());
            Poll::Pending
        };

        #[cfg(feature = "logging")]
        self.logger.borrow().log_receiving(&result, None);

        result
    }
}

impl<T, Context, Incoming, Outgoing, Error> FusedStream
    for TransferableTransport<T, Context, Incoming, Outgoing, Error>
where
    T: JsCast + AsRef<EventTarget> + PostMessage,
    Incoming: FromTransferable<Context, Error>,
    Outgoing: IntoTransferable<Context, Error>,
    Error: std::error::Error + 'static,
{
    fn is_terminated(&self) -> bool {
        self.state.borrow().is_terminated()
    }
}

#[cfg(feature = "logging")]
impl<T, Context, Incoming, Outgoing, Error> dnet_base::Logging
    for TransferableTransport<T, Context, Incoming, Outgoing, Error>
where
    T: JsCast + AsRef<EventTarget> + PostMessage,
    Incoming: FromTransferable<Context, Error>,
    Outgoing: IntoTransferable<Context, Error>,
    Error: std::error::Error + 'static,
{
    const KIND: &'static str = "Transferable";

    fn with_logger<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&dnet_base::Logger) -> R,
    {
        f(&self.logger.borrow())
    }

    fn with_logger_mut<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut dnet_base::Logger) -> R,
    {
        f(&mut self.logger.borrow_mut())
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use dnet_base::Receive;
    use dnet_codecs::{bincode, BincodeCodec};
    use dnet_macros::IntoTransferable;
    use dnet_tests::dtest_configure;
    use futures::{join, SinkExt};
    use js_sys::{ArrayBuffer, Uint32Array};
    use wasm_bindgen_test::wasm_bindgen_test;
    use web_sys::{MessageChannel, MessagePort};

    use crate::{wrapper, TransferableTransport};

    dtest_configure!();

    type Context = wrapper::Context<BincodeCodec>;
    type Error = wrapper::Error<bincode::EncodeError, bincode::DecodeError>;

    async fn create_transports<I, O>() -> (
        TransferableTransport<MessagePort, Context, I, O, Error>,
        TransferableTransport<MessagePort, Context, O, I, Error>,
    )
    where
        I: crate::FromTransferable<Context, Error> + crate::IntoTransferable<Context, Error>,
        O: crate::FromTransferable<Context, Error> + crate::IntoTransferable<Context, Error>,
    {
        let channel = MessageChannel::new().unwrap();

        let left_port = Rc::new(channel.port1());
        let right_port = Rc::new(channel.port2());

        let left = TransferableTransport::new(
            &left_port,
            None,
            Context::new(BincodeCodec::default()),
            true,
        );
        let right = TransferableTransport::new(
            &right_port,
            None,
            Context::new(BincodeCodec::default()),
            true,
        );

        left_port.start();
        right_port.start();

        let (left, right) = join!(left, right);
        let left = left.unwrap();
        let right = right.unwrap();

        (left, right)
    }

    #[wasm_bindgen_test]
    async fn test_into_transferable_struct_unnamed_fields() {
        #[derive(Debug, IntoTransferable)]
        struct Message(Vec<u32>, #[transferable] ArrayBuffer);

        #[derive(Debug, IntoTransferable)]
        struct Empty {}

        let (mut left, mut right) = create_transports::<Empty, Message>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let ints = vec![1, 2, 3];

        let array = Uint32Array::new_with_length(3);
        array.copy_from(&[4, 5, 6]);

        let message = Message(ints.clone(), array.buffer());

        left.send(message).await.unwrap();
        let received = right.receive().await.unwrap();

        assert_eq!(received.0, ints);
        assert_eq!(Uint32Array::new(&received.1).to_vec(), vec![4, 5, 6]);
    }

    #[wasm_bindgen_test]
    async fn test_into_transferable_struct() {
        #[derive(Debug, IntoTransferable)]
        struct Message {
            ints: Vec<u32>,

            #[transferable]
            array_buffer_1: ArrayBuffer,

            #[transferable]
            array_buffer_2: ArrayBuffer,
        }

        #[derive(Debug, IntoTransferable)]
        struct Empty {}

        let (mut left, mut right) = create_transports::<Empty, Message>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let ints = vec![1, 2, 3];

        let array_1 = Uint32Array::new_with_length(3);
        array_1.copy_from(&[4, 5, 6]);

        let array_2 = Uint32Array::new_with_length(3);
        array_2.copy_from(&[7, 8, 9]);

        let message = Message {
            ints: ints.clone(),
            array_buffer_1: array_1.buffer(),
            array_buffer_2: array_2.buffer(),
        };

        left.send(message).await.unwrap();
        let received = right.receive().await.unwrap();

        assert_eq!(received.ints, ints);
        assert_eq!(
            Uint32Array::new(&received.array_buffer_1).to_vec(),
            vec![4, 5, 6]
        );
        assert_eq!(
            Uint32Array::new(&received.array_buffer_2).to_vec(),
            vec![7, 8, 9]
        );
    }

    #[wasm_bindgen_test]
    async fn test_into_transferable_nested_struct() {
        #[derive(Debug, IntoTransferable)]
        struct Message {
            ints: Vec<u32>,

            #[transferable]
            array_buffer: ArrayBuffer,
        }

        #[derive(Debug, IntoTransferable)]
        struct Wrapper {
            #[into_transferable]
            message_1: Message,

            #[into_transferable]
            message_2: Message,

            ints: Vec<u32>,
        }

        #[derive(Debug, IntoTransferable)]
        struct Wrapper2 {
            #[into_transferable]
            wrapper: Wrapper,
        }

        #[derive(Debug, IntoTransferable)]
        struct Empty {}

        let (mut left, mut right) = create_transports::<Empty, Wrapper2>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let ints_1 = vec![1, 2, 3];
        let ints_2 = vec![4, 5, 6];
        let ints_3 = vec![7, 8, 9];

        let array_1 = Uint32Array::new_with_length(3);
        array_1.copy_from(&[10, 11, 12]);

        let array_2 = Uint32Array::new_with_length(3);
        array_2.copy_from(&[13, 14, 15]);

        let wrapper = Wrapper {
            message_1: Message {
                ints: ints_1.clone(),
                array_buffer: array_1.buffer(),
            },
            message_2: Message {
                ints: ints_2.clone(),
                array_buffer: array_2.buffer(),
            },
            ints: ints_3,
        };
        let wrapper_2 = Wrapper2 { wrapper };

        left.send(wrapper_2).await.unwrap();
        let received = right.receive().await.unwrap();

        assert_eq!(received.wrapper.message_1.ints, ints_1);
        assert_eq!(received.wrapper.message_2.ints, ints_2);
        assert_eq!(
            Uint32Array::new(&received.wrapper.message_1.array_buffer).to_vec(),
            vec![10, 11, 12]
        );
        assert_eq!(
            Uint32Array::new(&received.wrapper.message_2.array_buffer).to_vec(),
            vec![13, 14, 15]
        );
    }

    #[wasm_bindgen_test]
    async fn test_into_transferable_enum() {
        #[derive(Debug, IntoTransferable)]
        enum Message {
            Variant1 {
                ints: Vec<u32>,

                #[transferable]
                array_buffer_1: ArrayBuffer,

                #[transferable]
                array_buffer_2: ArrayBuffer,
            },
            Variant2 {
                ints: Vec<u32>,
            },
            Variant3 {
                #[transferable]
                array_buffer: ArrayBuffer,
            },
            Variant4,
        }

        #[derive(Debug, IntoTransferable)]
        struct Empty {}

        let (mut left, mut right) = create_transports::<Empty, Message>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let ints = vec![1, 2, 3];

        let array_1 = Uint32Array::new_with_length(3);
        array_1.copy_from(&[4, 5, 6]);

        let array_2 = Uint32Array::new_with_length(3);
        array_2.copy_from(&[7, 8, 9]);

        let message = Message::Variant1 {
            ints: ints.clone(),
            array_buffer_1: array_1.buffer(),
            array_buffer_2: array_2.buffer(),
        };

        left.send(message).await.unwrap();
        let received = right.receive().await.unwrap();

        if let Message::Variant1 {
            ints: ints_received,
            array_buffer_1,
            array_buffer_2,
        } = received
        {
            assert_eq!(ints_received, ints);
            assert_eq!(Uint32Array::new(&array_buffer_1).to_vec(), vec![4, 5, 6]);
            assert_eq!(Uint32Array::new(&array_buffer_2).to_vec(), vec![7, 8, 9]);
        } else {
            panic!("unexpected variant received");
        }

        let ints = vec![10, 11, 12];
        left.send(Message::Variant2 { ints: ints.clone() })
            .await
            .unwrap();
        let received = right.receive().await.unwrap();

        if let Message::Variant2 {
            ints: ints_received,
        } = received
        {
            assert_eq!(ints_received, ints);
        } else {
            panic!("unexpected variant received");
        }

        let array = Uint32Array::new_with_length(3);
        array.copy_from(&[13, 14, 15]);

        left.send(Message::Variant3 {
            array_buffer: array.buffer(),
        })
        .await
        .unwrap();
        let received = right.receive().await.unwrap();

        if let Message::Variant3 { array_buffer } = received {
            assert_eq!(Uint32Array::new(&array_buffer).to_vec(), vec![13, 14, 15]);
        } else {
            panic!("unexpected variant received");
        }

        left.send(Message::Variant4).await.unwrap();
        let received = right.receive().await.unwrap();

        if !matches!(received, Message::Variant4) {
            panic!("unexpected variant received");
        }
    }

    #[wasm_bindgen_test]
    async fn test_into_transferable_nested_enum() {
        #[derive(Debug, IntoTransferable)]
        struct Message {
            ints: Vec<u32>,

            #[transferable]
            array_buffer: ArrayBuffer,
        }

        #[derive(Debug, IntoTransferable)]
        enum Message2 {
            Variant1 {
                ints: Vec<u32>,

                #[transferable]
                array_buffer: ArrayBuffer,
            },

            Variant2 {
                #[into_transferable]
                message: Message,
            },
        }

        #[derive(Debug, IntoTransferable)]
        struct Empty {}

        let (mut left, mut right) = create_transports::<Empty, Message2>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let ints = vec![1, 2, 3];

        let array = Uint32Array::new_with_length(3);
        array.copy_from(&[4, 5, 6]);

        let message = Message2::Variant1 {
            ints: ints.clone(),
            array_buffer: array.buffer(),
        };

        left.send(message).await.unwrap();
        let received = right.receive().await.unwrap();

        if let Message2::Variant1 {
            ints: ints_received,
            array_buffer,
        } = received
        {
            assert_eq!(ints_received, ints);
            assert_eq!(Uint32Array::new(&array_buffer).to_vec(), vec![4, 5, 6]);
        } else {
            panic!("unexpected variant received");
        }

        let ints = vec![7, 8, 9];

        let array = Uint32Array::new_with_length(3);
        array.copy_from(&[10, 11, 12]);

        let message = Message2::Variant2 {
            message: Message {
                ints: ints.clone(),
                array_buffer: array.buffer(),
            },
        };
        left.send(message).await.unwrap();
        let received = right.receive().await.unwrap();

        if let Message2::Variant2 {
            message:
                Message {
                    ints: ints_received,
                    array_buffer,
                },
        } = received
        {
            assert_eq!(ints_received, ints);
            assert_eq!(Uint32Array::new(&array_buffer).to_vec(), vec![10, 11, 12]);
        } else {
            panic!("unexpected variant received");
        }
    }

    #[wasm_bindgen_test]
    async fn test_into_transferable_enum_unnamed_fields() {
        #[derive(Debug, IntoTransferable)]
        enum Message {
            Variant1(Vec<u32>, #[transferable] ArrayBuffer),
            Variant2(Vec<u32>),
            Variant3 {
                #[transferable]
                some_field: ArrayBuffer,
            },
            Variant4,
        }

        #[derive(Debug, IntoTransferable)]
        struct Empty {}

        let (mut left, mut right) = create_transports::<Empty, Message>().await;

        dnet_tests::init_logging(&mut left, &mut right);

        let ints = vec![1, 2, 3];

        let array = Uint32Array::new_with_length(3);
        array.copy_from(&[4, 5, 6]);

        let message = Message::Variant1(ints.clone(), array.buffer());

        left.send(message).await.unwrap();
        let received = right.receive().await.unwrap();

        if let Message::Variant1(ints_received, array_buffer) = received {
            assert_eq!(ints_received, ints);
            assert_eq!(Uint32Array::new(&array_buffer).to_vec(), vec![4, 5, 6]);
        } else {
            panic!("unexpected variant received");
        }

        let ints = vec![10, 11, 12];
        left.send(Message::Variant2(ints.clone())).await.unwrap();
        let received = right.receive().await.unwrap();

        if let Message::Variant2(ints_received) = received {
            assert_eq!(ints_received, ints);
        } else {
            panic!("unexpected variant received");
        }

        let array = Uint32Array::new_with_length(3);
        array.copy_from(&[13, 14, 15]);

        left.send(Message::Variant3 {
            some_field: array.buffer(),
        })
        .await
        .unwrap();
        let received = right.receive().await.unwrap();

        if let Message::Variant3 {
            some_field: array_buffer,
        } = received
        {
            assert_eq!(Uint32Array::new(&array_buffer).to_vec(), vec![13, 14, 15]);
        } else {
            panic!("unexpected variant received");
        }

        left.send(Message::Variant4).await.unwrap();
        let received = right.receive().await.unwrap();

        if !matches!(received, Message::Variant4) {
            panic!("unexpected variant received");
        }
    }
}
