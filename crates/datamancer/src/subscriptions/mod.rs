mod alpaca;
pub(crate) use alpaca::AlpacaDataClient;

use crate::{Error, Subscription};
use tokio::sync::oneshot;

#[derive(Debug)]
pub(super) enum SubscriptionRequest {
    Subscribe {
        subscription: Subscription,
        response: oneshot::Sender<Result<Vec<Subscription>, Error>>,
    },
    Unsubscribe {
        subscription: Subscription,
        response: oneshot::Sender<Result<Vec<Subscription>, Error>>,
    },
    ShutDown {
        response: oneshot::Sender<Result<(), Error>>,
    },
}

impl SubscriptionRequest {
    pub fn subscribe(
        subscription: Subscription,
    ) -> (Self, oneshot::Receiver<Result<Vec<Subscription>, Error>>) {
        let (response_sender, response_receiver) = oneshot::channel();
        (
            Self::Subscribe {
                subscription,
                response: response_sender,
            },
            response_receiver,
        )
    }

    pub fn unsubscribe(
        subscription: Subscription,
    ) -> (Self, oneshot::Receiver<Result<Vec<Subscription>, Error>>) {
        let (response_sender, response_receiver) = oneshot::channel();
        (
            Self::Unsubscribe {
                subscription,
                response: response_sender,
            },
            response_receiver,
        )
    }

    pub fn shut_down() -> (Self, oneshot::Receiver<Result<(), Error>>) {
        let (response_sender, response_receiver) = oneshot::channel();
        (
            Self::ShutDown {
                response: response_sender,
            },
            response_receiver,
        )
    }
}
