use tracing::error;

use crate::{
    Subscription,
    subscriptions::{AlpacaDataClient, SubscriptionRequest},
};
use tokio::sync::mpsc::{self, Sender};
use tracing::info;

use super::Error;

pub(super) struct Inner {
    requests: mpsc::Receiver<SubscriptionRequest>,
    alpaca: mpsc::Sender<SubscriptionRequest>,
    subscriptions: Vec<Subscription>,
}

impl Inner {
    pub(super) async fn new() -> Sender<SubscriptionRequest> {
        let (sender, receiver) = mpsc::channel(4);
        let alpaca = AlpacaDataClient::spawn().await;
        tokio::spawn(async move {
            let mut inner = Self {
                requests: receiver,
                alpaca,
                subscriptions: Vec::new(),
            };
            inner.run().await;
        });
        sender
    }

    async fn run(&mut self) {
        info!("Datamancer Running");
        while let Some(request) = self.requests.recv().await {
            match request {
                SubscriptionRequest::Subscribe {
                    subscription,
                    response,
                } => {
                    let subscription_result = self.subscribe(subscription).await;
                    match response.send(subscription_result) {
                        Ok(_) => (),
                        Err(_) => error!("Requester dropped before response could be sent!"),
                    }
                }
                SubscriptionRequest::Unsubscribe {
                    subscription,
                    response,
                } => {
                    let unsubscribe_result = self.unsubscribe(subscription).await;
                    match response.send(unsubscribe_result) {
                        Ok(_) => (),
                        Err(_) => error!("Requester dropped before response could be sent!"),
                    }
                }
                SubscriptionRequest::ShutDown { response } => {
                    let (message, response_channel) = SubscriptionRequest::shut_down();
                    self.alpaca.send(message).await.unwrap();
                    response_channel.await.unwrap().unwrap();
                    match response.send(Ok(())) {
                        Ok(_) => (),
                        Err(_) => error!("Requester dropped before response could be sent!"),
                    }
                    break;
                }
            }
        }
        info!("Datamancer Shutting Down");
    }

    pub async fn subscribe(
        &mut self,
        subscription: Subscription,
    ) -> Result<Vec<Subscription>, Error> {
        let (message, response_channel) = SubscriptionRequest::subscribe(subscription);
        self.alpaca.send(message).await.unwrap();
        match response_channel.await {
            Ok(Ok(subscriptions)) => {
                self.subscriptions = subscriptions;
                Ok(self.subscriptions.clone())
            }
            Ok(Err(e)) => Err(e),
            Err(_) => Err(Error::UnexpectedTermination),
        }
    }

    pub async fn unsubscribe(
        &mut self,
        subscription: Subscription,
    ) -> Result<Vec<Subscription>, Error> {
        let (message, response_channel) = SubscriptionRequest::unsubscribe(subscription);
        self.alpaca.send(message).await.unwrap();
        match response_channel.await {
            Ok(Ok(subscriptions)) => {
                self.subscriptions = subscriptions;
                Ok(self.subscriptions.clone())
            }
            Ok(Err(e)) => Err(e),
            Err(_) => Err(Error::UnexpectedTermination),
        }
    }
}
