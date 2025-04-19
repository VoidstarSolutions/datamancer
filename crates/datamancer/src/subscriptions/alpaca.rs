use std::collections::HashMap;

use oxidized_alpaca::{
    AccountType,
    streaming::{
        StreamingMarketDataClient,
        stock_data::{Request, StreamMessage, SubscriptionList},
    },
};
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::{Error, subscriptions::SubscriptionRequest};

use super::Subscription;

pub(crate) struct AlpacaDataClient {
    account_type: AccountType,
    request_receiver: mpsc::Receiver<SubscriptionRequest>,
    alpaca_client: Option<StreamingMarketDataClient<Vec<StreamMessage>, Request>>,
}

impl AlpacaDataClient {
    pub async fn spawn() -> mpsc::Sender<SubscriptionRequest> {
        let (request_sender, request_receiver) = mpsc::channel(4);
        tokio::spawn(async move {
            let mut alpaca = Self {
                account_type: AccountType::Paper,
                request_receiver,
                alpaca_client: None,
            };
            alpaca.run().await;
        });
        request_sender
    }

    async fn run(&mut self) {
        loop {
            let request = match self.request_receiver.recv().await {
                Some(request) => request,
                None => {
                    error!("Alpaca request sender dropped without shutting down Alpaca");
                    break;
                }
            };
            match request {
                SubscriptionRequest::Subscribe {
                    subscription,
                    response,
                } => {
                    self.subscribe(&subscription).await;
                    response.send(Ok(vec![])).unwrap();
                }
                SubscriptionRequest::Unsubscribe {
                    subscription,
                    response,
                } => {
                    let results = self.unsubscribe(&subscription).await;
                    response.send(results).unwrap();
                }
                SubscriptionRequest::ShutDown { response } => {
                    if let Some(client) = self.alpaca_client.take() {
                        client.shut_down().await.unwrap();
                    }
                    response.send(Ok(())).unwrap();
                    break;
                }
            }
        }
        info!("Alpaca Data Client shutting down")
    }

    async fn subscribe(&mut self, subscription: &Subscription) {
        let subscription_list = SubscriptionList::from(subscription);
        if self.alpaca_client.is_none() {
            info!("Alpaca client not connected, attempting connection");
            self.alpaca_client = Some(
                StreamingMarketDataClient::new_test_client(self.account_type)
                    .await
                    .unwrap(),
            );
            info!("Alpaca client connected");
            let subscriptions = self
                .alpaca_client
                .as_mut()
                .unwrap()
                .add_subscriptions(&subscription_list)
                .await
                .unwrap();
            info!("Subscribed to: {:?}", subscriptions);
        }
    }

    async fn unsubscribe(
        &mut self,
        subscription: &Subscription,
    ) -> Result<Vec<Subscription>, Error> {
        let subscription_list = SubscriptionList::from(subscription);

        let subscriptions = self
            .alpaca_client
            .as_mut()
            .unwrap()
            .remove_subscriptions(&subscription_list)
            .await?;
        Ok(subscription_list_to_subscriptions(subscriptions))
    }
}

impl From<&Subscription> for SubscriptionList {
    fn from(subscription: &Subscription) -> Self {
        let mut sub = SubscriptionList::new();
        if subscription.quotes {
            sub = sub.add_quotes(&subscription.instrument);
        }
        if subscription.trades {
            sub = sub.add_trades(&subscription.instrument);
        }
        if subscription.bars {
            sub = sub.add_minute_bars(&subscription.instrument);
        }
        if subscription.daily_bars {
            sub = sub.add_daily_bars(&subscription.instrument);
        }
        if subscription.updated_bars {
            sub = sub.add_updated_bars(&subscription.instrument);
        }
        if subscription.news {
            sub = sub.add_news(&subscription.instrument);
        }
        sub
    }
}

fn subscription_list_to_subscriptions(subscription_list: SubscriptionList) -> Vec<Subscription> {
    let mut subscriptions: HashMap<String, Subscription> = HashMap::new();
    if let Some(bars) = subscription_list.bars {
        for instrument in bars {
            let sub = subscriptions
                .entry(instrument.clone())
                .or_insert(Subscription::new(instrument));
            sub.with_bars(true);
        }
    }
    if let Some(daily_bars) = subscription_list.daily_bars {
        for instrument in daily_bars {
            let sub = subscriptions
                .entry(instrument.clone())
                .or_insert(Subscription::new(instrument));
            sub.with_daily_bars(true);
        }
    }
    if let Some(updated_bars) = subscription_list.updated_bars {
        for instrument in updated_bars {
            let sub = subscriptions
                .entry(instrument.clone())
                .or_insert(Subscription::new(instrument));
            sub.with_updated_bars(true);
        }
    }
    if let Some(quotes) = subscription_list.quotes {
        for instrument in quotes {
            let sub = subscriptions
                .entry(instrument.clone())
                .or_insert(Subscription::new(instrument));
            sub.with_quotes(true);
        }
    }
    if let Some(trades) = subscription_list.trades {
        for instrument in trades {
            let sub = subscriptions
                .entry(instrument.clone())
                .or_insert(Subscription::new(instrument));
            sub.with_trades(true);
        }
    }
    if let Some(news) = subscription_list.news {
        for instrument in news {
            let sub = subscriptions
                .entry(instrument.clone())
                .or_insert(Subscription::new(instrument));
            sub.with_news(true);
        }
    }
    subscriptions.into_values().collect()
}
