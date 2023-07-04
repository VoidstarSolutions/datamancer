use intercom::data_con::DataCon;

use crate::{
    alpaca::{alpaca_streaming_client::PricingSubscription, SubscriptionList},
    commands::{DataProvider, Subscription, SubscriptionError},
};

#[derive(Debug)]
pub(crate) struct SubscriptionManager {
    data_con: DataCon,
    alpaca_crypto_client: PricingSubscription,
}

impl SubscriptionManager {
    pub(crate) fn new(data_con: DataCon, alpaca_crypto_client: PricingSubscription) -> Self {
        SubscriptionManager {
            data_con,
            alpaca_crypto_client,
        }
    }

    pub(crate) async fn load_subscriptions(&mut self) -> Result<(), SubscriptionError> {
        let provider = DataProvider::AlpacaCrypto;
        let key = format!("subscriptions:{:?}", provider);
        let serialized_subscriptions = self.data_con.get(key).await.unwrap();
        let subscriptions: SubscriptionList = match serialized_subscriptions {
            Some(serialized_subscriptions) => {
                serde_json::from_str(&serialized_subscriptions).unwrap()
            }
            None => SubscriptionList::new(),
        };
        println!("Loaded from DB:{:?}", subscriptions);
        if (subscriptions.bars.is_some()) {
            for symbol in subscriptions.bars.as_ref().unwrap() {
                let sub = Subscription {
                    provider: provider.clone(),
                    symbol: symbol.clone(),
                };
                self.subscribe_data_broker(&sub);
            }
        }
        Ok(())
    }

    pub fn list(&self) -> SubscriptionList {
        self.alpaca_crypto_client
            .active_subscriptions
            .lock()
            .unwrap()
            .clone()
    }

    pub async fn subscribe(&mut self, subscription: Subscription) -> Result<(), SubscriptionError> {
        if self.check_subscription_exists(&subscription) {
            return Err(SubscriptionError::SubscriptionExists);
        }
        self.subscribe_data_broker(&subscription);
        Ok(())
    }

    pub async fn unsubscribe(&mut self, sub: Subscription) -> Result<(), SubscriptionError> {
        if self.check_subscription_exists(&sub) {
            let _: () = self
                .data_con
                .del(format!("subscriptions:{:?}:{}", &sub.provider, &sub.symbol))
                .await
                .unwrap();
            let subscriptions = SubscriptionList::new()
                .add_bars(&sub.symbol)
                .add_quotes(&sub.symbol)
                .add_trades(&sub.symbol);
            self.alpaca_crypto_client.unsubscribe(subscriptions);
            Ok(())
        } else {
            Err(SubscriptionError::SubscriptionDoesNotExist)
        }
    }

    fn check_subscription_exists(&mut self, sub: &Subscription) -> bool {
        let subscriptions = self.list();
        if let Some(bars) = subscriptions.bars {
            if bars.contains(&sub.symbol) {
                return true;
            }
        }
        false
    }

    fn subscribe_data_broker(&mut self, subscription: &Subscription) {
        let subscriptions = SubscriptionList::new()
            .add_bars(&subscription.symbol)
            .add_quotes(&subscription.symbol)
            .add_trades(&subscription.symbol);
        self.alpaca_crypto_client.subscribe(subscriptions);
    }
}
