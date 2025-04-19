mod error;
mod inner;
mod subscriptions;

pub use error::Error;
use inner::Inner;

use serde::{Deserialize, Serialize};
use subscriptions::SubscriptionRequest;
use tokio::sync::mpsc;
use tracing::{info, trace};

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Subscription {
    pub instrument: String,
    pub trades: bool,
    pub quotes: bool,
    pub bars: bool,
    pub daily_bars: bool,
    pub updated_bars: bool,
    pub news: bool,
}

impl Subscription {
    pub fn new(instrument: String) -> Self {
        Self {
            instrument,
            trades: false,
            quotes: false,
            bars: false,
            daily_bars: false,
            updated_bars: false,
            news: false,
        }
    }
    pub fn with_trades(&mut self, trades: bool) {
        self.trades = trades;
    }
    pub fn with_quotes(&mut self, quotes: bool) {
        self.quotes = quotes;
    }
    pub fn with_bars(&mut self, bars: bool) {
        self.bars = bars;
    }
    pub fn with_daily_bars(&mut self, daily_bars: bool) {
        self.daily_bars = daily_bars;
    }
    pub fn with_updated_bars(&mut self, updated_bars: bool) {
        self.updated_bars = updated_bars;
    }
    pub fn with_news(&mut self, news: bool) {
        self.news = news;
    }
}

#[derive(Debug)]
pub struct Datamancer {
    sender: mpsc::Sender<SubscriptionRequest>,
}

impl Datamancer {
    pub async fn spawn_subscription_manager() -> Self {
        info!("Spawning Datamancer");
        let sender = Inner::new().await;
        Self { sender }
    }

    pub async fn subscribe(&self, subscription: &Subscription) -> Result<Vec<Subscription>, Error> {
        trace!("Subscription request: {subscription:?}");
        let (request, response_channel) = SubscriptionRequest::subscribe(subscription.clone());
        self.sender.send(request).await.unwrap();
        match response_channel.await {
            Ok(Ok(subscriptions)) => Ok(subscriptions),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(Error::UnexpectedTermination),
        }
    }

    pub async fn unsubscribe(
        &self,
        subscription: &Subscription,
    ) -> Result<Vec<Subscription>, Error> {
        let (request, response_channel) = SubscriptionRequest::unsubscribe(subscription.clone());
        self.sender.send(request).await.unwrap();
        match response_channel.await {
            Ok(Ok(subscriptions)) => Ok(subscriptions),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(Error::UnexpectedTermination),
        }
    }

    pub async fn shut_down(self) -> Result<(), Error> {
        let (request, response_channel) = SubscriptionRequest::shut_down();
        self.sender.send(request).await.unwrap();
        match response_channel.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(Error::UnexpectedTermination),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_subscription_manager() {
        let controller = Datamancer::spawn_subscription_manager().await;
        let subscription = Subscription {
            instrument: "FAKEPACA".to_string(),
            trades: true,
            quotes: true,
            bars: true,
            daily_bars: true,
            updated_bars: true,
            news: true,
        };
        let subscriptions = controller.subscribe(&subscription).await.unwrap();
        assert!(subscriptions.len() == 1);
        tokio::time::sleep(Duration::from_secs(5)).await;
        controller.unsubscribe(&subscription).await.unwrap();
    }
}
