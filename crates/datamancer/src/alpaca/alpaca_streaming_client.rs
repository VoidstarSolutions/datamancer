use crate::{
    alpaca::{AccountType, Request},
    streaming_client::StreamingClient,
};
use futures::StreamExt;
use futures_util::Stream;
use serde::{Deserialize, Serialize};
use supermodel::environment::Env;

use super::SubscriptionList;

pub(crate) const MARKET_DATA_STREAM_HOST: &str =
    "wss://stream.data.alpaca.markets/v1beta3/crypto/us";

/// An enumeration of the different supported data feeds for streaming stock data
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Feed {
    /// Use the Investors Exchange (IEX) as the data source.
    ///
    /// This feed is available to all accounts
    IEX,
    /// This feed is only usable with the unlimited data plan
    SIP,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ControlMessage {
    Connected,
    Authenticated,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename = "lowercase", tag = "code")]
pub enum Error {
    #[serde(rename = "400")]
    InvalidSyntax,
    #[serde(rename = "401")]
    NotAuthenticated,
    #[serde(rename = "402")]
    AuthFailed,
    #[serde(rename = "403")]
    AlreadyAuthorized,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Quote {
    #[serde(rename = "S")]
    pub symbol: String,
    #[serde(rename = "ax")]
    pub ask_exchange: Option<String>,
    #[serde(rename = "ap")]
    pub ask_price: f64,
    #[serde(rename = "as")]
    pub ask_size: f64,
    #[serde(rename = "bx")]
    pub bid_exchange: Option<String>,
    #[serde(rename = "bp")]
    pub bid_price: f64,
    #[serde(rename = "bs")]
    pub bid_size: f64,
    #[serde(rename = "s")]
    pub trade_size: Option<f64>,
    #[serde(rename = "t")]
    pub timestamp: String,
    #[serde(rename = "z")]
    pub tape: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Trade {
    #[serde(rename = "S")]
    pub symbol: String,
    #[serde(rename = "i")]
    pub trade_id: i64,
    #[serde(rename = "x")]
    pub exchange: Option<String>,
    #[serde(rename = "p")]
    pub price: f64,
    #[serde(rename = "s")]
    pub size: f64,
    #[serde(rename = "t")]
    pub timestamp: String,
    #[serde(rename = "c")]
    pub conditions: Option<Vec<String>>,
    #[serde(rename = "z")]
    pub tape: Option<String>,
}

/// The following represent messages we can listen for
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "T")]
pub enum StreamMessage {
    /// Internally consumed stream acknowledging successful completion of requests
    #[serde(rename = "success")]
    Control { msg: ControlMessage },

    #[serde(rename = "error")]
    Error(Error),

    #[serde(rename = "subscription")]
    Subscription(SubscriptionList),

    #[serde(rename = "t")]
    Trade(Trade),
    #[serde(rename = "q")]
    Quote(Quote),
}

pub struct PricingSubscription {
    streaming_client: StreamingClient,
    _data_stream: Box<dyn Stream<Item = Vec<StreamMessage>>>,
}

impl PricingSubscription {
    pub fn subscribe(&mut self, subscriptions: SubscriptionList) {
        let request = Request::Subscribe(subscriptions);
        let serialized = serde_json::to_string(&request).unwrap();
        self.streaming_client.send(&serialized);
    }
    pub fn shutdown(&mut self) {
        self.streaming_client.shutdown();
    }
}

#[derive(Debug)]
pub struct AlpacaStreamingClient {}

impl AlpacaStreamingClient {
    pub async fn connect(environment: &Env, feed: Feed) -> PricingSubscription {
        let url = match feed {
            Feed::IEX => MARKET_DATA_STREAM_HOST.to_string(), /*+ "/iex"*/
            Feed::SIP => MARKET_DATA_STREAM_HOST.to_string(), /*+ "/sip"*/
        };
        println!("Connecting to {}", url);
        let mut streaming_client = StreamingClient::new(environment, &url);
        let inner_stream = streaming_client.connect().await.map(move |msg| {
            let messages: Vec<StreamMessage> = serde_json::from_str(&msg).unwrap();
            messages
        });
        let auth_request = Request::AuthMessage {
            key: streaming_client.env.alpaca_key_id.as_ref().unwrap().clone(),
            secret: streaming_client
                .env
                .alpaca_secret_key
                .as_ref()
                .unwrap()
                .clone(),
        };
        let serialized = serde_json::to_string(&auth_request).unwrap();
        streaming_client.send(&serialized);

        PricingSubscription {
            streaming_client,
            _data_stream: Box::new(inner_stream),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alpaca::AccountType;
    use serial_test::parallel;

    /// Check that we can decode a response containing no bars correctly.
    #[tokio::test]
    #[parallel]
    async fn ensure_connection() {
        let env = Env::from_env(true);
        let mut subscription = AlpacaStreamingClient::connect(&env, Feed::SIP).await;
        subscription.shutdown();
    }
}
