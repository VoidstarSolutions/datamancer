use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{alpaca::Request, streaming_client::StreamingClient};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use spinners::{Spinner, Spinners};
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

#[derive(PartialEq)]
pub enum StreamingClientState {
    Error,
    NotConnected,
    Connecting,
    Connected,
    Authenticating,
    Authenticated,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ControlMessage {
    Connected,
    Authenticated,
}

#[derive(Clone, Debug, Serialize_repr, Deserialize_repr)]
#[repr(u16)]
pub enum Error {
    InvalidSyntax = 400,
    NotAuthenticated = 401,
    AuthFailed = 402,
    AlreadyAuthorized = 403,
    AuthTimeout = 404,
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
    Error { code: Error, msg: String },

    #[serde(rename = "subscription")]
    Subscription(SubscriptionList),

    #[serde(rename = "t")]
    Trade(Trade),
    #[serde(rename = "q")]
    Quote(Quote),
}

pub struct PricingSubscription {
    streaming_client: StreamingClient,
}

impl PricingSubscription {
    pub fn subscribe(&mut self, subscriptions: SubscriptionList) {
        let request = Request::Subscribe(subscriptions);
        self.streaming_client.send(request);
    }
    pub fn unsubscribe(&mut self, subscriptions: SubscriptionList) {
        let request = Request::Unsubscribe(subscriptions);
        self.streaming_client.send(request);
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
        let mut inner_stream = streaming_client.connect().await.map(move |msg| {
            let messages: Vec<StreamMessage> = serde_json::from_str(&msg).unwrap();
            messages
        });
        let client_state = Arc::new(Mutex::new(StreamingClientState::NotConnected));
        let thread_state = client_state.clone();
        tokio::spawn(async move {
            while let Some(result) = inner_stream.next().await {
                result.iter().for_each(|msg| {
                    println!("Received message: {:?}", msg);
                    match msg {
                        StreamMessage::Control { msg } => match msg {
                            ControlMessage::Connected => {
                                println!("Connected to {}", url);
                                *thread_state.lock().unwrap() = StreamingClientState::Connected;
                            }
                            ControlMessage::Authenticated => {
                                println!("Authenticated to {}", url);
                                *thread_state.lock().unwrap() = StreamingClientState::Authenticated;
                            }
                        },
                        StreamMessage::Error { code, msg } => {
                            println!("Error: {:?} {}", code, msg);
                        }
                        StreamMessage::Subscription(subscriptions) => {
                            println!("Subscribed to {:?}", subscriptions);
                        }
                        StreamMessage::Trade(trade) => {
                            println!("Trade: {:?}", trade);
                        }
                        StreamMessage::Quote(quote) => {
                            println!("Quote: {:?}", quote);
                        }
                    }
                });
            }
        });
        let mut sp = Spinner::new(Spinners::Aesthetic, "Waiting for connection".into());
        while *client_state.lock().unwrap() == StreamingClientState::NotConnected {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        sp.stop_with_message("Websocket Connected\n".into());

        let auth_request = Request::AuthMessage {
            key: streaming_client.env.alpaca_key_id.as_ref().unwrap().clone(),
            secret: streaming_client
                .env
                .alpaca_secret_key
                .as_ref()
                .unwrap()
                .clone(),
        };
        streaming_client.send(auth_request);
        *client_state.lock().unwrap() = StreamingClientState::Authenticating;
        let mut sp = Spinner::new(Spinners::Aesthetic, "Waiting for auth response".into());
        while *client_state.lock().unwrap() == StreamingClientState::Authenticating {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        sp.stop_with_message("Authenticated\n".into());

        PricingSubscription { streaming_client }
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
