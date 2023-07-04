use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{alpaca::Request, streaming_client::StreamingClient};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use intercom::{data_con::DataCon, tx::TX};
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
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Bar {
    #[serde(rename = "S")]
    pub symbol: String,
    #[serde(rename = "o")]
    pub open_price: f64,
    #[serde(rename = "h")]
    pub high_price: f64,
    #[serde(rename = "l")]
    pub low_price: f64,
    #[serde(rename = "c")]
    pub close_price: f64,
    #[serde(rename = "v")]
    pub volume: f64,
    #[serde(rename = "t")]
    pub timestamp: String,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
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

#[derive(Clone, Debug, Deserialize, Serialize)]
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
    #[serde(rename = "b")]
    MinuteBar(Bar),
    #[serde(rename = "u")]
    MinuteUpdateBar(Bar),
    #[serde(rename = "d")]
    DailyBar(Bar),
}

#[derive(Debug)]
pub struct PricingSubscription {
    streaming_client: StreamingClient,
    pub active_subscriptions: Arc<Mutex<SubscriptionList>>,
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
    pub async fn connect(
        environment: &Env,
        feed: Feed,
        mut data_con: DataCon,
        mut transmitter: TX,
    ) -> PricingSubscription {
        let url = match feed {
            Feed::IEX => MARKET_DATA_STREAM_HOST.to_string(), /*+ "/iex"*/
            Feed::SIP => MARKET_DATA_STREAM_HOST.to_string(), /*+ "/sip"*/
        };
        println!("Connecting to {}", url);
        let active_subscriptions = Arc::from(Mutex::from(SubscriptionList {
            trades: None,
            quotes: None,
            bars: None,
            news: None,
        }));
        let sub_ref = active_subscriptions.clone();
        let mut streaming_client = StreamingClient::new(environment, &url);
        let mut inner_stream = streaming_client.connect().await.map(move |msg| {
            let messages: Vec<StreamMessage> = serde_json::from_str(&msg).unwrap();
            messages
        });
        let client_state = Arc::new(Mutex::new(StreamingClientState::NotConnected));
        let thread_state = client_state.clone();

        tokio::spawn(async move {
            while let Some(result) = inner_stream.next().await {
                let start = Instant::now();

                for msg in result.iter() {
                    match msg {
                        StreamMessage::Control { msg } => match msg {
                            ControlMessage::Connected => {
                                *thread_state.lock().unwrap() = StreamingClientState::Connected;
                            }
                            ControlMessage::Authenticated => {
                                *thread_state.lock().unwrap() = StreamingClientState::Authenticated;
                            }
                        },
                        StreamMessage::Error { code, msg } => {
                            println!("Error: {:?} {}", code, msg);
                        }
                        StreamMessage::Subscription(subscriptions) => {
                            *sub_ref.lock().unwrap() = subscriptions.clone();
                            let subscription_key = "subscriptions:AlpacaCrypto".to_string();
                            let serialized = serde_json::to_string(&subscriptions).unwrap();
                            data_con.set(subscription_key, serialized).await.unwrap();
                        }
                        StreamMessage::Trade(trade) => {
                            let serialized_trade = serde_json::to_string(&trade).unwrap();
                            transmitter.send("trade", &serialized_trade).await.unwrap();
                        }
                        StreamMessage::Quote(quote) => {
                            let quote = supermodel::data::quote::Quote {
                                symbol: quote.symbol.clone(),
                                ask_price: quote.ask_price,
                                ask_size: quote.ask_size,
                                bid_price: quote.bid_price,
                                bid_size: quote.bid_size,
                                timestamp: DateTime::parse_from_rfc3339(&quote.timestamp)
                                    .unwrap()
                                    .with_timezone(&Utc),
                            };

                            let serialized_quote = serde_json::to_string(&quote).unwrap();
                            transmitter.send("quote", &serialized_quote).await.unwrap();
                            println!("Time to publish quote: {:?}", start.elapsed());
                        }
                        StreamMessage::MinuteBar(bar) => {
                            let serialized_bar = serde_json::to_string(&bar).unwrap();
                            transmitter
                                .send("minute_bar", &serialized_bar)
                                .await
                                .unwrap();
                        }
                        StreamMessage::MinuteUpdateBar(bar) => {
                            let serialized_bar = serde_json::to_string(&bar).unwrap();
                            transmitter
                                .send("minute_update_bar", &serialized_bar)
                                .await
                                .unwrap();
                        }
                        StreamMessage::DailyBar(bar) => {
                            let serialized_bar = serde_json::to_string(&bar).unwrap();
                            transmitter
                                .send("daily_bar", &serialized_bar)
                                .await
                                .unwrap();
                        }
                    }
                }
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

        PricingSubscription {
            streaming_client,
            active_subscriptions,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use intercom::{data_con, Intercom};
    use serial_test::parallel;

    /// Check that we can decode a response containing no bars correctly.
    #[tokio::test]
    #[parallel]
    async fn ensure_connection() {
        let env = Env::from_env(true);
        let mut intercom = Intercom::initialize(&env).await.unwrap();
        let data_con = intercom.get_data_con().await.unwrap();
        let transmitter = intercom.get_transmitter().await.unwrap();
        let mut subscription =
            AlpacaStreamingClient::connect(&env, Feed::SIP, data_con, transmitter).await;
        subscription.shutdown();
    }
}
