use serde::{Deserialize, Serialize};

pub mod alpaca_streaming_client;
/// The type of Alpaca account
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AccountType {
    /// Paper trading account
    Paper,
    /// Live trading account
    Live,
}

/// All the possible market data types that can be subscribed to for streaming clients
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SubscriptionList {
    /// List of symbols for bars subscriptions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bars: Option<Vec<String>>,
    /// List of symbols for quotes subscriptions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quotes: Option<Vec<String>>,
    /// List of symbols for trades subscriptions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trades: Option<Vec<String>>,
    /// List of symbols for news subscriptions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub news: Option<Vec<String>>,
}

impl SubscriptionList {
    /// Create a new SubscriptionList
    pub fn new() -> Self {
        Self {
            bars: None,
            quotes: None,
            trades: None,
            news: None,
        }
    }

    /// Add a symbol to the bars subscription list
    pub fn add_bars(mut self, symbol: &str) -> Self {
        if let Some(bars) = &mut self.bars {
            bars.push(symbol.to_string());
        } else {
            self.bars = Some(vec![symbol.to_string()]);
        }
        self
    }

    /// Add a symbol to the quotes subscription list
    pub fn add_quotes(mut self, symbol: &str) -> Self {
        if let Some(quotes) = &mut self.quotes {
            quotes.push(symbol.to_string());
        } else {
            self.quotes = Some(vec![symbol.to_string()]);
        }
        self
    }

    /// Add a symbol to the trades subscription list
    pub fn add_trades(mut self, symbol: &str) -> Self {
        if let Some(trades) = &mut self.trades {
            trades.push(symbol.to_string());
        } else {
            self.trades = Some(vec![symbol.to_string()]);
        }
        self
    }

    /// Add a symbol to the news subscription list
    pub fn add_news(mut self, symbol: &str) -> Self {
        if let Some(news) = &mut self.news {
            news.push(symbol.to_string());
        } else {
            self.news = Some(vec![symbol.to_string()]);
        }
        self
    }
}

/// Streaming Authentication Message
#[derive(Serialize)]
#[serde(tag = "action")]
pub enum Request {
    #[serde(rename = "auth")]
    AuthMessage { key: String, secret: String },
    #[serde(rename = "subscribe")]
    Subscribe(SubscriptionList),
    #[serde(rename = "unsubscribe")]
    Unsubscribe(SubscriptionList),
}
