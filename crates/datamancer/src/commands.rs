use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub enum SubscriptionError {
    DatabaseError,
    SubscriptionExists,
    SubscriptionDoesNotExist,
    AuthFailed,
    AlreadyAuthorized,
    AuthTimeout,
}

#[derive(Clone, Debug, Deserialize, Parser, Serialize)]
pub struct Subscription {
    // Data provider for Subscription
    pub provider: DataProvider,
    // Symbol for the subscription
    pub symbol: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, ValueEnum)]
pub enum DataProvider {
    AlpacaCrypto,
    AlpacaStock,
}
/// The following represent messages we can listen for
#[derive(Clone, Debug, Deserialize, Subcommand, Serialize)]
pub enum Command {
    /// Shut down Datamancer processing
    ShutDown,
    Sub(Subscription),
    Unsub(Subscription),
    #[serde(rename = "ls")]
    List,
}
