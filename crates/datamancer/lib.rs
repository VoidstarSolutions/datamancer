pub mod alpaca;
pub mod commands;
mod data;
pub mod env;
mod streaming_client;
mod subscription;
use crate::{commands::Command, env::Env};

use alpaca::alpaca_streaming_client::{AlpacaStreamingClient, PricingSubscription};
use redis::{aio::MultiplexedConnection, Client};
use tokio_stream::StreamExt;

pub struct Datamancer {
    client: Client,
    _alpaca_crypto_client: PricingSubscription,
}

impl Datamancer {
    pub async fn initialize_datamancer() -> Datamancer {
        let env = Env::from_env();
        // Allows you to pass along context (i.e., trace IDs) across services
        println!(
            "Attempting to connect to redis instance at: {}",
            env.redis_url
        );
        let client = redis::Client::open(env.redis_url).unwrap();
        println!("Connected to redis instance");
        let _alpaca_crypto_client = AlpacaStreamingClient::connect(
            alpaca::AccountType::Paper,
            alpaca::alpaca_streaming_client::Feed::SIP,
        )
        .await;
        Datamancer {
            client,
            _alpaca_crypto_client,
        }
    }

    pub async fn run(&self) {
        let mut pubsub_conn = self
            .client
            .get_async_connection()
            .await
            .unwrap()
            .into_pubsub();
        pubsub_conn.subscribe("datamancer").await.unwrap();
        let mut pubsub_stream = pubsub_conn.on_message();
        let connection = self
            .client
            .get_multiplexed_tokio_connection()
            .await
            .unwrap();
        let mut run = true;
        while run {
            if let Some(message) = pubsub_stream.next().await {
                let connection_clone = connection.clone();
                let msg: String = message.get_payload().unwrap();
                run = process_command(connection_clone, &msg).await;
            }
        }
    }
}

async fn process_command(mut connection: MultiplexedConnection, command: &str) -> bool {
    let command: Command = serde_json::from_str(command).unwrap();
    match command {
        Command::ShutDown => return false,
        Command::Subscribe(sub) => subscription::subscribe(&mut connection, sub).await,
        Command::Unsubscribe(sub) => subscription::unsubscribe(&mut connection, sub).await,
        Command::ListSubscriptions => subscription::list(&connection).await,
    }
    true
}
