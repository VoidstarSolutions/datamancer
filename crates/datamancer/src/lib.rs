pub mod alpaca;
mod streaming_client;
mod subscription;
use supermodel::{
    commands::{Command, SubscriptionError},
    environment::Env,
};

use alpaca::alpaca_streaming_client::{AlpacaStreamingClient, PricingSubscription};
use redis::{aio::MultiplexedConnection, AsyncCommands, Client, Commands};
use tokio_stream::StreamExt;

pub struct Datamancer {
    client: Client,
    alpaca_crypto_client: PricingSubscription,
}

impl Datamancer {
    pub async fn initialize_datamancer() -> Datamancer {
        let env = Env::from_env(true);
        // Allows you to pass along context (i.e., trace IDs) across services
        println!(
            "Attempting to connect to redis instance at: {}",
            &env.redis_url
        );
        let client = redis::Client::open(env.redis_url.as_ref()).unwrap();
        println!("Connected to redis instance");
        let alpaca_crypto_client =
            AlpacaStreamingClient::connect(&env, alpaca::alpaca_streaming_client::Feed::SIP).await;
        Datamancer {
            client,
            alpaca_crypto_client,
        }
    }

    pub async fn run(&mut self) {
        let mut pubsub_conn = self
            .client
            .get_async_connection()
            .await
            .unwrap()
            .into_pubsub();
        pubsub_conn.subscribe("commander").await.unwrap();
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
                let result = self.process_command(connection_clone, &msg).await;
                let serialized = serde_json::to_string(&result).unwrap();
                let _: () = self
                    .client
                    .get_connection()
                    .unwrap()
                    .publish("commanded", serialized)
                    .unwrap();
            }
        }
    }
    async fn process_command(
        &mut self,
        mut connection: MultiplexedConnection,
        command: &str,
    ) -> Result<(), SubscriptionError> {
        let command: Command = serde_json::from_str(command).unwrap();
        match command {
            Command::DataShutDown => return Ok(()),
            Command::DataSub(sub) => {
                subscription::subscribe(&mut connection, sub, &mut self.alpaca_crypto_client).await
            }
            Command::DataUnsub(sub) => {
                subscription::unsubscribe(&mut connection, sub, &mut self.alpaca_crypto_client)
                    .await
            }
            Command::DataListSub => subscription::list(&connection).await,
        }
    }
}
