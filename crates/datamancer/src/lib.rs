pub mod alpaca;
mod streaming_client;
mod subscription;
use intercom::Intercom;
use supermodel::{
    commands::{Command, SubscriptionError},
    environment::Env,
};

use alpaca::alpaca_streaming_client::{AlpacaStreamingClient, PricingSubscription};
use tokio_stream::StreamExt;

pub struct Datamancer {
    intercom: Intercom,
    alpaca_crypto_client: PricingSubscription,
    run: bool,
}

impl Datamancer {
    pub async fn initialize_datamancer() -> Datamancer {
        let env = Env::from_env(true);
        // Allows you to pass along context (i.e., trace IDs) across services
        println!(
            "Attempting to connect to redis instance at: {}",
            &env.redis_url
        );
        let intercom = Intercom::initialize(&env).await.unwrap();
        let transmitter = intercom.get_transmitter().await.unwrap();

        let alpaca_crypto_client = AlpacaStreamingClient::connect(
            &env,
            alpaca::alpaca_streaming_client::Feed::SIP,
            transmitter,
        )
        .await;

        Datamancer {
            run: true,
            intercom,
            alpaca_crypto_client,
        }
    }

    pub async fn run(&mut self) {
        let mut listener = self.intercom.get_listener().await.unwrap();
        let mut transmitter = self.intercom.get_transmitter().await.unwrap();
        listener.subscribe("cmd").await.unwrap();
        let mut pubsub_stream = listener.listen();
        while self.run {
            if let Some(message) = pubsub_stream.next().await {
                let message = message.unwrap();
                let result = self.process_command(&message.content).await;
                let serialized = serde_json::to_string(&result).unwrap();
                transmitter.send("cmd_rsp", &serialized).await.unwrap();
            } else {
                break;
            }
        }
    }

    async fn process_command(&mut self, command: &str) -> Result<(), SubscriptionError> {
        let command: Command = serde_json::from_str(command).unwrap();
        match command {
            Command::DataShutDown => {
                self.run = false;
                Ok(())
            }
            Command::DataSub(sub) => {
                let mut data_con = self.intercom.get_data_con().await.unwrap();
                subscription::subscribe(&mut data_con, sub, &mut self.alpaca_crypto_client).await
            }
            Command::DataUnsub(sub) => {
                let mut data_con = self.intercom.get_data_con().await.unwrap();
                subscription::unsubscribe(&mut data_con, sub, &mut self.alpaca_crypto_client).await
            }
            Command::DataListSub => {
                let data_con = self.intercom.get_data_con().await.unwrap();
                subscription::list(&data_con).await
            }
        }
    }
}
