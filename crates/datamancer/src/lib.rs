pub mod alpaca;
pub mod commands;
mod streaming_client;
mod subscription;

use alpaca::alpaca_streaming_client::AlpacaStreamingClient;
use commands::{Command, SubscriptionError};
use intercom::Intercom;
use subscription::SubscriptionManager;
use supermodel::environment::Env;
use tokio_stream::StreamExt;

pub struct Datamancer {
    intercom: Intercom,
    subscription_manager: SubscriptionManager,
    run: bool,
}

impl Datamancer {
    pub async fn initialize_datamancer() -> Datamancer {
        let env = Env::from_env(true);
        let mut intercom = Intercom::initialize(&env).await.unwrap();
        let data_con = intercom.get_data_con().await.unwrap();
        let transmitter = intercom.get_transmitter().await.unwrap();
        let alpaca_crypto_client = AlpacaStreamingClient::connect(
            &env,
            alpaca::alpaca_streaming_client::Feed::SIP,
            data_con,
            transmitter,
        )
        .await;
        let subscription_manager =
            SubscriptionManager::new(intercom.get_data_con().await.unwrap(), alpaca_crypto_client);

        Datamancer {
            run: true,
            intercom,
            subscription_manager,
        }
    }

    pub async fn run(&mut self) {
        let _ = self
            .subscription_manager
            .load_subscriptions()
            .await
            .unwrap();
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
            Command::ShutDown => {
                self.run = false;
                Ok(())
            }
            Command::Sub(sub) => self.subscription_manager.subscribe(sub).await,
            Command::Unsub(sub) => self.subscription_manager.unsubscribe(sub).await,
            Command::List => {
                let data = self.subscription_manager.list();
                Ok(())
            }
        }
    }
}
