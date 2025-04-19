use datamancer::{Datamancer, Subscription};
use tracing::level_filters::LevelFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::INFO)
        .init();
    let subscription_manager = Datamancer::spawn_subscription_manager().await;
    let subscription = Subscription::new("FAKEPACA".to_string());
    for _ in 0..3 {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        subscription_manager.subscribe(&subscription).await.unwrap();
    }
    subscription_manager.shut_down().await.unwrap();
}
