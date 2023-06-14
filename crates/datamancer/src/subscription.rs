use redis::{aio::MultiplexedConnection, AsyncCommands};
use supermodel::commands::Subscription;

async fn subscription_exists(
    connection: &mut MultiplexedConnection,
    subscription: &Subscription,
) -> bool {
    let result: u64 = connection
        .exists(format!(
            "subscriptions:{}:{}",
            &subscription.provider, &subscription.symbol
        ))
        .await
        .unwrap();
    result == 1
}

pub async fn list(_connection: &MultiplexedConnection) {}

pub async fn subscribe(connection: &mut MultiplexedConnection, subscription: Subscription) {
    if subscription_exists(connection, &subscription).await {
        println!(
            "Subscription to {} {} already exists!",
            &subscription.provider, &subscription.symbol
        )
    } else {
        println!(
            "Subscribing to {} {}",
            &subscription.provider, &subscription.symbol
        );

        let _: () = connection
            .set(
                format!(
                    "subscriptions:{}:{}",
                    &subscription.provider, &subscription.symbol
                ),
                true,
            )
            .await
            .unwrap();
    }
}

pub async fn unsubscribe(connection: &mut MultiplexedConnection, sub: Subscription) {
    if subscription_exists(connection, &sub).await {
        println!("Unsubscribing from {} {}", &sub.provider, &sub.symbol);
        let _: () = connection
            .del(format!("subscriptions:{}:{}", &sub.provider, &sub.symbol))
            .await
            .unwrap();
    } else {
        println!(
            "Subscription to {} {} does not exist!",
            &sub.provider, &sub.symbol
        )
    }
}
