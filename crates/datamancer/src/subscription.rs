use redis::{aio::MultiplexedConnection, AsyncCommands};
use supermodel::commands::{Subscription, SubscriptionError};

use crate::alpaca::{alpaca_streaming_client::PricingSubscription, SubscriptionList};

async fn check_subscription_exists(
    connection: &mut MultiplexedConnection,
    subscription: &Subscription,
) -> bool {
    let result: u64 = connection
        .exists(format!(
            "subscriptions:{:?}:{}",
            &subscription.provider, &subscription.symbol
        ))
        .await
        .unwrap();
    result == 1
}

pub async fn list(_connection: &MultiplexedConnection) -> Result<(), SubscriptionError> {
    Ok(())
}

pub async fn subscribe(
    connection: &mut MultiplexedConnection,
    subscription: Subscription,
    data_client: &mut PricingSubscription,
) -> Result<(), SubscriptionError> {
    if check_subscription_exists(connection, &subscription).await {
        return Err(SubscriptionError::SubscriptionExists);
    } else if !subscribe_database(connection, &subscription).await {
        return Err(SubscriptionError::DatabaseError);
    }
    subscribe_data_broker(&subscription, data_client);
    Ok(())
}
async fn subscribe_database(
    connection: &mut MultiplexedConnection,
    subscription: &Subscription,
) -> bool {
    let res: Result<(), redis::RedisError> = connection
        .set(
            format!(
                "subscriptions:{:?}:{}",
                &subscription.provider, &subscription.symbol
            ),
            true,
        )
        .await;
    res.is_ok()
}

fn subscribe_data_broker(subscription: &Subscription, data_client: &mut PricingSubscription) {
    let subscriptions = SubscriptionList::new()
        .add_bars(&subscription.symbol)
        .add_quotes(&subscription.symbol)
        .add_trades(&subscription.symbol);
    data_client.subscribe(subscriptions);
}

pub async fn unsubscribe(
    connection: &mut MultiplexedConnection,
    sub: Subscription,
    data_client: &mut PricingSubscription,
) -> Result<(), SubscriptionError> {
    if check_subscription_exists(connection, &sub).await {
        let _: () = connection
            .del(format!("subscriptions:{:?}:{}", &sub.provider, &sub.symbol))
            .await
            .unwrap();
        let subscriptions = SubscriptionList::new()
            .add_bars(&sub.symbol)
            .add_quotes(&sub.symbol)
            .add_trades(&sub.symbol);
        data_client.unsubscribe(subscriptions);
        Ok(())
    } else {
        Err(SubscriptionError::SubscriptionDoesNotExist)
    }
}
