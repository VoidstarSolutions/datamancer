//! The single-writer socket task: drains the outbound frame channel and writes
//! each frame as a WebSocket text message. One writer per connection means
//! event frames and control replies (both enqueued as strings) never interleave
//! mid-frame and their order is deterministic.

use futures::{Sink, SinkExt as _};
use tokio::sync::mpsc::Receiver;
use tokio_tungstenite::tungstenite::Message;

/// Drain `rx`, sending each string as `Message::Text` on `write`, until the
/// channel closes (all senders dropped) or a socket send fails. Generic over the
/// sink so it is unit-testable without a real socket.
pub async fn run_writer<S>(mut rx: Receiver<String>, mut write: S)
where
    S: Sink<Message> + Unpin,
{
    while let Some(text) = rx.recv().await {
        if write.send(Message::Text(text.into())).await.is_err() {
            break;
        }
    }
    let _ = write.close().await;
}

#[cfg(test)]
mod tests {
    use super::run_writer;
    use futures::StreamExt as _;
    use tokio_tungstenite::tungstenite::Message;

    #[tokio::test]
    async fn writer_wraps_strings_as_text_and_stops_on_close() {
        // A futures mpsc as the "sink" side; collect what the writer sends.
        let (sink_tx, sink_rx) = futures::channel::mpsc::unbounded::<Message>();
        let (tx, rx) = tokio::sync::mpsc::channel::<String>(4);

        tx.send("hello".to_string()).await.unwrap();
        tx.send("world".to_string()).await.unwrap();
        drop(tx); // closes the channel -> writer returns

        run_writer(rx, sink_tx).await;

        let got: Vec<Message> = sink_rx.collect().await;
        assert_eq!(got, vec![
            Message::Text("hello".to_string().into()),
            Message::Text("world".to_string().into()),
        ]);
    }
}
