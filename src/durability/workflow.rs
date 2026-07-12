use crate::protocol::Message;
use log::info;

#[resonate_sdk::function]
pub async fn record_message(_ctx: &Context, message: Message) -> resonate_sdk::error::Result<()> {
    info!("Recording message {} to durable store", message.id);
    Ok(())
}

#[resonate_sdk::function]
pub async fn broadcast_message(_ctx: &Context, message: Message) -> resonate_sdk::error::Result<()> {
    info!("Broadcasting message {} to peers", message.id);
    Ok(())
}

/// A durable workflow that processes a message for a specific topic.
#[resonate_sdk::function]
pub async fn process_topic_message(ctx: &Context, message: Message) -> resonate_sdk::error::Result<bool> {
    info!("Processing durable message {} for topic '{}'", message.id, message.topic);

    // Call registered functions durably
    let _ = ctx.run(record_message, message.clone()).await?;
    let _ = ctx.run(broadcast_message, message).await?;

    Ok(true)
}
// governed-by: ADR-0001
