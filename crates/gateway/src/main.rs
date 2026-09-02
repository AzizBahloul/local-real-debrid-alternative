#[tokio::main]
async fn main() -> anyhow::Result<()> {
    streaming_gateway::run().await
}
