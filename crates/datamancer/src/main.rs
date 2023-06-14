use datamancer::Datamancer;

#[tokio::main]
async fn main() {
    let datamancer = Datamancer::initialize_datamancer().await;
    datamancer.run().await;
}
