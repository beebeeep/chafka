use chafka::{ingester::Ingester, settings::Settings};
use clap::Parser;
use tokio::task::JoinSet;

#[doc(hidden)]
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    config: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let settings = Settings::new(&args.config).expect("cannot load config");
    let mut ingesters = JoinSet::new();
    for (name, cfg) in settings.ingesters {
        ingesters.spawn(async move {
            let mut ingester = Ingester::new(cfg)
                .await
                .unwrap_or_else(|e| panic!("failed to create ingester {name}: {:#}", e));
            ingester.start().await;
        });
    }
    while let Some(_) = ingesters.join_next().await {}
}
