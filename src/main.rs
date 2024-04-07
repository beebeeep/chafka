use clap::Parser;


use ingester::Ingester;

use settings::Settings;

use tokio::{
    task::{JoinSet},
};

pub mod decoder;
pub mod ingester;
pub mod settings;

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
            let mut ingester =
                Ingester::new(cfg).unwrap_or_else(|_| panic!("failed to create ingester {name}"));
            ingester.start().await;
        });
    }
    while let Some(_) = ingesters.join_next().await {}
}
