use albertdb::{config::Config, frontend::http};
use clap::Parser;

// TODO move everything in this into frontend in preparation to support
// multiple frontend implementations

#[derive(clap::Parser)]
struct CliArgs {
    #[clap(long, value_parser)]
    config: String,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = CliArgs::parse();
    let config = Config::from_file(&args.config);
    http::start(config).await
}
