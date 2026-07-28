//! Broadcast (or receive) server's mouse position.

mod client;
mod server;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use clap::Parser;

    #[derive(Debug, Parser)]
    struct Args {
        /// Run as server (as opposed to client).
        #[arg(short, long)]
        server: bool,

        /// Broadcast address.
        #[arg(short, long, default_value = "192.168.0.255:1234")]
        url: String,
    }

    let args = Args::parse();

    if args.server {
        server::run(&args.url).await?;
    } else {
        client::run().await?;
    }

    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn main() {
    panic!("not supported on wasm");
}
