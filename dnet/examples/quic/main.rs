//! Simple chat application over TCP.

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

        /// Server address.
        #[arg(short, long, default_value = "127.0.0.1:8080")]
        address: String,
    }

    let Args { server, address } = Args::parse();

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install CryptoProvider");

    if server {
        server::run(&address).await?;
    } else {
        client::run(&address).await?;
    }

    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn main() {
    panic!("not supported on wasm");
}
