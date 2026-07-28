//! Simple chat application using IPC.

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

        /// IPC endpoint path.
        #[arg(short, long, default_value = "/tmp/chat.ipc")]
        path: String,
    }

    let Args { server, path } = Args::parse();

    if server {
        server::run(&path).await?;
    } else {
        client::run(&path).await?;
    }

    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn main() {
    panic!("not supported on wasm");
}
