use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Instant;
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

mod driver;
mod node;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Print tracing events to stderr, filtered by RUST_LOG (e.g. RUST_LOG=debug).
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args: Vec<String> = env::args().collect();

    let port: u16 = args
        .get(1)
        .expect("missing port arg")
        .parse()
        .expect("port must be an integer");

    let t0 = Instant::now(); // TODO will this cause issues since the loop hasnt started?

    // give node A the address of node B
    let mut peers = Vec::new();
    if port == 8080 {
        let peer = node::Peer {
            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 10, 110)), 8081),
        };
        peers.push(peer);
    }
    let node = node::Node::new(t0, 42, peers);

    let addr = format!("0.0.0.0:{port}");
    let socket = UdpSocket::bind(addr).await?;
    let token = CancellationToken::new();
    let _ = driver::run(node, socket, token).await?;
    Ok(())
}
