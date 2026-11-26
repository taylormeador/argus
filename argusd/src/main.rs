use postcard::{from_bytes, to_extend};
use std::env;
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::{select, time};

mod node;

// wire protocol
// first byte is protocol version
// the rest of the message is serialized by Postcard
fn encode(message: &node::Message) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut bytes = vec![1u8]; // protocol version 1
    bytes = to_extend(&message, bytes)?;
    Ok(bytes)
}

fn decode(buf: &[u8]) -> Result<node::Message, Box<dyn Error>> {
    let protocol_version = match buf.first() {
        Some(byte) => *byte,
        None => return Err("empty packet".into()),
    };
    // for now we just check that it is `1` since there's only this version
    if protocol_version != 1 {
        return Err("unrecognized protocol version".into());
    }
    let message: node::Message = from_bytes(&buf[1..])?;
    Ok(message)
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
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
    let mut node = node::Node::new(t0, 42, peers);

    let addr = format!("0.0.0.0:{port}");
    let socket = UdpSocket::bind(addr).await?;
    const UDP_BUFFER_SIZE: usize = 1400;
    let mut buf = [0u8; UDP_BUFFER_SIZE];

    let mut interval = time::interval(Duration::from_millis(100));
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip); // TODO think through how protocol period handles this
    loop {
        select! {
            _ = interval.tick() => {
                if let Some(resp) = node.handle_time(Instant::now()) {
                    let bytes = match encode(&resp.message) {
                        Ok(b) => b,
                        Err(err) => {eprintln!("error encoding message: {}", err); continue},
                    };
                    println!("sending to {}", resp.peer.address);
                    match socket.send_to(&bytes, resp.peer.address).await {
                        Ok(_) => {}
                        Err(err) => eprintln!("error sending response: {}", err)
                    }
                }

            }
            res = socket.recv_from(&mut buf) => {
                // decode the request into Payload
                let (len, addr) = res?;
                println!("Received {} bytes from {}: {:?}", len, addr, &buf[..len]);

                let req = match decode(&buf[..len]) {
                    Ok(m) => m,
                    Err(err) => {eprintln!("error decoding message: {}", err); continue},
                };
                let payload = node::Payload{
                    peer: node::Peer{address: addr},
                    message: req
                };

                // deliver request to the Core and relay response
                if let Some(resp) = node.handle_message(payload) {
                    let bytes = match encode(&resp.message) {
                        Ok(b) => b,
                        Err(err) => {eprintln!("error encoding message: {}", err); continue},
                    };
                    match socket.send_to(&bytes, resp.peer.address).await {
                        Ok(_) => {}
                        Err(err) => eprintln!("error sending response: {}", err)
                    }
                }
            }
        }
    }
}
