use std::error::Error;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::{select, time};

mod node;

// wire protocol
// first byte is message type
// 1 = Ping
// 2 = Ack
// 3 = PingReq

fn decode(buf: &[u8]) -> Result<node::Message, Box<dyn Error>> {
    let first = match buf.first() {
        Some(byte) => byte,
        None => return Err("empty packet".into()),
    };
    match first {
        1 => return Ok(node::Message::Ping(1u64)), // TODO
        2 => return Ok(node::Message::Ack(1u64)),
        // 3 => { return node::Message::PingReq(1u64) }  TODO
        _ => return Err("unknown message type".into()),
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let t0 = Instant::now();
    let mut node = node::Node::new(t0, 42, vec![]);

    let socket = UdpSocket::bind("0.0.0.0:8080").await?;
    const UDP_BUFFER_SIZE: usize = 1400;
    let mut buf = [0u8; UDP_BUFFER_SIZE];

    let mut interval = time::interval(Duration::from_millis(100));
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip); // TODO think through how protocol period handles this
    loop {
        select! {
            _ = interval.tick() => { node.handle_time(Instant::now()); }
            res = socket.recv_from(&mut buf) => {
                let (len, addr) = res?;
                println!("Received {} bytes from {}: {:?}", len, addr, &buf[..len]);

                let message = match decode(&buf[..len]) {
                    Ok(m) => m,
                    Err(err) => {eprintln!("error decoding message: {}", err); continue},
                };

                let sender = node::Peer{address: addr};
                let req = node::Payload{peer: sender, message: message};
                let resp = node.handle_message(req);
            }
        }
    }
}
