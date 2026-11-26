use postcard::{from_bytes, to_extend};
use std::error::Error;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::{select, time};

// wire protocol
// first byte is protocol version
// the rest of the message is serialized by Postcard
fn encode(message: &crate::node::Message) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut bytes = vec![1u8]; // protocol version 1
    bytes = to_extend(&message, bytes)?;
    Ok(bytes)
}

fn decode(buf: &[u8]) -> Result<crate::node::Message, Box<dyn Error>> {
    let protocol_version = match buf.first() {
        Some(byte) => *byte,
        None => return Err("empty packet".into()),
    };
    // for now we just check that it is `1` since there's only this version
    if protocol_version != 1 {
        return Err("unrecognized protocol version".into());
    }
    let message: crate::node::Message = from_bytes(&buf[1..])?;
    Ok(message)
}

pub async fn run(mut node: crate::node::Node, socket: UdpSocket) -> std::io::Result<()> {
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
                let payload = crate::node::Payload{
                    peer: crate::node::Peer{address: addr},
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
