use postcard::{from_bytes, to_extend};
use std::error::Error;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::{select, time};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

// wire protocol
// first byte is protocol version
// the rest of the message is serialized by Postcard
fn encode(message: &crate::node::Message) -> Result<Vec<u8>, Box<dyn Error>> {
    debug!("encoding: {:?}", message);
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
    debug!("decoded: {:?}", message);
    Ok(message)
}

pub async fn run(
    mut node: crate::node::Node,
    socket: UdpSocket,
    token: CancellationToken,
) -> std::io::Result<crate::node::Node> {
    const UDP_BUFFER_SIZE: usize = 1400;
    let mut buf = [0u8; UDP_BUFFER_SIZE];

    let mut interval = time::interval(Duration::from_millis(100));
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip); // TODO think through how protocol period handles this
    loop {
        select! {
            _ = token.cancelled() => {
                return Ok(node)
            }
            _ = interval.tick() => {
                let responses = node.handle_time(Instant::now());
                for resp in responses {
                    let bytes = match encode(&resp.message) {
                        Ok(b) => b,
                        Err(err) => {error!("error encoding message: {}", err); continue},
                    };
                    info!("sending {:?}", resp);
                    match socket.send_to(&bytes, resp.to.address).await {
                        Ok(_) => {}
                        Err(err) => error!("error sending response: {}", err)
                    }
                };
            }
            res = socket.recv_from(&mut buf) => {
                // decode the request into Incoming
                let (len, addr) = res?;
                let req = match decode(&buf[..len]) {
                    Ok(m) => m,
                    Err(err) => {error!("error decoding message: {}", err); continue},
                };
                let payload = crate::node::Incoming{
                    from: crate::node::Peer{address: addr},
                    message: req
                };
                info!("received {:?}", payload);

                // deliver request to the Core and relay response
                if let Some(resp) = node.handle_message(payload) {
                    let bytes = match encode(&resp.message) {
                        Ok(b) => b,
                        Err(err) => {error!("error encoding message: {}", err); continue},
                    };
                    info!("sending {:?}", resp);
                    match socket.send_to(&bytes, resp.to.address).await {
                        Ok(_) => {}
                        Err(err) => error!("error sending response: {}", err)
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{Node, Peer};

    #[tokio::test]
    async fn nodes_discover_ping_and_ack() {
        // Port 0 lets the OS pick free ports, read them back to seed the nodes.
        let socket_a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let socket_b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr_a = socket_a.local_addr().unwrap();
        let addr_b = socket_b.local_addr().unwrap();

        // A knows B; B knows nobody and should learn A from A's ping.
        let t0 = Instant::now();
        let node_a = Node::new(t0, 42, vec![Peer { address: addr_b }]);
        let node_b = Node::new(t0, 42, Vec::new());

        let token = CancellationToken::new();
        let handle_a = tokio::spawn(run(node_a, socket_a, token.clone()));
        let handle_b = tokio::spawn(run(node_b, socket_b, token.clone()));

        // Long enough for A's second period to start, which is when an
        // unacked B would have been removed.
        time::sleep(Duration::from_millis(2500)).await;
        token.cancel();

        // Outer unwrap: the task didn't panic. Inner: run returned Ok.
        let node_a = handle_a.await.unwrap().unwrap();
        let node_b = handle_b.await.unwrap().unwrap();

        assert_eq!(node_a.members(), &[Peer { address: addr_b }]);
        assert_eq!(node_b.members(), &[Peer { address: addr_a }]);
    }
}
