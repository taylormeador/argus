use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

type ProtocolSequence = u64;

#[derive(Debug, PartialEq)]
pub enum Message {
    Ping(ProtocolSequence),
    Ack(ProtocolSequence),
}

#[derive(Debug, PartialEq)]
pub struct Payload {
    pub peer: Peer,
    pub message: Message,
}

// TODO constructor?
#[derive(Clone, PartialEq, Debug)]
pub struct Peer {
    pub address: SocketAddr,
}

// TODO consider breaking fields into ProbeState struct
pub struct Node {
    protocol_period: Duration,
    protocol_start: Instant,
    protocol_sequence: u64,
    protocol_peer: Option<usize>, // the peer that we targeted this period. index of `members`
    protocol_ack: bool,           // tracks whether the peer has ack'd our ping this period
    members: Vec<Peer>,           // piggyback in equal amounts from each list of members
    rng: StdRng,
    // members_failed_recently: Vec<Peer>,
    // recent_membership_updates: Vec<u64>,
    // piggyback_count: Vec<u64>
}

impl Node {
    pub fn new(now: Instant, seed: u64, peers: Vec<Peer>) -> Self {
        let protocol_period = Duration::from_secs(1); // TODO figure out a good protocol period
        let rng = StdRng::seed_from_u64(seed);
        Node {
            protocol_period,
            protocol_start: now,
            protocol_sequence: 0,
            protocol_peer: None,
            protocol_ack: false,
            members: peers,
            rng,
        }
    }

    pub fn handle_time(&mut self, i: Instant) -> Option<Payload> {
        let elapsed = i - self.protocol_start;
        if elapsed > self.protocol_period {
            if let Some(idx) = self.protocol_peer {
                if !self.protocol_ack {
                    self.members.swap_remove(idx);
                }
            }

            // Start new period
            self.protocol_start = self.protocol_start + self.protocol_period;
            self.protocol_sequence = self.protocol_sequence + 1;
            self.protocol_peer = None;
            self.protocol_ack = false;
            return self.send_ping();
        }
        return None;
    }

    pub fn handle_message(&mut self, payload: Payload) -> Option<Payload> {
        match payload.message {
            Message::Ping(seq) => {
                if !self.members.contains(&payload.peer) {
                    self.members.push(payload.peer.clone());
                }
                return Some(Payload {
                    peer: payload.peer,
                    message: self.send_ack(seq),
                });
            }
            Message::Ack(seq) => {
                // if the ack is for this period, update state. otherwise ignore
                if seq == self.protocol_sequence {
                    self.protocol_ack = true;
                }
                None
            }
        }
    }

    fn send_ping(&mut self) -> Option<Payload> {
        // pick a random peer and ping it
        // wait for the response
        // if it doesnt come back before the timeout (less than timeout period), try to indirect probe
        // if it does come back, compare with this node's unique sequence number in the protocol period

        // Each element is piggybacked at most lamda log n times. If the
        // size of this buffer is larger than the maximum number of ele-
        // ments that can be piggybacked on a single ping message (or
        // ack), elements that have been gossiped fewer times are pre-
        // ferred.
        if self.members.len() == 0 {
            return None;
        }
        let idx = self.rng.random_range(0..self.members.len());
        self.protocol_peer = Some(idx);
        Some(Payload {
            peer: self.members[idx].clone(),
            message: Message::Ping(self.protocol_sequence),
        })
    }

    fn indirect_probe(peer: Peer) {
        // select k members at random and send a ping-req request
        // at the end of the period, see if we have received any acks (direct or indirect)
        // if not, declare peer failed (later suspect) and hand off to Dissemination Component
    }

    fn send_ping_req(target: Peer) {
        // send ping-req to random Peer asking to send ping to `target`
    }

    fn send_ack(&self, seq: ProtocolSequence) -> Message {
        // respond to ping() messages we receive
        // include piggyback
        Message::Ack(seq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NODE_A: Peer = Peer {
        address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 8080),
    };
    const NODE_B: Peer = Peer {
        address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 8080),
    };
    const NODE_C: Peer = Peer {
        address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3)), 8080),
    };

    /// Node A, whose only peer is node B.
    fn test_node(now: Instant) -> Node {
        Node::new(now, 42, vec![NODE_B])
    }

    #[test]
    fn no_ping_before_protocol_period() {
        let t0 = Instant::now();
        let mut node = test_node(t0);

        let resp_exp = None;
        let resp = node.handle_time(t0 + Duration::from_millis(999));

        assert_eq!(resp, resp_exp);
    }

    #[test]
    fn pings_seed_after_protocol_period() {
        let t0 = Instant::now();
        let mut node = test_node(t0);

        let resp_exp = Some(Payload {
            peer: NODE_B,
            message: Message::Ping(1),
        });
        let resp = node.handle_time(t0 + Duration::from_millis(1001));

        assert_eq!(resp, resp_exp);
    }

    #[test]
    fn ack_when_pinged() {
        let t0 = Instant::now();
        let mut node = test_node(t0);

        let req = Payload {
            peer: NODE_B,
            message: Message::Ping(7),
        };
        let payload_exp = Payload {
            peer: NODE_B,
            message: Message::Ack(7),
        };
        let resp = node.handle_message(req);

        assert_eq!(resp, Some(payload_exp));
    }

    #[test]
    fn sole_member_removed_after_ping_not_acked() {
        let t0 = Instant::now();
        let mut node = test_node(t0);

        // seed the ping
        let _ = node.handle_time(t0 + Duration::from_millis(1001));

        // let the period expire without ack
        let _ = node.handle_time(t0 + Duration::from_millis(2001));

        assert_eq!(node.members.len(), 0);
        assert_eq!(node.protocol_peer, None);
    }

    #[test]
    fn member_removed_after_previous_member_ack() {
        let t0 = Instant::now();
        let mut node = test_node(t0);

        // seed the ping
        let _ = node.handle_time(t0 + Duration::from_millis(1001));

        // B acks the ping
        let payload = Payload {
            peer: NODE_B,
            message: Message::Ack(1),
        };
        let _ = node.handle_message(payload);

        // seed a new ping
        let _ = node.handle_time(t0 + Duration::from_millis(2001));

        // let the period expire without ack, member is removed
        let _ = node.handle_time(t0 + Duration::from_millis(3001));

        assert_eq!(node.members.len(), 0);
    }

    #[test]
    fn add_peer_on_ping() {
        let t0 = Instant::now();
        let mut node = test_node(t0);

        let payload = Payload {
            peer: NODE_C,
            message: Message::Ping(7),
        };
        let _ = node.handle_message(payload);

        assert_eq!(node.members, vec![NODE_B, NODE_C]);
    }

    #[test]
    fn dont_duplicate_peer_on_ping() {
        let t0 = Instant::now();
        let mut node = test_node(t0);

        let payload = Payload {
            peer: NODE_C,
            message: Message::Ping(7),
        };
        let _ = node.handle_message(payload);

        let payload = Payload {
            peer: NODE_C,
            message: Message::Ping(8),
        };
        let _ = node.handle_message(payload);

        assert_eq!(node.members, vec![NODE_B, NODE_C]);
    }

    #[test]
    fn two_nodes_find_each_other() {
        // A knows about B but B should find out about A
        let t0 = Instant::now();
        let mut node_a = test_node(t0);
        let mut node_b = Node::new(t0, 42, Vec::new());

        // let A ping B
        let payload = node_a.handle_time(t0 + Duration::from_millis(1001));
        let a_resp = match payload {
            Some(message) => message,
            None => panic!(),
        };
        let req = Payload {
            peer: NODE_A,
            message: a_resp.message,
        };

        let _ = node_b.handle_message(req);

        // B should have A in the peer list
        assert_eq!(node_b.members, vec![NODE_A]);
    }
}
