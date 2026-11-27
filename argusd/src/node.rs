use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::IteratorRandom;
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};
use tracing::{debug, error, info, instrument, warn};

const PROTOCOL_PERIOD: Duration = Duration::from_millis(1000);
const PING_TIMEOUT: Duration = Duration::from_millis(75);
const PROBING_SUBGROUP_SIZE: usize = 3;

type ProtocolSequence = u64;

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
pub struct PingReq {
    seq: ProtocolSequence,
    target: Peer,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
pub enum Message {
    Ping(ProtocolSequence),
    Ack(ProtocolSequence),
    PingReq(PingReq),
}

#[derive(Debug, PartialEq)]
pub struct Outgoing {
    pub to: Peer,
    pub message: Message,
}

#[derive(Debug, PartialEq)]
pub struct Incoming {
    pub from: Peer,
    pub message: Message,
}

// TODO constructor?
#[derive(Serialize, Deserialize, Copy, Clone, PartialEq, Debug)]
pub struct Peer {
    pub address: SocketAddr,
}

#[derive(Copy, Clone, Debug)]
struct ProtocolPing {
    peer: Peer,
    ping_sequence: u64,             // this node's ping_sequence
    req: Option<Peer>,              // If no peer requested the ping, it was us
    req_ping_sequence: Option<u64>, // the ping_sequence of the node who requested the ping
    ack: bool,
}

// TODO consider breaking fields into ProbeState struct
pub struct Node {
    protocol_period: Duration,
    protocol_start: Instant,
    ping_sequence: u64, // incremented each time a ping is sent, direct or on behalf of another peer
    protocol_pings: Vec<ProtocolPing>, // the peers that we pinged this period
    members: Vec<Peer>, // piggyback in equal amounts from each list of members
    rng: StdRng,
    // members_failed_recently: Vec<Peer>,
    // recent_membership_updates: Vec<u64>,
    // piggyback_count: Vec<u64>

    // TODO Each element is piggybacked at most lamda log n times. If the
    // size of this buffer is larger than the maximum number of ele-
    // ments that can be piggybacked on a single ping message (or
    // ack), elements that have been gossiped fewer times are pre-
    // ferred.
}

impl Node {
    pub fn new(now: Instant, seed: u64, peers: Vec<Peer>) -> Self {
        let rng = StdRng::seed_from_u64(seed);
        Node {
            protocol_period: PROTOCOL_PERIOD,
            protocol_start: now,
            ping_sequence: 0,
            protocol_pings: Vec::new(),
            members: peers,
            rng,
        }
    }

    pub fn members(&self) -> &[Peer] {
        &self.members
    }

    pub fn handle_time(&mut self, i: Instant) -> Vec<Outgoing> {
        let elapsed = i - self.protocol_start;
        // If the protocol has ended, handle unacked ping and start the new protocol
        if elapsed > self.protocol_period {
            debug!("protocol_period ended");
            // The first ping is always our direct probe
            if let Some(ping) = self.protocol_pings.first() {
                if !ping.ack {
                    for i in 0..self.members.len() {
                        if self.members[i].address == ping.peer.address {
                            info!("member suspected: {}", ping.peer.address);
                            self.members.swap_remove(i);
                            break;
                        }
                    }
                }
            }

            // Start new protocol
            self.protocol_start = self.protocol_start + self.protocol_period; // TODO what if we are really slow/behind?
            self.ping_sequence = self.ping_sequence + 1;
            self.protocol_pings = Vec::new();

            // Ping a random peer, if one exists
            if let Some(random_peer) = self.get_random_peer() {
                let random_ping = ProtocolPing {
                    peer: random_peer,
                    ping_sequence: self.ping_sequence,
                    req: None,
                    req_ping_sequence: None,
                    ack: false,
                };
                self.protocol_pings.push(random_ping);

                let payload = Outgoing {
                    to: random_peer,
                    message: Message::Ping(self.ping_sequence),
                };
                return vec![payload];
            }
        }
        // If elapsed is greater than ping timeout, we send PingReq to up to k members if we haven't already.
        // self.protocol_pings.len() should be either `1` or `1 + min(PROBING_SUBGROUP_SIZE, self.members.len())`
        // This is because the first item is always populated immediately when a new protocol starts.
        // Later in the protocol, we would check here that it's `1` and then add k more pings.
        if elapsed > PING_TIMEOUT && self.protocol_pings.len() == 1 && !self.protocol_pings[0].ack {
            debug!("ping timed out for {:?}", self.protocol_pings[0].peer);
            let k = std::cmp::min(PROBING_SUBGROUP_SIZE, self.members.len());
            // TODO dont target the suspected node
            let subgroup = self.members.iter().sample(&mut self.rng, k);
            let mut pings = Vec::new();
            for peer in subgroup {
                self.ping_sequence = self.ping_sequence + 1;
                let ping = ProtocolPing {
                    peer: peer.clone(),
                    ping_sequence: self.ping_sequence,
                    req: None,
                    req_ping_sequence: None,
                    ack: false,
                };
                self.protocol_pings.push(ping);

                let ping_req = PingReq {
                    target: self.protocol_pings[0].peer,
                    seq: self.protocol_pings[0].ping_sequence,
                };
                let payload = Outgoing {
                    to: peer.clone(),
                    message: Message::PingReq(ping_req),
                };
                pings.push(payload);
            }
            return pings;
        }
        return Vec::new();
    }

    pub fn handle_message(&mut self, incoming: Incoming) -> Option<Outgoing> {
        match incoming.message {
            Message::Ping(seq) => {
                // add peer as member if not already there
                if !self.members.contains(&incoming.from) {
                    self.members.push(incoming.from.clone());
                }

                return Some(Outgoing {
                    to: incoming.from,
                    message: Message::Ack(seq),
                });
            }
            Message::Ack(seq) => {
                // The Ack could be from a node we probed either directly or indirectly.
                // It could also be from a sequence other than the one it was created in.
                // We need to find the ProtocolPing info associated with this sequence since we have a unique sequence for every ping we have sent.
                // If it was a direct probe, we update our state.
                // If it was indirect, we forward the Ack to the requestor.
                for ping in &mut self.protocol_pings {
                    if ping.ping_sequence == seq {
                        // This is a valid Ack
                        if let Some(requestor) = ping.req {
                            // This ping was requested by a peer, forward it to them
                            // ping.req_ping_sequence should never be None here since we always store the requestor and their sequence in the ping
                            if let Some(req_ping_sequence) = ping.req_ping_sequence {
                                let payload = Outgoing {
                                    to: requestor,
                                    message: Message::Ack(req_ping_sequence),
                                };
                                return Some(payload);
                            }
                        } else {
                            // This ping was sent by us, update our internal state
                            ping.ack = true;
                        }
                    }
                }
                None
            }
            // TODO we are never actually sending this
            Message::PingReq(req) => {
                // add peer as member if not already there
                if !self.members.contains(&incoming.from) {
                    self.members.push(incoming.from.clone());
                }

                self.ping_sequence = self.ping_sequence + 1;
                let ping = ProtocolPing {
                    peer: req.target,
                    ping_sequence: self.ping_sequence,
                    req: Some(incoming.from),
                    req_ping_sequence: Some(req.seq),
                    ack: false,
                };
                self.protocol_pings.push(ping);

                Some(Outgoing {
                    to: req.target,
                    message: Message::Ping(self.ping_sequence),
                })
            }
        }
    }

    // return a random peer, if any exist
    fn get_random_peer(&mut self) -> Option<Peer> {
        if self.members.len() == 0 {
            debug!("cannot randomly select peer, none exist");
            return None;
        }
        let idx = self.rng.random_range(0..self.members.len());
        Some(self.members[idx])
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

        let resp_exp = Vec::new();
        let resp = node.handle_time(t0 + Duration::from_millis(999));

        assert_eq!(resp, resp_exp);
    }

    #[test]
    fn pings_seed_after_protocol_period() {
        let t0 = Instant::now();
        let mut node = test_node(t0);

        let resp_exp = vec![Outgoing {
            to: NODE_B,
            message: Message::Ping(1),
        }];
        let resp = node.handle_time(t0 + Duration::from_millis(1001));

        assert_eq!(resp, resp_exp);
    }

    #[test]
    fn ack_when_pinged() {
        let t0 = Instant::now();
        let mut node = test_node(t0);

        let req = Incoming {
            from: NODE_B,
            message: Message::Ping(7),
        };
        let payload_exp = Outgoing {
            to: NODE_B,
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
        assert!(node.protocol_pings.is_empty());
    }

    #[test]
    fn member_removed_after_previous_member_ack() {
        let t0 = Instant::now();
        let mut node = test_node(t0);

        // seed the ping
        let _ = node.handle_time(t0 + Duration::from_millis(1001));

        // B acks the ping
        let payload = Incoming {
            from: NODE_B,
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

        let payload = Incoming {
            from: NODE_C,
            message: Message::Ping(7),
        };
        let _ = node.handle_message(payload);

        assert_eq!(node.members, vec![NODE_B, NODE_C]);
    }

    #[test]
    fn dont_duplicate_peer_on_ping() {
        let t0 = Instant::now();
        let mut node = test_node(t0);

        let payload = Incoming {
            from: NODE_C,
            message: Message::Ping(7),
        };
        let _ = node.handle_message(payload);

        let payload = Incoming {
            from: NODE_C,
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
        let req = Incoming {
            from: NODE_A,
            message: payload[0].message,
        };

        let _ = node_b.handle_message(req);

        // B should have A in the peer list
        assert_eq!(node_b.members, vec![NODE_A]);
    }

    #[test]
    fn ping_req_begets_ping() {
        let t0 = Instant::now();
        let mut node_a = test_node(t0);

        // B sends a PingReq to A targeting C
        let ping_req = PingReq {
            seq: 50,
            target: NODE_C,
        };
        let req = Incoming {
            from: NODE_B,
            message: Message::PingReq(ping_req),
        };

        let resp_exp = Some(Outgoing {
            to: NODE_C,
            message: Message::Ping(1),
        });

        let resp = node_a.handle_message(req);

        assert_eq!(resp, resp_exp)
    }

    #[test]
    fn node_forwards_ack_for_ping_req() {
        let t0 = Instant::now();
        let mut node_a = test_node(t0);

        // B sends a PingReq to A targeting C
        let ping_req = PingReq {
            seq: 50,
            target: NODE_C,
        };
        let req = Incoming {
            from: NODE_B,
            message: Message::PingReq(ping_req),
        };

        // node A should remember this indirect ping
        let _ = node_a.handle_message(req);

        // C sends Ack to A with it's sequence, A should forward to B with B's sequence
        let req = Incoming {
            from: NODE_C,
            message: Message::Ack(1),
        };
        let resp_exp = Some(Outgoing {
            to: NODE_B,
            message: Message::Ack(50),
        });
        let resp = node_a.handle_message(req);

        assert_eq!(resp, resp_exp)
    }
}
