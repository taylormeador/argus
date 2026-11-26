use std::time::{Duration, Instant};
use rand::RngExt;

type ProtocolSequence = u64;

#[derive(Debug, PartialEq)]
enum Message {
    Ping(ProtocolSequence),
    Ack(ProtocolSequence),
}

#[derive(Clone, PartialEq)]
struct Peer {
    address: String
}

// TODO consider breaking fields into ProbeState struct
struct Node {
    protocol_period: Duration,
    protocol_start: Instant,
    protocol_sequence: u64,
    protocol_peer: Option<usize>, // the peer that we targeted this period. index of `members`
    protocol_ack: bool, // tracks whether the peer has ack'd our ping this period
    members: Vec<Peer>,  // piggyback in equal amounts from each list of members


    // members_failed_recently: Vec<Peer>,
    // recent_membership_updates: Vec<u64>,
    // piggyback_count: Vec<u64>
}

impl Node {
    fn new(protocol_start: Instant) -> Self {
        let protocol_period = Duration::from_secs(1);  // TODO figure out a good protocol period
        let peer = Peer{ address: "10.0.0.2".to_string() };  // TODO let it discover peers
        let members = vec![peer];
        Node{ protocol_period, protocol_start, protocol_sequence: 0, protocol_peer: None, protocol_ack: false, members }
    }

    pub fn handle_time(&mut self, i: Instant) -> (Option<String>, Option<Message>) {
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
            return self.send_ping()
        }
        return (None, None);
    }

    pub fn handle_message(&mut self, sender: Peer, message: Message) -> (Option<String>, Option<Message>) {
        match message {
            Message::Ping(seq) => {
                if !self.members.contains(&sender) {
                    self.members.push(sender.clone());
                }
                return (Some(sender.address), Some(self.send_ack(seq)));
            }
            Message::Ack(seq) => {
                // if the ack is for this period, update state. otherwise ignore
                if seq == self.protocol_sequence {
                    self.protocol_ack = true;
                }
                (None, None) 
            }
        }
    }

    fn send_ping(&mut self) -> (Option<String>, Option<Message>) {
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
            return (None, None);
        }
        let idx = rand::rng().random_range(0..self.members.len());
        self.protocol_peer = Some(idx);  // TODO seed RNG in Node
        let address = self.members[idx].address.clone();
        (Some(address), Some(Message::Ping(self.protocol_sequence)))
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


fn main() {
    let t0 = Instant::now();
    let mut node = Node::new(t0);

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_ping_before_protocol_period() {
        let t0 = Instant::now();
        let mut node = Node::new(t0);

        let resp = node.handle_time(t0 + Duration::from_millis(999));

        assert_eq!(resp, (None, None));
    }

    #[test]
    fn pings_seed_after_protocol_period() {
        let t0 = Instant::now();
        let mut node = Node::new(t0);

        let resp = node.handle_time(t0 + Duration::from_millis(1001));

        assert_eq!(resp, (Some("10.0.0.2".to_string()), Some(Message::Ping(1))));
    }

    #[test]
    fn ack_when_pinged() {
        let t0 = Instant::now();
        let mut node = Node::new(t0);
        let sender = Peer { address: "10.0.0.3".to_string() };

        let resp = node.handle_message(sender, Message::Ping(7));

        assert_eq!(resp, (Some("10.0.0.3".to_string()), Some(Message::Ack(7))));
    }

    #[test]
    fn sole_member_removed_after_ping_not_acked() {
        let t0 = Instant::now();
        let mut node = Node::new(t0);

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
        let mut node = Node::new(t0);
        let sender = Peer { address: "10.0.0.3".to_string() };

        // seed the ping
        let _ = node.handle_time(t0 + Duration::from_millis(1001));

        // ack the ping
        let _ = node.handle_message(sender, Message::Ack(1));
    
        // seed a new ping
        let _ = node.handle_time(t0 + Duration::from_millis(2001));

        // let the period expire without ack, member is removed
        let _ = node.handle_time(t0 + Duration::from_millis(3001));

        assert_eq!(node.members.len(), 0);
    }

    #[test]
    fn add_peer_on_ping() {
        let t0 = Instant::now();
        let mut node = Node::new(t0);
        let sender = Peer { address: "10.0.0.3".to_string() };

        let _ = node.handle_message(sender, Message::Ping(7));
        
        assert_eq!(node.members.len(), 2);
    }

    #[test]
    fn dont_duplicate_peer_on_ping() {
        let t0 = Instant::now();
        let mut node = Node::new(t0);
        let sender = Peer { address: "10.0.0.3".to_string() };
        let sender2 = sender.clone();

        let _ = node.handle_message(sender, Message::Ping(7));
        let _ = node.handle_message(sender2, Message::Ping(8));
        
        assert_eq!(node.members.len(), 2);
    }
}
