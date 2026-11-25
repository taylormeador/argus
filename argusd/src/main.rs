use std::time::{Duration, Instant};

type ProtocolSequence = u64;

#[derive(Debug, PartialEq)]
enum Message {
    Ping(ProtocolSequence),
    Ack(ProtocolSequence),
}

struct Peer {
    address: String
}

struct Node {
    protocol_period: Duration,
    protocol_start: Instant,
    protocol_sequence: u64,
    members: Vec<Peer>,  // piggyback in equal amounts from each list of members

    // members_failed_recently: Vec<Peer>,
    // recent_membership_updates: Vec<u64>,
    // piggyback_count: Vec<u64>
}

// the driver will 
impl Node {
    fn new(protocol_start: Instant) -> Self {
        let protocol_period = Duration::from_secs(1);  // TODO figure out a good protocol period
        let peer = Peer{ address: "10.0.0.2".to_string() };
        let members = vec![peer];
        Node{ protocol_period, protocol_start, protocol_sequence: 0, members }
    }

    pub fn handle_time(&mut self, i: Instant) -> (Option<String>, Option<Message>) {
        let elapsed = i - self.protocol_start;
        if elapsed > self.protocol_period {
            // TODO need to check that ping from last period was ack'd.

            // Start new period
            self.protocol_start = i;
            self.protocol_sequence = self.protocol_sequence + 1;
            return self.send_ping()
        }
        return (None, None);
    }

    pub fn handle_message(&mut self, sender: Peer, message: Message) -> (Option<String>, Option<Message>) {
        // TODO determine the message type and call message handler
        match message {
            Message::Ping(seq) => { 
                return (Some(sender.address), Some(self.send_ack(seq)));
            }
            Message::Ack(seq) => {
                // TODO was this ack for the ping in the current period?
                if seq == self.protocol_sequence {
                    // the node is alive
                } else {
                    // this is a stale message... not sure what SWIM is supposed to do here, probably ignore?
                }
                (None, None) 
            }
        }
    }

    fn send_ping(&self) -> (Option<String>, Option<Message>) {
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
        (Some(self.members[0].address.clone()), Some(Message::Ping(self.protocol_sequence)))
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
    let now = Instant::now();
    let mut node = Node::new(now);

    let resp = node.handle_time(now + Duration::from_millis(999));
    println!("after 0.999s: {:?}", resp);
    let resp = node.handle_time(now + Duration::from_millis(1001));
    println!("after 1.001s: {:?}", resp);

    let sender = Peer { address: "10.0.0.3".to_string() };
    let resp = node.handle_message(sender, Message::Ping(0));
    println!("ping response: {:?}", resp);
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
}
