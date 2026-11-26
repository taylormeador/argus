use std::time::Instant;

mod node;

fn main() {
    let t0 = Instant::now();
    let mut node = node::Node::new(t0, 42, vec![]);
}
