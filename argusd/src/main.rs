use tokio::net::UdpSocket as TokioUdpSocket;
use std::net::UdpSocket as StdUdpSocket;

#[tokio::main]
async fn main() {
    let port: u16 = std::env::var("ARGUSD_PORT")
      .unwrap_or_else(|_| "7946".to_string())
      .parse()
      .expect("ARGUSD_PORT must be a valid port number");

    let bind_addr = format!("0.0.0.0:{}", port);
    let std_socket = StdUdpSocket::bind(bind_addr).unwrap();
    std_socket.set_broadcast(true).unwrap();
    std_socket.set_nonblocking(true).unwrap();

    let tokio_socket = TokioUdpSocket::from_std(std_socket).unwrap();

    let message = b"broadcast message";
    let send_addr = format!("255.255.255.255:{}", port);
    tokio_socket.send_to(message, send_addr).await.unwrap();

    println!("listening on {}", tokio_socket.local_addr().unwrap());

    let mut buf = [0u8; 1024];
    loop {
        let (len, addr) = tokio_socket.recv_from(&mut buf).await.unwrap();
        let msg = String::from_utf8_lossy(&buf[..len]);
        println!("received from {addr}: {msg}");

        tokio_socket.send_to(&buf[..len], addr).await.unwrap();
    }
}
