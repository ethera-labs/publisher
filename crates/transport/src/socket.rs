//! UDP socket construction with OS-level buffer tuning for QUIC.

use std::io;
use std::net::SocketAddr;

use socket2::{Domain, Protocol, Socket, Type};

const SOCKET_BUFFER_SIZE: usize = 7 * 1024 * 1024;

pub(crate) fn build_udp_socket(addr: SocketAddr) -> io::Result<std::net::UdpSocket> {
    let domain = if addr.is_ipv6() {
        Domain::IPV6
    } else {
        Domain::IPV4
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    let _ = socket.set_recv_buffer_size(SOCKET_BUFFER_SIZE);
    let _ = socket.set_send_buffer_size(SOCKET_BUFFER_SIZE);
    socket.bind(&addr.into())?;
    Ok(socket.into())
}
