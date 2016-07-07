use std::sync::mpsc::Sender;
use std::net::{Ipv4Addr, UdpSocket, SocketAddrV4};
use std::thread::sleep;
use std::time::Duration;


pub enum Event {
    UdpMessage(Vec<u8>),
    TimerFlush,
}

/// statsd
pub fn udp_server(chan: Sender<Event>, port: u16) {
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port);
    let socket = UdpSocket::bind(addr).ok().unwrap();
    let mut buf = [0; 8192];
    loop {
        let (len, _) = match socket.recv_from(&mut buf) {
            Ok(r) => r,
            Err(_) => panic!("Could not read UDP socket."),
        };
        let bytes = Vec::from(&buf[..len]);
        chan.send(Event::UdpMessage(bytes)).unwrap();
    }
}

/// emit flush event into channel on a regular interval
pub fn flush_timer_loop(chan: Sender<Event>, interval: u64) {
    let duration = Duration::new(interval, 0);
    loop {
        sleep(duration);
        chan.send(Event::TimerFlush).unwrap();
    }
}
