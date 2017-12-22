use metric;
use mio;
use util;
use constants;
use source::Source;
use std;
use std::net::ToSocketAddrs;
use std::sync;
use thread;
use thread::Stoppable;

/// Configured for the `metric::Telemetry` source.
#[derive(Debug, Deserialize, Clone)]
pub struct TCPConfig {
    /// The unique name of the source in the routing topology.
    pub config_path: Option<String>,
    /// The host that the source will listen on. May be an IP address or a DNS
    /// hostname.
    pub host: String,
    /// The port that the source will listen on.
    pub port: u16,
    /// The tags that the source will apply to all Telemetry it creates.
    pub tags: metric::TagMap,
    /// The forwards that the source will send all its Telemetry.
    pub forwards: Vec<String>,

}

impl Default for TCPConfig {
    fn default() -> TCPConfig {
        TCPConfig {
            host: "localhost".to_string(),
            port: 8080,
            tags: metric::TagMap::default(),
            forwards: Vec::new(),
            config_path: Some("sources.tcp".to_string()),
        }
    }
}

/// State for a TCP backed source.
pub struct TCP {
    config: TCPConfig,
    thread:  thread::ThreadHandle,
}

impl Source for TCP {

    fn run(&mut self, _poll: mio::Poll) {
        //let addrs = (host_port.to_socket_addrs();
        //match addrs {
        //    Ok(ips) => {
        //        let ips: Vec<_> = ips.collect();
        //        let mut listeners = util::TokenSlab::<mio::net::TcpListener>::new();

        //        for addr in ips {
        //            let listener = mio::net::TcpListener::bind(&addr)
        //                .expect("Unable to bind to TCP socket");
        //            info!("registered listener for {:?}", addr);
        //            let token = listeners.insert(listener);
        //            poll.register(
        //                &listeners[token],
        //                token,
        //                mio::Ready::readable(),
        //                mio::PollOpt::edge(),
        //            ).unwrap();
        //        }

        //        accept_loop(poll, listeners);
        //    }

        //    Err(e) => {
        //        panic!(
        //            "Unable to perform DNS lookup on {} with error {}",
        //            host_port, e);
        //    }
        //}
    }
}

fn spawn_stream_handlers<H>(
    chans: util::Channel,
    tags: &sync::Arc<metric::TagMap>,
    listener: &mio::net::TcpListener,
    handler_fn: H,
    stream_handlers: &mut Vec<thread::ThreadHandle>,
) -> () 
where
    H: Send + Sync + Copy + 'static + FnOnce(util::Channel, &sync::Arc<metric::TagMap>, &mio::Poll, mio::net::TcpStream) -> ()
{
    loop {
        match listener.accept() {
            Ok((stream, _addr)) => {
                let rchans = chans.clone();
                let rtags = sync::Arc::clone(tags);
                let new_stream = thread::spawn(move |poller| {
                    poller
                        .register(
                            &stream,
                            mio::Token(0),
                            mio::Ready::readable(),
                            mio::PollOpt::edge(),
                        )
                        .unwrap();

                    handler_fn(rchans, &rtags, &poller, stream);
                });
                stream_handlers.push(new_stream);
            }

            Err(e) => match e.kind() {
                std::io::ErrorKind::WouldBlock => {
                    break;
                }
                _ => unimplemented!(),
            },
        };
    }
}


fn accept_loop<H>(
    mut chans: util::Channel,
    tags: &sync::Arc<metric::TagMap>,
    poll: mio::Poll,
    listeners: util::TokenSlab<mio::net::TcpListener>, 
    handler_fn: H,
) -> ()
where
    H: Send + Sync + Copy + 'static + FnOnce(util::Channel, &sync::Arc<metric::TagMap>, &mio::Poll, mio::net::TcpStream) -> ()
{
    let mut stream_handlers: Vec<thread::ThreadHandle> = Vec::new();
    loop {
        let mut events = mio::Events::with_capacity(1024);
        match poll.poll(&mut events, None) {
            Err(e) => panic!(format!("Failed during poll {:?}", e)),
            Ok(_num_events) => {
                for event in events {
                    match event.token() {
                        constants::SYSTEM => {
                            for handler in stream_handlers {
                                handler.shutdown();
                            }

                            util::send(&mut chans, metric::Event::Shutdown);
                            return;
                        }
                        listener_token => {
                            let listener = &listeners[listener_token];
                            spawn_stream_handlers(
                                chans.clone(), // TODO: do not clone, make an Arc
                                tags,
                                listener,
                                handler_fn,
                                &mut stream_handlers,
                            );
                        }
                    }
                }
            }
        }
    }
}

impl Stoppable for TCP {
    fn join(self) {
        self.thread.join();
    }

    fn shutdown(self) {
        self.thread.shutdown();
    }
}

impl TCP {

    /// Constructs and starts a new TCP source.
    pub fn new <H>(chans: util::Channel, config: TCPConfig, stream_handler: H) -> Self 
    where
        H: Send + Sync + Copy + 'static + FnOnce(util::Channel, &sync::Arc<metric::TagMap>, &mio::Poll, mio::net::TcpStream) -> ()
    {
        let addrs = (config.host.as_str(), config.port).to_socket_addrs();
        trace!("ADDRS {:?}", addrs);
        let mut listeners = util::TokenSlab::<mio::net::TcpListener>::new();
        match addrs {
            Ok(ips) => {
                let ips: Vec<_> = ips.collect();
                for addr in ips {
                    let listener = mio::net::TcpListener::bind(&addr)
                        .expect("Unable to bind to TCP socket");
                    info!("registered listener for {:?}", addr);
                    listeners.insert(listener);
                }
            }

            Err(e) => {
                panic!(
                    "Unable to perform DNS lookup on {:?}:{:?} with error {}",
                    config.host.as_str(), config.port, e);
            }
        };
   
        let tags = sync::Arc::new(config.clone().tags);
        TCP {
            config: config,
            thread: thread::spawn(move |poll| {
                for (token, listener) in listeners.iter() {
                    poll.register(
                        listener,
                        mio::Token::from(token),
                        mio::Ready::readable(),
                        mio::PollOpt::edge(),
                    ).unwrap();
                };
                accept_loop(chans, &tags, poll, listeners, stream_handler)}),
        }
    }
}
