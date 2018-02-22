use constants;
use hopper;
use metric;
use mio;
use source::Source;
use std;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::net::ToSocketAddrs;
use thread;
use thread::Stoppable;
use util;

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
    /// The forwards that the source will send all its Telemetry.
    pub forwards: Vec<String>,
}

impl Default for TCPConfig {
    fn default() -> TCPConfig {
        TCPConfig {
            host: "localhost".to_string(),
            port: 8080,
            forwards: Vec::new(),
            config_path: Some("sources.tcp".to_string()),
        }
    }
}

/// Simple single threaded TCP Stream handler.
pub trait TCPStreamHandler: 'static + Default + Clone + Sync + Send {
    /// Constructs a new handler for mio::net::TCPStreams.
    fn new() -> Self {
        Default::default()
    }

    /// Handler for a single HTTP request.
    fn handle_stream(&mut self, util::Channel, &mio::Poll, mio::net::TcpStream) -> ();
}

/// State for a TCP backed source.
pub struct TCP<H> {
    listeners: util::TokenSlab<mio::net::TcpListener>,
    handlers: Vec<thread::ThreadHandle>,
    phantom: PhantomData<H>,
}

impl<H> Source<TCPConfig> for TCP<H>
where
    H: TCPStreamHandler,
{
    /// Constructs and starts a new TCP source.
    fn init(config: TCPConfig) -> Self {
        let addrs = (config.host.as_str(), config.port).to_socket_addrs();
        let mut listeners = util::TokenSlab::<mio::net::TcpListener>::new();
        match addrs {
            Ok(ips) => {
                let ips: Vec<_> = ips.collect();
                for addr in ips {
                    let listener = mio::net::TcpListener::bind(&addr)
                        .expect("Unable to bind to TCP socket");
                    info!("Registering listener for {:?}", addr);
                    listeners.insert(listener);
                }
            }

            Err(e) => {
                panic!(
                    "Unable to perform DNS lookup on {:?}:{:?} with error {}",
                    config.host.as_str(),
                    config.port,
                    e
                );
            }
        };

        TCP {
            listeners: listeners,
            handlers: Vec::new(),
            phantom: PhantomData,
        }
    }

    /// Starts the accept loop.
    fn run(self, chans: util::Channel, poller: mio::Poll) -> () {
        for (idx, listener) in self.listeners.iter() {
            if let Err(e) = poller.register(
                listener,
                mio::Token::from(idx),
                mio::Ready::readable(),
                mio::PollOpt::edge(),
            ) {
                error!("Failed to register {:?} - {:?}!", listener, e);
            }
        }

        self.accept_loop(chans, &poller)
    }
}

impl<H> TCP<H>
where
    H: TCPStreamHandler,
{
    fn accept_loop(mut self, mut chans: util::Channel, poll: &mio::Poll) -> () {
        loop {
            let mut events = mio::Events::with_capacity(1024);
            match poll.poll(&mut events, None) {
                Err(e) => panic!(format!("Failed during poll {:?}", e)),
                Ok(_num_events) => for event in events {
                    match event.token() {
                        constants::SYSTEM => {
                            for handler in self.handlers {
                                handler.shutdown();
                            }

                            util::send(&mut chans, metric::Event::Shutdown);
                            return;
                        }
                        listener_token => {
                            if let Err(e) =
                                self.spawn_stream_handlers(&chans, listener_token)
                            {
                                let listener = &self.listeners[listener_token];
                                error!("Failed to spawn stream handlers! {:?}", e);
                                error!("Deregistering listener for {:?} due to unrecoverable error!", *listener);
                                let _ = poll.deregister(listener);
                            }
                        }
                    }
                },
            }
        }
    }

    fn spawn_stream_handlers(
        &mut self,
        chans: &[hopper::Sender<metric::Event>],
        listener_token: mio::Token,
    ) -> Result<(), std::io::Error> {
        let listener = &self.listeners[listener_token];
        loop {
            match listener.accept() {
                Ok((stream, _addr)) => {
                    let rchans = chans.to_owned();
                    let new_stream = thread::spawn(move |poller| {
                        // Note - Stream handlers are allowed to crash without
                        // compromising Cernan's ability to gracefully shutdown.
                        poller
                            .register(
                                &stream,
                                mio::Token(0),
                                mio::Ready::readable(),
                                mio::PollOpt::edge(),
                            )
                            .unwrap();

                        let mut handler = H::new();
                        handler.handle_stream(rchans, &poller, stream);
                    });
                    self.handlers.push(new_stream);
                }
                Err(e) => match e.kind() {
                    ErrorKind::ConnectionAborted
                    | ErrorKind::Interrupted
                    | ErrorKind::TimedOut => {
                        // Connection was closed before we could accept or
                        // we were interrupted. Press on.
                        continue;
                    }
                    ErrorKind::WouldBlock => {
                        //Out of connections to accept. Wrap it up.
                        return Ok(());
                    }
                    _ => return Err(e),
                },
            };
        }
    }
}
