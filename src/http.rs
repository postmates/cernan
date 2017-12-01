//#! Tiny, unassuming HTTP Server

extern crate tiny_http;

use std;
use thread;

pub type Request = tiny_http::Request;
pub type Response = tiny_http::Response<std::io::Cursor<std::vec::Vec<u8>>>;
pub type Header = tiny_http::Header;
pub type HeaderField = tiny_http::HeaderField;

/// Simple single threaded HTTP request handler.
pub trait Handler: Sync + Send {
    fn handle(&self, request: Request) -> ();
}

/// Single threaded HTTP server.
pub struct Server {
    /// Thread handle for the operating HTTP server.
    thread: thread::ThreadHandle,
}

fn http_server<H>(
    poller: thread::Poll,
    tiny_http_server: tiny_http::Server,
    handler: H,
) -> ()
where
    H: Handler,
{
    loop {
        let mut events = thread::Events::with_capacity(1024);
        match poller.poll(&mut events, Some(std::time::Duration::from_millis(5))) {
            Ok(_) => {
                break;
            }

            Err(e) => {
                panic!(format!("Failed during poll {:?}", e));
            }
        }

        // match tiny_http_server.recv_timeout(std::time::Duration::from_millis(1000))
        // {     Ok(maybe_a_request) => {
        //         if let Some(request) = maybe_a_request {
        //             handler.handle(request);
        //         }
        //     }

        //     Err(e) => {
        //         panic!(format!("Failed during recv_timeout {:?}", e));
        //     }
        // }
    }
}

/// Single threaded HTTP server implementation.
impl Server {
    pub fn new<H>(host_port: String, handler: H) -> Self
    where
        H: Handler,
    {
        Server {
            thread: thread::spawn(move |poller| {
                let tiny_http_server = tiny_http::Server::http(host_port).unwrap();
                http_server(poller, tiny_http_server, handler)
            }),
        }
    }
}

/// Graceful shutdown support for Server.
impl thread::Stoppable for Server {
    fn join(self) {
        self.thread.join();
    }

    fn shutdown(self) {
        self.thread.shutdown();
    }
}
