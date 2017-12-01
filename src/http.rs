//! Tiny, unassuming HTTP Server

extern crate tiny_http;

use std;
use thread;

/// HTTP request.  Alias of tiny_http::Request.
pub type Request = tiny_http::Request;

/// HTTP response.  Alias of tiny_http::Response.
pub type Response = tiny_http::Response<std::io::Cursor<std::vec::Vec<u8>>>;

/// HTTP header.  Alias of tiny_http::Header.
pub type Header = tiny_http::Header;

/// HTTP header field.  Alias of tiny_http::HeaderField.
pub type HeaderField = tiny_http::HeaderField;

/// Simple single threaded HTTP request handler.
pub trait Handler: Sync + Send {

    /// Handler for a single HTTP request.
    fn handle(&self, request: Request) -> ();
}

/// Single threaded HTTP server.
pub struct Server {
    /// Thread handle for the operating HTTP server.
    thread: thread::ThreadHandle,
}

fn http_server<H>(
    poller: thread::Poll,
    _tiny_http_server: tiny_http::Server,
    _handler: H,
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

    /// Create and start an HTTP server on the given host and port.
    pub fn new<H: Handler + 'static>(host_port: String, handler: H) -> Self
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
