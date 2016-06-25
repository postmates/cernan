//! Provides the CLI option parser
//!
//! Used to parse the argv/config file into a struct that
//! the server can consume and use as configuration data.

use docopt::Docopt;

static USAGE: &'static str =
    "
Usage: cernan [options]
       cernan --help

Options:
  -h, --help              Print help \
     information.
  -p, --port=<p>          The UDP port to bind to [default: 8125].
  \
     --flush-interval=<p>    How frequently to flush metrics to the backends in seconds. \
     [default: 10].
  --console               Enable the console backend.
  --wavefront             \
     Enable the wavefront backend.
  --librato               Enable the librato backend.
  \
  --tags=<p>     A comma separated list of tags to report to supporting backends. [default: source=cernan]
  --wavefront-port=<p>    The port wavefront proxy is running on. [default: 2878].
  \
     --wavefront-host=<p>    The host wavefront proxy is running on. [default: 127.0.0.1].
  \
     --librato-username=<p>  The librato username for authentication. [default: statsd].
  \
     --librato-host=<p>      The librato host to report to. [default: \
     https://metrics-api.librato.com/v1/metrics].
  --librato-token=<p>     The librato token for \
     authentication. [default: statsd].
  --version
";

/// Holds the parsed command line arguments
#[derive(RustcDecodable, Debug)]
pub struct Args {
    pub flag_port: u16,
    pub flag_flush_interval: u64,
    pub flag_console: bool,
    pub flag_wavefront: bool,
    pub flag_librato: bool,
    pub flag_tags: String,
    pub flag_wavefront_port: u16,
    pub flag_wavefront_host: String,
    pub flag_librato_username: String,
    pub flag_librato_token: String,
    pub flag_librato_host: String,
    pub flag_help: bool,
    pub flag_version: bool,
}

pub fn parse_args() -> Args {
    Docopt::new(USAGE)
        .and_then(|d| d.decode())
        .unwrap_or_else(|e| e.exit())
}
