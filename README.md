# cernan - telemetry aggregation and shipping, last up the ladder

![cernan](Gene-Cernan-1-578x485.jpg)

`cernan` is a telemetry and logging aggregation server. It exposes a statsd and
graphite interface as of this writing. In the Glorious Future it will ingest
logs as well.

[![Build Status](https://travis-ci.com/postmates/cernan.svg?token=YZ973qi8DocmxHi3Nn48&branch=master)](https://travis-ci.com/postmates/cernan)

# Installation

The ambition is for `cernan` to be easily installed and run on development
machines. The only slight rub is that you _will_ need to install rust. Should be
as simple as:

    > curl -sSf https://static.rust-lang.org/rustup.sh | sh

Ensure that `~/.cargo/bin` is in your PATH. Then from the root of this project:

    > cargo install

Recent versions of OSX may have some goofy OpenSSL issues, which can be resolved
by issuing

    > brew install openssl
    > brew link --force openssl

and following the onscreen instructions. Run

    > cargo run -- --console

and you're good to go. `cargo install` will drop a binary in your path or you
can `cargo run` from inside this project. If you would like to debug your
service--to determine if the telemetry you intend is issued--run cernan like

    > cargo run -- --console -vvvv

and full trace output will be reported to the tty.

# Usage

The cernan server has options to control which ports its interfaces run on:

```
--statsd-port <port>        The UDP port to bind to for statsd traffic. [default: 8125]
--graphite-port <port>      The TCP port to bind to for graphite traffic. [default: 2003]
```

## Changing how frequently metrics are output

```
--flush-interval=<p>  How frequently to flush metrics to the backends in seconds. [default: 10].
```

On each flush interval event, derived metrics for timers are calculated. This
duration is tracked as `statsd.processing_time`. You can use this metric to
track how long statsd is spending generating derived metrics.

## Enabling the console or graphite backends

By default no backends are enabled. In this mode the statsd server doesn't do
that much. To backends use the CLI flags:

```
--console             Enable the console backend.
--librato             Enable the librato backend.
--wavefront           Enable the wavefront backend.
```

For backends which support it `cernan` can report metadata about the
metric. These are called "tags" by some aggregators and `cernan` uses this
terminology. In AWS you might choose to include your instance ID with each
metric point, as well as the service name. You may set tags like so:

```
--tags=<p>     A comma separated list of tags to report to supporting backends. [default: source=cernan]
```

The librato backend has additional options for defining authentication:

```
--librato-username=<p>        The librato username for authentication. [default: ceran].
--librato-token=<p>        The librato token for authentication. [default: cernan_totally_valid_token].
--librato-host <librato-host>       The librato host to report to. [default: https://metrics-api.librato.com/v1/metrics]
```

The wavefront backend has additional options for defining where the wavefront
proxy runs:

```
--wavefront-skip-aggrs    Send aggregate metrics to wavefront
--wavefront-port=<p>      The port wavefront proxy is running on. [default: 2878].
--wavefront-host=<p>      The host wavefront proxy is running on. [default: 127.0.0.1].
```

## Prior Art

The inspiration for the intial `cernan` work leaned very heavily on Mark Story's
[rust-stats-server](https://github.com/markstory/rust-statsd-server). I
originally thought I might just adapt that project but ambitions grew. Thank you
Mark!
