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

The cernan server has a few command-line toggles to control its behaviour:

```
-C, --config <config>    The config file to feed in.
-v               Turn on verbose output.
```

The verbose flag `-v` allows multiples, each addition cranking up the verbosity
by one. So:

* `-v` -- error, warning
* `-vv` -- error, warning, info
* `-vvv` -- error, warning, info, debug
* `-vvvv` -- error, warning, info, debug, trace

The `--config` flag allows for more fine grained control of cernan via the
config file.

## Configuration

The cernan server has options to control which ports its ingestion interfaces
run on:

```
statsd-port=<INT>        The UDP port to bind to for statsd traffic. [default: 8125]
graphite-port=<INT>      The TCP port to bind to for graphite traffic. [default: 2003]
```

## Changing how frequently metrics are output

```
flush-interval=<INT>  How frequently to flush metrics to the backends in seconds. [default: 10].
```

## Enabling the console or graphite backends

By default no backends are enabled. In this mode cernan server doesn't do that
much. Backends are configured in a `backends` table. To enable a backend with
defaults it is sufficient to make a sub-table for it. In the following, all
backends are enabled with their default:

```
[backends]
  [console]
  [wavefront]
  [librato]
```

For backends which support it `cernan` can report metadata about the
metric. These are called "tags" by some aggregators and `cernan` uses this
terminology. In AWS you might choose to include your instance ID with each
metric point, as well as the service name. You may set tags like so:

```
[tags]
source = cernan
```

Each key / value pair will converted to the appropriate tag, depending on the
backend.

### librato

The librato backend has additional options for defining authentication:

```
username=<STRING>   The librato username for authentication. [default: ceran].
token=<STRING>      The librato token for authentication. [default: cernan_totally_valid_token].
host=<STRING>       The librato host to report to. [default: https://metrics-api.librato.com/v1/metrics]
```

### wavefront

The wavefront backend has additional options for defining where the wavefront
proxy runs:

```
skip-aggrs=<BOOL]  Send aggregate metrics to wavefront
port=<INT>         The port wavefront proxy is running on. [default: 2878].
host=<STRING>      The host wavefront proxy is running on. [default: 127.0.0.1].
```

You may find an example configuration file in the top-level of this project,
`example-config.toml`. The TOML specification is
[here](https://github.com/toml-lang/toml).

## Prior Art

The inspiration for the intial `cernan` work leaned very heavily on Mark Story's
[rust-stats-server](https://github.com/markstory/rust-statsd-server). I
originally thought I might just adapt that project but ambitions grew. Thank you
Mark!
