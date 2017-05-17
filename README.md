# cernan - telemetry aggregation and shipping, last up the ladder

[![Build Status](https://travis-ci.org/postmates/cernan.svg?branch=master)](https://travis-ci.org/postmates/cernan) [![Codecov](https://img.shields.io/codecov/c/github/postmates/cernan.svg)](https://codecov.io/gh/postmates/cernan) 

![Eugene Cernan, Apollo 17 EVA](Gene-Cernan-1-578x485.jpg)

Cernan is a telemetry and logging aggregation server. It exposes multiple
interfaces for ingestion and can emit to multiple aggregation sources while doing
in-flight manipulation of data. Cernan has minimal CPU and memory requirements
and is intended to service bursty telemetry _without_ load shedding. Cernan aims
to be _reliable_ and _convenient_ to use, both for application engineers and
operations staff.

Why you might choose to use cernan: 

  * You need to ingest telemetry from multiple protocols. 
  * You need to multiplex telemetry over aggregation services. 
  * You want to convert log lines into telemetry. 
  * You want to convert telemetry into log lines. 
  * You want to transform telemetry or log lines in-flight. 

If you'd like to learn more, please do have a look in
our [wiki](https://github.com/postmates/cernan/wiki/).

# Quickstart

To build cernan you will need to
have [Rust](https://www.rust-lang.org/en-US/). This should be as simple as:

    > curl -sSf https://static.rust-lang.org/rustup.sh | sh

Once Rust is installed, from the root of this project run:

    > cargo run -- --config examples/configs/quickstart.toml

and you're good to go. Cernan will report to stdout what ports it is now
listening on. If you would like to debug your service--to determine if the
telemetry you intend is issued--run cernan like:

    > cargo run -- -vvvv --config examples/configs/quickstart.toml

and full trace output will be reported to stdout.

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

# License 

cernan is copyright © 2017 Postmates, Inc and released to the public under the
terms of the MIT license.
