# cernan - telemetry aggregation and shipping, last up the ladder

![Eugene Cernan, Apollo 17 EVA](Gene-Cernan-1-578x485.jpg)

Cernan is a telemetry and logging aggregation server. It exposes a statsd and
graphite interface and can ingest logs as well. In the Glorious Future cernan
will be able to act as a forwarding server and be able to do in-flight
manipulation of data.

[![Build Status](https://travis-ci.com/postmates/cernan.svg?token=YZ973qi8DocmxHi3Nn48&branch=master)](https://travis-ci.com/postmates/cernan)

# Installation

The ambition is for cernan to be easily installed and run on development
machines. The only slight rub is that you _will_ need to install rust. Should be
as simple as:

    > curl -sSf https://static.rust-lang.org/rustup.sh | sh

Ensure that `~/.cargo/bin` is in your PATH. Then from the root of this project:

    > cargo install

Recent versions of OSX may have some goofy OpenSSL issues, which can be resolved
by issuing

    > brew install openssl
    > export OPENSSL_INCLUDE_DIR=/usr/local/opt/openssl/include
    > export DEP_OPENSSL_INCLUDE=/usr/local/opt/openssl/include

and following the onscreen instructions. Run

    > cargo run -- --console

and you're good to go. `cargo install` will drop a binary in your path or you
can `cargo run` from inside this project. If you would like to debug your
service--to determine if the telemetry you intend is issued--run cernan like

    > cargo run -- --console -vvvv

and full trace output will be reported to the tty.

# Cernan's Data Model

There are two stories to cernan's data model, one to do with durability of data
and the other with aggregation.

## Durability

Cernan works very hard to store and process every piece of information you send
it and, in doing so, to never overwhelm your system. This is born of our
frustation with other telemetry systems who fail during crisis periods on
account of high telemetry load. That is, should your application begin to
frantically emit telemetry about its failing state cernan _must_ be able to
ingest and ship this outward.

Cernan's main line of effort in this regard is a disk based queueing system that
allow individual source and sinks to communicate with one another. Each
telemetry point that comes into the system is parsed and serialized to
disk. These serialized points are only read from disk when a sink is capable of
processing it. This limits cernan's eposure to restart related data-loss and
puts a hard cap on cernan's online allocations.

## Aggregation

Cernan is timestamp accurate to the second. Every point of telemetry that is
ingested by cernan is timestamped on receipt, in the case of log lines and
statsd, or by parsing the payload, as in the case of graphite. Cernan sinks
which opt into the use of the
[`buckets`](https://github.com/postmates/cernan/blob/master/src/buckets.rs)
structure aggregate points according to their "kind". Each metric is binned by
the second, according to their needs. Sub-second aggregation is not available
though we'd be open to it. The metric kinds are:

  * `gauge` :: A gauge represents a point-in-time metric that sustains across
    bins. A gauge _never_ resets automatically.
  * `counter` :: A counter is a sum of all intput values in a given time bin. A
    counter _will_ reset at the start of each new time bin. Zero will _not_ be
    reported for a counter which does not receive a point.
  * `timer` | `histogram` - Timers and histograms compute percentiles over input
    points. This is done by storing a subset of these points in a structure
    called CKMS, implemented and further discussed in the
    [quantiles](https://github.com/postmates/quantiles) project. Cernan sinks
    may vary in the percentile queries they emit but the default wavefront and
    console emissions are `min`, `max`, `2`, `9`, `25`, `50`, `75`, `90`, `91`,
    `95`, `98`, `99`, `999`. Timers and histograms _will_ reset at the start of
    each new time bin.
  * `delta gauge` :: A delta gauge adjusts the value of a gauge by being summed
    to the existing value. If no such value is present, a value of 0 is
    assumed. A delta gauge modifies a gauge and has its reset properties.
  * `raw` :: A raw represents a piece of telemetry for which no aggregation is
    valid. All graphite points are raw as are all log lines. Raw points are
    stored in last-one-wins fashion per time bin. A raw _will_ reset across time
    bins and will _not_ report values in their absence.

The kinds are encoded
[here](https://github.com/postmates/cernan/blob/master/src/serde_types.in.rs#L17).

# Resource Consumption

Cernan is intended to be a good citizen. It consumes three major resources:

  * disk space / IO
  * CPU
  * memory

Let's talk CPU. Cernan comes with a set of benchmarks and we, in development,
track these closely. On my system the source parsing benchmarks look like so:

```
test bench_graphite                      ... bench:         165 ns/iter (+/- 23)
test bench_statsd_counter_no_sample      ... bench:         159 ns/iter (+/- 19)
test bench_statsd_counter_with_sample    ... bench:         195 ns/iter (+/- 89)
test bench_statsd_gauge_mit_sample       ... bench:         166 ns/iter (+/- 125)
test bench_statsd_gauge_no_sample        ... bench:         156 ns/iter (+/- 21)
test bench_statsd_histogram              ... bench:         158 ns/iter (+/- 26)
test bench_statsd_incr_gauge_no_sample   ... bench:         164 ns/iter (+/- 24)
test bench_statsd_incr_gauge_with_sample ... bench:         172 ns/iter (+/- 48)
test bench_statsd_timer                  ... bench:         161 ns/iter (+/- 21)
```

That is, cernan is able to parse approximately 2,000,000 points per second on my
system. The mpsc round-trip benchmark:

```
test bench_snd_rcv                          ... bench:       2,370 ns/iter (+/- 380)
```

suggests that we're able to clock around 500,000 points from source, to disk and
then out to sink. Experimentation with the `null` sink--below--bears this
out. We encourage you to run these for yourself on your own system. You'll need
a [nightly compiler](https://doc.rust-lang.org/stable/book/benchmark-tests.html)
to run them--for now--but once you've got the nightly compiler `cargo bench`
will get you where you want to be.

Cernan's disk consumption is proportional to the number of telemetry points
added into the system multiplied by the number of enabled sinks, complicated by
the speed of said sinks. That is, for each telemetry point that comes into
cernan we make N duplicates of it in its parsed form, where N is the number of
enabled sinks. If a sink is especially slow--as the
[`firehose`](https://github.com/postmates/cernan/blob/master/src/sinks/firehose.rs)
sink can be--then more points will pool up in the disk-queue. A fast sink--like
[`null`](https://github.com/postmates/cernan/blob/master/src/sinks/null.rs) will
keep only a minimal number of points on disk. At present, this is 100MB worth.

Cernan's allocation patterns are tightly controlled. By flushing to disk we
reduce the need for especially fancy tricks and sustain max allocation of a few
megabytes on our heavily loaded systems. Cernan is vulnerable to randomized
attacks--that is, attacks where randomized metrics names are shipped--and it may
allocate tens of megabytes while sustaining such an attack. If anyone has
suggestions for systematically benchmarking memory use we'd be all for it.

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

# Configuration

## Sources

The cernan server has options to control which ports and protocols are running
ingestion interfaces. These are referred to as 'sources'. 

```
[sources.statsd.primary]
port = 8125 # UDP port to bind for statsd traffic. [default: 8125]

[sources.graphite.primary]
port = 2003 # TCP port to bind for graphite traffic. [default: 2003]

[sources.native]
port = 1972 # TCP port to bind for native cernan traffic. [default: 1972]
```

All sources are optional and may be present in configuration but in a disabled
state. For example, this configuration disables the statsd listeners but keeps
graphite going:

```
[sources.statsd.primary]
enabled = false
port = 8125

[sources.statsd.secondary]
enabled = false
port = 8126

[sources.graphite.primary]
port = 2003
```

It is possible to run multiple sources that cover the same protocol so long as
they are offset on different ports. Each source must be named uniquely. In the
above we have three sources--only one of which is enabled--named
`sources.statsd.primary`, `sources.statsd.secondary` and
`sources.graphite.primary`. All _must_ have a unique name. The native server is
distinct. It is a singleton.

The native protocol is cernan's preferred method of ingestion. The file
`resources/protobufs/native_protocol.proto` defines the encoding. Cernan
requires that each encoded payload be sent over TCP. The payload must be length
prefixed, the length being an unsigned, network-ordered 32 bit integer. Please
see the the above encoding definition for more details.

In addition to network ports, cernan is able to ingest log files. This is
configured in a different manner than the above as there may be many different
file ingesters. Files are also considered sources. 

```
[sources.files.very_important]
path = "/var/log/upstart/very_important.log"
```

Will follow a single file. 

```
[sources.files.var_log_logs]
path = "/var/log/**/*.log"
```

Will follow all files that match the above pattern. 

```
[sources.files.var_log_logs]
path = "/var/log/**/*.log"

[sources.files.temporary_log]
path = "/tmp/temporary.log"
```

Will follow all the files that match `/var/log/**/*.log` as well as
`/tmp/temporary.log`. Cernan will pick up new files that match the given
patterns, though it make take up to a minute for cernan to start ingesting them.

## Forwards

A forward is a routing distination from one source to potentially many
sinks. Each source _must_ set at least one forward. Forwards are configured
through the `forwards` parameter on each source. Consider the following:

```
[sources]
  [sources.statsd.primary] 
  enabled = true
  port = 8125
  forwards = ["sinks.console"]
  
  [sources.statsd.secondary] 
  enabled = true
  port = 8126
  forwards = ["sinks.null"]

  [sources.graphite.primary] 
  enabled = true
  port = 2004
  forwards = ["sinks.null", "sinks.console"]

[sinks]
  [sinks.console] 
  bin_width = 1

  [sinks.null] 
  bin_width = 1
```

This sets up a cernan to have two statsd sources, running on ports 8125 and
8126, named 'primary' and 'secondary'. Additionally, a sole graphite source is
enabled. The primary statsd source is will forward all of its metrics to the
console sink while the secondary statsd source will forward to the null
sink. The graphite source will forward its metrics to all available sinks.

## Changing how frequently metrics are output

By default cernan's sinks will flush every sixty seconds. You may adjust this
behaviour can be configured by modifying the `flush-interval` directive:

``` flush-interval=<INT> How frequently to flush metrics to the sinks in
seconds. [default: 60].  ```

The `flush-interval` does not affect aggregations. A full discussion of cernan's
aggregation model is discussed in the sub-section
[Aggregation](#Aggregation).

## Where Cernan stores its on-disk queues

By default cernan will put its on-disk queues into TMPDIR. While this is
acceptable for testing and development this is not desirable for production
deployments. You may adjust where cernan stores its on-disk queues with the
`data-directory` directive:

```
data-directory = "/var/lib/cernan/"
```

In the above, we are requiring that cernan store its files in
`/var/lib/cernan`. The structure of this data is not defined. Cernan will _not_
create the path `data-directory` points to if it does not exist.

## Elliding Points

In some cases it's not nessary for cernan to ship the aggregates of each point
for every second it receives them to achieve a statistically accurate impression
of your system. To that end, cernan allows the user to control the width of
aggregation bins on a per-source basis. For instance, the following will
aggregate points into 1 second bins on the console sink and 10 second bins for
the wavefront sink:

```
[sinks.console]
bin_width = 1

[sinks.wavefront]
bin_width = 10
```

How many points will this ellide? If the `flush-interval` of the system is `F`
and the QOS for a given type of metic is `Q` then the maximum number of points
that will be retained in `F` is `ceil(F/bin_width)`. By default, `F=60`.

## Filters

Cernan provides a facility for transforming in-flight telemetry and logs. This
facility is the 'filter'. Filters are programmable
in [lua 5.3](https://www.lua.org/). The full API is documented in the project
wiki [here](https://github.com/postmates/cernan/wiki/Filter-API). 

Let's say we want to write a filter to strip instance metadata from collectd
metrics. Recall that collectd metric names look like so:

    collectd.computer_name.interface-lo.if_errors.tx

and what we'd like to do is avoid emitting the 'computer_name' to our
sinks. Enabling filters in configuration is much like enabling a sink or source. 

```
scripts-directory = "examples/scripts/"

[sources]
  [sources.graphite.primary]
  enabled = true
  port = 2004
  forwards = ["filters.collectd_scrub"]

[filters]
  [filters.collectd_scrub]
  script = "collectd_scrub.lua"
  forwards = ["sinks.console"]

[sinks]
  [sinks.console]
  bin_width = 1
```

Note that `scripts-directory` must exist and contain the file
`collectd_scrub.lua`. The scrub program:

```
count_per_tick = 0

function process_metric(pyld)
   count_per_tick = count_per_tick + 1

   local old_name = payload.metric_name(pyld, 1)
   local collectd, rest = string.match(old_name, "^(collectd)[%.@][%w_]+(.*)")
   if collectd ~= nil then
      local new_name = string.format("%s%s", collectd, rest)
      payload.set_metric_name(pyld, 1, new_name)
   end
end

function process_log(pyld)
end

function tick(pyld)
   payload.push_metric(pyld, "count_per_tick", count_per_tick)
   payload.push_log(pyld, string.format("count_per_tick: %s", count_per_tick))
   count_per_tick = 0
end 
```

Cernan will pass every metric through the function `process_metric` and this
program will inspect the name, extract the offending bit of metadata and reset
the name in the payload metric. 

There's a little more than filtering going on here. Every `flush-interval` a
filter's `tick` will be called. `collectd_scrub` is pushing a new metric called
`count_per_tick` into the payload, as well as a new log line. 

Payloads are indexed from 1. Negative offsets are also supported, as in
Lua. This is a break from similar systems like heka that index from 0. Metrics
and logs are indexed separately. 

Please see [the wiki](https://github.com/postmates/cernan/wiki/Filter-API) for
information on the full API.

## Sinks

By default no sinks are enabled. In this mode cernan server doesn't do that
much. Sinks are configured individually, by name. To enable a sink with defaults
it is sufficient to make an entry for it. In the following, all sinks are
enabled with their default:

```
[sinks]
  [sinks.console]
  [sinks.wavefront]
  [sinks.null]
  [sinks.firehose.stream_one]
```

For sinks which support it cernan can report metadata about the
metric. These are called "tags" by some aggregators and cernan uses this
terminology. In AWS you might choose to include your instance ID with each
metric point, as well as the service name. You may set tags like so:

```
[tags]
source = cernan
```

Each key / value pair will converted to the appropriate tag, depending on the
sink. Milestone [0.5.0 - agena](https://github.com/postmates/cernan/milestone/7)
increases cernan's ambitions with regard to tags.

### console

The console sink accepts points and aggregates them into
[buckets](https://github.com/postmates/cernan/blob/master/src/buckets.rs), as
the wavefront sink does. The console sink will, once per `flush-interval`, print
its aggregations. The console sink is useful for smoke testing applications.

The console sink accepts only one configuration parameter, `bin_width`.

```
bin_width=<INT>    The width in seconds for aggregating bins. [default: 1].
```

### wavefront

The wavefront sink has additional options for defining where the wavefront
proxy runs:

```
port=<INT>         The port wavefront proxy is running on. [default: 2878].
host=<STRING>      The host wavefront proxy is running on. [default: 127.0.0.1].
bin_width=<INT>    The width in seconds for aggregating bins. [default: 1].
```

You may find an example configuration file in `examples/configs/basic.toml` The
TOML specification is [here](https://github.com/toml-lang/toml).

### influxdb

The InfluxDB sink has options for defining where the InfluxDB ingestion runs:

```
port=<INT>         The port influxdb is running on. [default: 8089].
host=<STRING>      The host influxdb is running on. [default: 127.0.0.1].
bin_width=<INT>    The width in seconds for aggregating bins. [default: 1].
```

You may find an example configuration file in `examples/configs/basic.toml` The
TOML specification is [here](https://github.com/toml-lang/toml).

### null

The null sink accepts points and immediately discards them. This is useful for
testing deployments of cernan, especially on systems with slow disks. For more
information, see the section on 'Durability' and also the section on 'Resource
Consumption'.

There are no configurable options for the null sink.

### firehose 

The `firehose` sink accepts logging information and emits it
into
[Amazon Kinesis Firehose](https://aws.amazon.com/kinesis/firehose/). Kinesis
Firehose can be configured to emit into multiple targets.

You may configure multiple firehoses. In the following, two firehoses are
configured: 

```
[sinks.firehose.stream_one]
delivery_stream = "stream_one"
batch_size = 20

[sinks.firehose.stream_two]
delivery_stream = "stream_two"
batch_size = 800
region = "us-east-1"
```

By default, region is equivalent to `us-west-2`. In the above `stream_one`
should exist in `us-west-2` and `stream_two` in `us-east-1`. 

### native 

The `native` sink accepts both telemetry and logging information and is intended
to be used for one cernan to pass information onto another. In earlier versions
of cernan this was referred to as 'federation' but over an undefined
protocol. The `native` sink uses cernan's native protocol, defined above.

You may have only one native sink. 

```
[sinks.native]
host = "foo.example.com"
port = 1972
```

## Prior Art

The inspiration for the intial cernan work leaned very heavily on Mark Story's
[rust-stats-server](https://github.com/markstory/rust-statsd-server). I
originally thought I might just adapt that project but ambitions grew. Thank you
Mark!
