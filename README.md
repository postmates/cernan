# cernan - telemetry aggregation and shipping, last up the ladder

![cernan](Gene-Cernan-1-578x485.jpg)

`cernan` is a telemetry and logging aggregation server. It exposes a statsd
interface as of this writing. There are further ambitions.

# Usage

The statsd server has several options to control which ports it runs on:

```
-p, --port=<p>        The UDP port to bind to [default: 8125].
--admin-host=<p>      The host to bind the management server on. [default: 127.0.0.1]
--admin-port=<p>      The port to bind the management server to. [default: 8126]
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

For backends which support it `cernan` can report the source of the metric. In
AWS you might choose to set this to the instance ID. You may set the source like
so:

```
--metric-source=<p>     The source that will be reported to supporting backends. [default: cernan]
```

The librato backend has additional options for defining authentication:

```
--librato-username=<p>  The librato username for authentication. [default: ceran].
--librato-token=<p>     The librato token for authentication. [default: cernan_totally_valid_token].
```

The wavefront backend has additional options for defining where the wavefront
proxy runs:

```
--wavefront-port=<p>    The port wavefront proxy is running on. [default: 2878].
--wavefront-host=<p>    The host wavefront proxy is running on. [default: 127.0.0.1].
```

## Prior Art

The inspiration for the intial `cernan` work leans very heavily on Mark Story's
[rust-stats-server](https://github.com/markstory/rust-statsd-server). I
originally thought I might just adapt that project but ambitions grew. Thank you
Mark!
