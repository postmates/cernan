# Contributing to cernan

Hey there, wow! Cernan is a collaborative effort and we're really excited to see
that you're giving it a shot. Thank you!

## Feature Requests

If you'd like to request we add a feature to cernan, go right ahead! Please
create an issue in [our tracker](https://github.com/postmates/cernan/issues) and
tag it as a "feature request".

## Bug Reports

Cernan is intended to run on a variety of hosts but the developers, as of this
writing, have access to a limited pool of systems. If you've caught a bug please
do create an issue in [our tracker](http://github.com/postmates/cernan/issues).
Here's a template that you can use to file a bug, though it's not necessary to
use it exactly:

    <short summary of the bug>

    I tried this:

    <reproducible steps to trigger the bug>

    I expected to see this happen: <explanation>

    Instead, this happened: <explanation>

    ## Meta

    `rustc --version --verbose`:

    `cernan --version`:

    Backtrace:

All three components are important: what you did, what you expected, what
happened instead. Please include the output of `rustc --version --verbose`,
which includes important information about what platform you're on and what
version of Rust you're using to compile cernan.

Sometimes, a backtrace is helpful, and so including that is nice. To get a
backtrace, set the `RUST_BACKTRACE` environment variable to a value other than
`0`. The easiest way to do this is to invoke cernan like this:

```bash
$ RUST_BACKTRACE=1 cernan ...
```

## Pull Requests

Pull requests are the mechanism we use to incorporate changes to cernan. GitHub
itself has [documentation](https://metrics.wavefront.com/alert/1469751512848) on
using the Pull Request feature. We use the 'fork and pull' model described
there.

Please make pull requests against the `master` branch.
