//! Transform or create `metric::Event` from a stream of `metric::Event`.
//!
//! cernan filters are intended to input `metric::Event` and then adapt that
//! stream, either by injecting new `metric::Event` or by transforming the
//! stream members as they come through. Exact behaviour varies by filters. The
//! filter receives on an input channel and outputs over its forwards.

use hopper;
use metric;
use time;
use util;

mod programmable_filter;
pub mod delay_filter;
mod flush_boundary_filter;
pub mod json_encode_filter;

pub use self::delay_filter::{DelayFilter, DelayFilterConfig};
pub use self::flush_boundary_filter::{FlushBoundaryFilter, FlushBoundaryFilterConfig};
pub use self::json_encode_filter::{JSONEncodeFilter, JSONEncodeFilterConfig};
pub use self::programmable_filter::{ProgrammableFilter, ProgrammableFilterConfig};

/// Errors that can strike a Filter
#[derive(Debug)]
pub enum FilterError {
    /// Specific to a ProgrammableFilter, means no function is available as
    /// called in the script
    NoSuchFunction(&'static str, metric::Event),
}

fn name_in_fe(fe: &FilterError) -> &'static str {
    match *fe {
        FilterError::NoSuchFunction(n, _) => n,
    }
}

fn event_in_fe(fe: FilterError) -> metric::Event {
    match fe {
        FilterError::NoSuchFunction(_, m) => m,
    }
}

/// The Filter trait
///
/// All filters take as input a stream of `metric::Event` and produce as output
/// another `metric::Event` stream. That's it. The exact method by which each
/// stream works depends on the implementation of the Filter.
pub trait Filter {
    /// Process a single `metric::Event`
    ///
    /// Individual Filters will implementat this function depending on their
    /// mechanism. See individaul filters for details.
    fn process(
        &mut self,
        event: metric::Event,
        res: &mut Vec<metric::Event>,
    ) -> Result<(), FilterError>;

    /// Run the Filter
    ///
    /// It is not expected that most Filters will re-implement this. If this is
    /// done, take care to obey overload signals and interpret errors from
    /// `Filter::process`.
    fn run(
        &mut self,
        recv: hopper::Receiver<metric::Event>,
        sources: Vec<String>,
        mut chans: util::Channel,
    ) {
        let mut attempts = 0;
        let mut events = Vec::with_capacity(64);
        let mut recv = recv.into_iter();
        let mut total_shutdowns = 0;
        loop {
            time::delay(attempts);
            match recv.next() {
                None => attempts += 1,
                Some(metric::Event::Shutdown) => {
                    util::send(&mut chans, metric::Event::Shutdown);
                    total_shutdowns += 1;
                    if total_shutdowns >= sources.len() {
                        trace!(
                            "Received shutdown from every configured source: {:?}",
                            sources
                        );
                        return;
                    }
                }
                Some(event) => {
                    attempts = 0;
                    match self.process(event, &mut events) {
                        Ok(()) => for ev in events.drain(..) {
                            util::send(&mut chans, ev)
                        },
                        Err(fe) => {
                            error!(
                                "Failed to run filter with error: {:?}",
                                name_in_fe(&fe)
                            );
                            let event = event_in_fe(fe);
                            util::send(&mut chans, event);
                        }
                    }
                }
            }
        }
    }
}
