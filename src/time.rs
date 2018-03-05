//! Collection of time utilities for cernan
//!
//! Time in cernan is not based strictly on wall-clock. We keep a global clock
//! for cernan and update it ourselves periodically. See `update_time` in this
//! module for more details.

use chrono::offset::Utc;
use std::{thread, time};
use std::sync::atomic::{AtomicUsize, Ordering};

lazy_static! {
    static ref NOW: AtomicUsize = AtomicUsize::new(Utc::now().timestamp() as usize);
}

/// Return the current time in epoch seconds
pub fn now() -> i64 {
    NOW.load(Ordering::Relaxed) as i64
}

/// Update cernan's view of time every 500ms. Time is in UTC.
pub fn update_time() {
    let dur = time::Duration::from_millis(500);
    loop {
        thread::sleep(dur);
        let now = Utc::now().timestamp() as usize;
        let order = Ordering::Relaxed;
        NOW.store(now, order);
    }
}

/// Pause a thread of execution
///
/// This function pauses the thread of execution for a fixed number of
/// attempts. That input, attempts, is used to eponentially increase the length
/// of delay, from 0 milliseconds to 512. A delay attempt of X will pause the
/// thread of execution for:
///
/// - 0 = 0 ms
/// - x, x >= 9 = 512 ms
/// - x, x < 9 = 2**x ms
#[inline]
pub fn delay(attempts: u32) {
    let delay = match attempts {
        0 => return,
        1 => 1,
        2 => 4,
        3 => 8,
        4 => 16,
        5 => 32,
        6 => 64,
        7 => 128,
        8 => 256,
        _ => 512,
    };
    let sleep_time = time::Duration::from_millis(delay as u64);
    thread::sleep(sleep_time);
}
