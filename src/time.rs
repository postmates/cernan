use chrono::UTC;
use std::{thread, time};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

lazy_static! {
    static ref NOW: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(UTC::now().timestamp() as usize));
}

pub fn elapsed_ns(i: Instant) -> u64 {
    let elapsed = i.elapsed();
    (elapsed.as_secs().saturating_mul(1_000_000_000)).saturating_add(elapsed.subsec_nanos() as u64)
}

pub fn now() -> i64 {
    NOW.load(Ordering::Relaxed) as i64
}

pub fn now_ns() -> u64 {
    let now = UTC::now();
    let seconds = (now.timestamp() as u64).saturating_mul(1_000_000_000);
    seconds.saturating_add(now.timestamp_subsec_nanos() as u64)
}

pub fn update_time() {
    let dur = time::Duration::from_millis(500);
    loop {
        thread::sleep(dur);
        let now = UTC::now().timestamp() as usize;
        let order = Ordering::Relaxed;
        NOW.store(now, order);
    }
}

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
