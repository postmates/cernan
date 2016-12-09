use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use chrono::UTC;
use std::{cmp, time, thread};
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

pub fn update_time() {
    let dur = time::Duration::from_millis(500);
    loop {
        thread::sleep(dur);
        let now = UTC::now().timestamp() as usize;
        let order = Ordering::Relaxed;
        trace!("updated cernan {:?} now, is: {}", order, now);
        NOW.store(now, order);
    }
}

#[inline]
pub fn delay(attempts: u32) {
    println!("ATTEMPTS: {}", attempts);
    if attempts > 0 && attempts < 9 {
        let delay = cmp::min(500, 2u32.pow(attempts));
        let sleep_time = time::Duration::from_millis(delay as u64);
        thread::sleep(sleep_time);
    } else if attempts >= 9 {
        let sleep_time = time::Duration::from_millis(500);
        thread::sleep(sleep_time);
    };
}
