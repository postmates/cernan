use std::sync::Arc;
use std::sync::atomic::{AtomicUsize,Ordering};
use chrono::UTC;
use std::{cmp,time,thread};

lazy_static! {
    static ref NOW: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(UTC::now().timestamp() as usize));
}

pub fn now() -> i64 {
    NOW.load(Ordering::Relaxed) as i64
}

pub fn update_time() {
    let dur = time::Duration::from_millis(500);
    loop {
        thread::sleep(dur);
        NOW.store(UTC::now().timestamp() as usize, Ordering::Relaxed);
    }
}

#[inline]
pub fn delay(attempts: u32) {
    if attempts > 0 {
        let max_delay : u32 = 60_000;
        let delay = cmp::min(max_delay, 2u32.pow(attempts));
        let sleep_time = time::Duration::from_millis(delay as u64);
        thread::sleep(sleep_time);
    }
}
