use std::sync::Arc;
use std::sync::atomic::{AtomicUsize,Ordering};
use chrono::UTC;
use std::{time,thread};

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
