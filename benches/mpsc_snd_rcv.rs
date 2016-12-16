#![feature(test)]

extern crate test;
extern crate cernan;
extern crate tempdir;
extern crate bincode;

use self::test::Bencher;

use cernan::mpsc;
use cernan::metric::{Event, Metric};

use bincode::serde::{serialize, deserialize};
use bincode::SizeLimit;

#[bench]
fn bench_serialize_deserialize_timerflush(b: &mut Bencher) {
    b.iter(|| {
        let s = serialize(&Event::TimerFlush, SizeLimit::Infinite).unwrap();
        assert_eq!(Event::TimerFlush, deserialize(s.as_slice()).unwrap());
    });
}

#[bench]
fn bench_serialize_deserialize_metric(b: &mut Bencher) {
    let m = Metric::new("bench", 1.0).counter();
    b.iter(|| {
        let s = serialize(&m, SizeLimit::Infinite).unwrap();
        let new_m: Metric = deserialize(s.as_slice()).unwrap();
        assert_eq!(Some(1.0), new_m.value());
    });
}

#[bench]
fn bench_snd(b: &mut Bencher) {
    let dir = tempdir::TempDir::new("cernan").unwrap();
    let (mut snd, _) = mpsc::channel("bench_snd", dir.path());
    b.iter(|| {
        for _ in 0..1000 {
            snd.send(Event::TimerFlush);
        }
    });
}

#[bench]
fn bench_snd_rcv(b: &mut Bencher) {
    let dir = tempdir::TempDir::new("cernan").unwrap();
    let (mut snd, mut rcv) = mpsc::channel("bench_snd", dir.path());
    b.iter(|| {
        snd.send(Event::TimerFlush);
        rcv.next().unwrap();
    });
}

#[bench]
fn bench_all_snd_all_rcv(b: &mut Bencher) {
    let dir = tempdir::TempDir::new("cernan").unwrap();
    let (mut snd, mut rcv) = mpsc::channel("bench_snd", dir.path());
    b.iter(|| {
        for _ in 0..1000 {
            snd.send(Event::TimerFlush);
        }
        for _ in 0..1000 {
            rcv.next().unwrap();
        }
    });
}
