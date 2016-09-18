#![feature(test)]

extern crate test;
extern crate cernan;
extern crate tempdir;

use self::test::Bencher;

use cernan::mpsc;
use cernan::metric::Event;

#[bench]
fn bench_snd(b: &mut Bencher) {
    b.iter(|| {
        let dir = tempdir::TempDir::new("cernan").unwrap();
        let (mut snd, _) = mpsc::channel("bench_snd", dir.path());

        for i in 0..1000 {
            snd.send(&Event::TimerFlush);
        }
    });
}

#[bench]
fn bench_snd_rcv(b: &mut Bencher) {
    b.iter(|| {
        let dir = tempdir::TempDir::new("cernan").unwrap();
        let (mut snd, mut rcv) = mpsc::channel("bench_snd", dir.path());

        for i in 0..1000 {
            snd.send(&Event::TimerFlush);
            rcv.next().unwrap();
        }
    });
}
