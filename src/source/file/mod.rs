mod file_server;
mod file_watcher;

pub use self::file_server::{FileServer, FileServerConfig};

#[cfg(test)]
mod test {
    extern crate tempdir;

    use self::file_watcher::FileWatcher;
    use super::*;
    use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
    use std::fs;
    use std::io::Write;
    use time;

    // actions that apply to a single FileWatcher
    #[derive(Clone, Debug)]
    enum FWAction {
        WriteLine(String),
        RotateFile,
        DeleteFile,
        Pause(u32),
        Exit,
    }

    impl Arbitrary for FWAction {
        fn arbitrary<G>(g: &mut G) -> FWAction
        where
            G: Gen,
        {
            let i: usize = g.gen_range(0, 100);
            let ln_sz = g.gen_range(0, 256);
            let pause = g.gen_range(1, 3);
            match i {
                0...50 => {
                    FWAction::WriteLine(g.gen_ascii_chars().take(ln_sz).collect())
                }
                51...75 => FWAction::Pause(pause),
                76...85 => FWAction::RotateFile,
                86...95 => FWAction::DeleteFile,
                _ => FWAction::Exit,
            }
        }
    }

    #[test]
    fn test_file_watcher() {
        fn inner(actions: Vec<FWAction>) -> TestResult {
            let dir = tempdir::TempDir::new("file_watcher_qc").unwrap();
            let path = dir.path().join("a_file.log");
            let mut fp = fs::File::create(&path).expect("could not create");
            let mut fw =
                FileWatcher::new(path.clone()).expect("must be able to create");

            let mut expected_read = Vec::new();

            for action in actions.iter() {
                match *action {
                    FWAction::DeleteFile => {
                        let _ = fs::remove_file(&path);
                        assert!(!path.exists());
                        assert!(expected_read.is_empty());
                        break;
                    }
                    FWAction::Pause(ps) => time::delay(ps),
                    FWAction::Exit => break,
                    FWAction::WriteLine(ref s) => {
                        assert!(fp.write(s.as_bytes()).is_ok());
                        expected_read.push(s);
                        assert!(fp.write("\n".as_bytes()).is_ok());
                        assert!(fp.flush().is_ok());
                    }
                    FWAction::RotateFile => {
                        let mut new_path = path.clone();
                        new_path.set_extension("log.1");
                        match fs::rename(&path, &new_path) {
                            Ok(_) => {}
                            Err(e) => {
                                println!("ERROR: {:?}", e);
                                assert!(false);
                            }
                        }
                        fp = fs::File::create(&path).expect("could not create");
                    }
                }

                let mut buf = String::new();
                let mut attempts = 0;
                while !expected_read.is_empty() {
                    if fw.dead() {
                        break;
                    }
                    match fw.read_line(&mut buf) {
                        Ok(0) => {
                            attempts += 1;
                            if attempts > 512 {
                                break;
                            }
                            // It's possible that we've written a blank
                            // string. Here we check to see if that is the
                            // case. `exp` will be empty in that case.
                            let exp =
                                expected_read.pop().expect("must be a read here");
                            if !exp.is_empty() {
                                expected_read.push(exp);
                            }
                        }
                        Ok(sz) => {
                            attempts = 0;
                            let exp =
                                expected_read.pop().expect("must be a read here");
                            assert_eq!(buf, *exp);
                            assert_eq!(sz, buf.len());
                            buf.clear();
                        }
                        Err(_) => break,
                    }
                }

                assert!(expected_read.is_empty());
            }
            TestResult::passed()
        }
        QuickCheck::new()
            .tests(10000)
            .max_tests(100000)
            .quickcheck(inner as fn(Vec<FWAction>) -> TestResult);
    }
}
