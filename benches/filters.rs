#![feature(test)]

mod benches {
    mod programmable_filter {
        extern crate cernan;
        extern crate test;


        use self::cernan::filter::{Filter, ProgrammableFilter,
                                   ProgrammableFilterConfig};
        use self::cernan::metric;
        use self::test::Bencher;
        use std::path::PathBuf;

        #[bench]
        fn bench_collectd_extraction(b: &mut Bencher) {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            let mut script_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts/collectd_scrub.lua");
            script_dir.push("resources/tests/scripts/");

            let config = ProgrammableFilterConfig {
                scripts_directory: Some(script_dir),
                script: Some(script),
                forwards: Vec::new(),
                config_path: Some("filters.collectd_scrub".to_string()),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let orig = "collectd.cernan-llrv-prod-b3fbb697.protocols-TcpExt.\
                        protocol_counter-TCPFastOpenActive";

            b.iter(|| {
                let metric =
                    metric::Telemetry::new().name(orig).value(12.0).harden().unwrap();
                let event = metric::Event::new_telemetry(metric);
                let mut events = Vec::new();
                let res = cs.process(event, &mut events);
                assert!(res.is_ok());
            });
        }

    }
}
