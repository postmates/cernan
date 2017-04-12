#![feature(test)]

mod benches {
    mod programmable_filter {
        extern crate test;
        extern crate cernan;


        use self::cernan::filter::{Filter, ProgrammableFilter, ProgrammableFilterConfig};
        use self::cernan::metric;
        use self::test::Bencher;
        use std::path::PathBuf;

        #[bench]
        fn bench_collectd_extraction(b: &mut Bencher) {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts/collectd_scrub.lua");

            let config = ProgrammableFilterConfig {
                script: script,
                forwards: Vec::new(),
                config_path: "filters.collectd_scrub".to_string(),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let orig = "collectd.cernan-llrv-prod-b3fbb697.protocols-TcpExt.\
                        protocol_counter-TCPFastOpenActive";

            b.iter(|| {
                       let metric = metric::Telemetry::new(orig, 12.0);
                       let event = metric::Event::new_telemetry(metric);
                       let res = cs.process(event);
                       assert!(res.is_ok());
                   });
        }

    }
}
