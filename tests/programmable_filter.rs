mod integration {
    mod programmable_filter {

        extern crate cernan;

        use self::cernan::filter::{Filter, ProgrammableFilterConfig, ProgrammableFilter};
        use self::cernan::metric;
        use std::path::PathBuf;

        #[test]
        fn test_id_filter() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts/identity.lua");

            let config = ProgrammableFilterConfig {
                script: script,
                forwards: Vec::new(),
                config_path: "filters.identity".to_string(),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let metric = metric::Metric::new("identity", 12.0)
                .overlay_tag("foo", "bar")
                .overlay_tag("bizz", "bazz");
            let event = metric::Event::Telemetry(metric);

            let res = cs.process(event.clone());
            assert!(res.is_ok());
            let events = res.ok().unwrap();
            assert!(!events.is_empty());
            assert_eq!(events.len(), 1);
            assert_eq!(events[0], event);
        }

        #[test]
        fn test_remove_log_tag_kv() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts/remove_keys.lua");

            let config = ProgrammableFilterConfig {
                script: script,
                forwards: Vec::new(),
                config_path: "filters.remove_keys".to_string(),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let orig_log = metric::LogLine::new("identity",
                                                "i am the very model of the modern major general")
                .overlay_tag("foo", "bar")
                .overlay_tag("bizz", "bazz");
            let expected_log = metric::LogLine::new("identity",
                                                    "i am the very model of the modern major \
                                                     general")
                .overlay_tag("foo", "bar");
            let orig_event = metric::Event::Log(orig_log);
            let expected_event = metric::Event::Log(expected_log);

            let res = cs.process(orig_event.clone());
            assert!(res.is_ok());
            let events = res.ok().unwrap();
            assert!(!events.is_empty());
            assert_eq!(events.len(), 1);
            assert_eq!(events[0], expected_event);
        }

        #[test]
        fn test_remove_metric_tag_kv() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts/remove_keys.lua");

            let config = ProgrammableFilterConfig {
                script: script,
                forwards: Vec::new(),
                config_path: "filters.remove_keys".to_string(),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let orig_metric = metric::Metric::new("identity", 12.0)
                .overlay_tag("foo", "bar")
                .overlay_tag("bizz", "bazz");
            let expected_metric = metric::Metric::new("identity", 12.0).overlay_tag("foo", "bar");
            let orig_event = metric::Event::Telemetry((orig_metric));
            let expected_event = metric::Event::Telemetry((expected_metric));

            let res = cs.process(orig_event.clone());
            assert!(res.is_ok());
            let events = res.ok().unwrap();
            assert!(!events.is_empty());
            assert_eq!(events.len(), 1);
            assert_eq!(events[0], expected_event);
        }

        #[test]
        fn test_insufficient_args_no_crash() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts/insufficient_args.lua");

            let config = ProgrammableFilterConfig {
                script: script,
                forwards: Vec::new(),
                config_path: "filters.no_args_no_crash".to_string(),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let orig_metric = metric::Metric::new("identity", 12.0)
                .overlay_tag("foo", "bar")
                .overlay_tag("bizz", "bazz");

            let orig_event = metric::Event::Telemetry((orig_metric));

            let res = cs.process(orig_event.clone());
            assert!(res.is_ok());
            let events = res.ok().unwrap();
            assert!(!events.is_empty());
            assert_eq!(events.len(), 1);
            assert_eq!(events[0], orig_event);
        }

        #[test]
        fn test_missing_func() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts/missing_func.lua");

            let config = ProgrammableFilterConfig {
                script: script,
                forwards: Vec::new(),
                config_path: "filters.missing_func".to_string(),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let orig_metric = metric::Metric::new("identity", 12.0)
                .overlay_tag("foo", "bar")
                .overlay_tag("bizz", "bazz");

            let orig_event = metric::Event::Telemetry((orig_metric));

            let res = cs.process(orig_event);
            assert!(res.is_err());
        }

        #[test]
        fn test_add_log_tag_kv() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts/add_keys.lua");

            let config = ProgrammableFilterConfig {
                script: script,
                forwards: Vec::new(),
                config_path: "filters.add_keys".to_string(),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let expected_log = metric::LogLine::new("identity",
                                                    "i am the very model of the modern major \
                                                     general")
                .overlay_tag("foo", "bar")
                .overlay_tag("bizz", "bazz");
            let orig_log = metric::LogLine::new("identity",
                                                "i am the very model of the modern major general")
                .overlay_tag("foo", "bar");
            let orig_event = metric::Event::Log(orig_log);
            let expected_event = metric::Event::Log(expected_log);

            let res = cs.process(orig_event);
            assert!(res.is_ok());
            let events = res.ok().unwrap();
            assert!(!events.is_empty());
            assert_eq!(events.len(), 1);
            assert_eq!(events[0], expected_event);
        }

        #[test]
        fn test_add_metric_tag_kv() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts/add_keys.lua");

            let config = ProgrammableFilterConfig {
                script: script,
                forwards: Vec::new(),
                config_path: "filters.add_keys".to_string(),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let expected_metric = metric::Metric::new("identity", 12.0)
                .overlay_tag("foo", "bar")
                .overlay_tag("bizz", "bazz");
            let orig_metric = metric::Metric::new("identity", 12.0).overlay_tag("foo", "bar");
            let orig_event = metric::Event::Telemetry((orig_metric));
            let expected_event = metric::Event::Telemetry((expected_metric));

            let res = cs.process(orig_event);
            assert!(res.is_ok());
            let events = res.ok().unwrap();
            assert!(!events.is_empty());
            assert_eq!(events.len(), 1);
            assert_eq!(events[0], expected_event);
        }

        #[test]
        fn test_tick_keeps_counts() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts/keep_count.lua");

            let config = ProgrammableFilterConfig {
                script: script,
                forwards: Vec::new(),
                config_path: "filters.keep_count".to_string(),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let metric0 = metric::Event::Telemetry((metric::Metric::new("identity", 12.0)));
            let metric1 = metric::Event::Telemetry((metric::Metric::new("identity", 13.0)));
            let metric2 = metric::Event::Telemetry((metric::Metric::new("identity", 14.0)));

            let log0 = metric::Event::Log(metric::LogLine::new("identity", "a log line"));
            let log1 = metric::Event::Log(metric::LogLine::new("identity", "another"));
            let log2 = metric::Event::Log(metric::LogLine::new("identity", "more"));
            let log3 = metric::Event::Log(metric::LogLine::new("identity", "less"));

            let flush = metric::Event::TimerFlush;

            for ev in &[metric0, metric1, metric2, log0, log1] {
                let _ = cs.process(ev.clone());
            }
            let res = cs.process(flush.clone());
            assert!(res.is_ok());
            let events = res.ok().unwrap();

            assert!(!events.is_empty());
            assert_eq!(events.len(), 2);
            println!("EVENTS: {:?}", events);
            assert_eq!(events[1],
                       metric::Event::Telemetry((metric::Metric::new("count_per_tick", 5.0))));
            assert_eq!(events[0],
                       metric::Event::Log(metric::LogLine::new("filters.keep_count",
                                                               "count_per_tick: 5")));

            for ev in &[log2, log3] {
                let _ = cs.process(ev.clone());
            }
            let res = cs.process(flush);
            assert!(res.is_ok());
            let events = res.ok().unwrap();

            assert!(!events.is_empty());
            assert_eq!(events.len(), 2);
            println!("EVENTS: {:?}", events);
            assert_eq!(events[1],
                       metric::Event::Telemetry((metric::Metric::new("count_per_tick", 2.0))));
            assert_eq!(events[0],
                       metric::Event::Log(metric::LogLine::new("filters.keep_count",
                                                               "count_per_tick: 2")));
        }

        #[test]
        fn test_collectd_non_ip_extraction() {
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
            let expected = "collectd.protocols-TcpExt.protocol_counter-TCPFastOpenActive";

            let metric = metric::Metric::new(orig, 12.0);
            let event = metric::Event::Telemetry(metric);

            let res = cs.process(event);
            assert!(res.is_ok());
            let events = res.ok().unwrap();
            assert!(!events.is_empty());
            assert_eq!(events.len(), 1);

            for event in events {
                match event {
                    metric::Event::Telemetry(m) => {
                        assert_eq!(m.name, expected);
                    }
                    _ => {
                        assert!(false);
                    }
                }
            }
        }

        #[test]
        fn test_non_collectd_extraction() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts/collectd_scrub.lua");

            let config = ProgrammableFilterConfig {
                script: script,
                forwards: Vec::new(),
                config_path: "filters.collectd_scrub".to_string(),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let orig = "totally_fine.interface-lo.if_errors.tx 0 1478751126";
            let expected = "totally_fine.interface-lo.if_errors.tx 0 1478751126";

            let metric = metric::Metric::new(orig, 12.0);
            let event = metric::Event::Telemetry(metric);

            let res = cs.process(event);
            assert!(res.is_ok());
            let events = res.ok().unwrap();
            assert!(!events.is_empty());
            assert_eq!(events.len(), 1);

            for event in events {
                match event {
                    metric::Event::Telemetry(m) => {
                        assert_eq!(m.name, expected);
                    }
                    _ => {
                        assert!(false);
                    }
                }
            }
        }
    }
}
