mod integration {
    mod programmable_filter {

        extern crate cernan;

        use self::cernan::filter::{Filter, ProgrammableFilter,
                                   ProgrammableFilterConfig};
        use self::cernan::metric;
        use self::cernan::metric::AggregationMethod;
        use std::path::PathBuf;

        #[test]
        fn test_id_filter() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts");
            let script_dir = script.clone();
            script.push("identity.lua");

            let config = ProgrammableFilterConfig {
                scripts_directory: Some(script_dir),
                script: Some(script),
                forwards: Vec::new(),
                config_path: Some("filters.identity".to_string()),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let metric = metric::Telemetry::new()
                .name("identity")
                .value(12.0)
                .kind(metric::AggregationMethod::Set)
                .harden()
                .unwrap()
                .overlay_tag("foo", "bar")
                .overlay_tag("bizz", "bazz");
            let event = metric::Event::new_telemetry(metric);

            let mut events = Vec::new();
            let res = cs.process(event.clone(), &mut events);
            assert!(res.is_ok());
            assert!(!events.is_empty());
            assert_eq!(events.len(), 1);
            assert_eq!(events[0], event);
        }

        #[test]
        fn test_clear_metrics_filter() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts");
            let script_dir = script.clone();
            script.push("clear_metrics.lua");

            let config = ProgrammableFilterConfig {
                scripts_directory: Some(script_dir),
                script: Some(script),
                forwards: Vec::new(),
                config_path: Some("filters.clear_metrics".to_string()),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let metric = metric::Telemetry::new()
                .name("clear_me")
                .value(12.0)
                .kind(metric::AggregationMethod::Set)
                .harden()
                .unwrap()
                .overlay_tag("foo", "bar")
                .overlay_tag("bizz", "bazz");
            let event = metric::Event::new_telemetry(metric);

            let mut events: Vec<metric::Event> = Vec::new();
            let res = cs.process(event.clone(), &mut events);
            assert!(res.is_ok());
            assert!(events.is_empty());
        }

        #[test]
        fn test_clear_logs_filter() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts");
            let script_dir = script.clone();
            script.push("clear_logs.lua");

            let config = ProgrammableFilterConfig {
                scripts_directory: Some(script_dir),
                script: Some(script),
                forwards: Vec::new(),
                config_path: Some("filters.clear_logs".to_string()),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let log = metric::LogLine::new(
                "clear_me",
                "i am the very model of the modern major general",
            ).overlay_tag("foo", "bar")
                .overlay_tag("bizz", "bazz");
            let event = metric::Event::new_log(log);

            let mut events: Vec<metric::Event> = Vec::new();
            let res = cs.process(event.clone(), &mut events);
            assert!(res.is_ok());
            assert!(events.is_empty());
        }

        #[test]
        fn test_remove_log_tag_kv() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts");
            let script_dir = script.clone();
            script.push("remove_keys.lua");

            let config = ProgrammableFilterConfig {
                scripts_directory: Some(script_dir),
                script: Some(script),
                forwards: Vec::new(),
                config_path: Some("filters.remove_keys".to_string()),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let orig_log = metric::LogLine::new(
                "identity",
                "i am the very model of the modern major general",
            ).overlay_tag("foo", "bar")
                .overlay_tag("bizz", "bazz");
            let expected_log = metric::LogLine::new(
                "identity",
                "i am the very model of the modern major \
                 general",
            ).overlay_tag("foo", "bar");
            let orig_event = metric::Event::new_log(orig_log);
            let expected_event = metric::Event::new_log(expected_log);

            let mut events = Vec::new();
            let res = cs.process(orig_event.clone(), &mut events);
            assert!(res.is_ok());
            assert!(!events.is_empty());
            assert_eq!(events.len(), 1);
            assert_eq!(events[0], expected_event);
        }

        #[test]
        fn test_get_log_path() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts");
            let script_dir = script.clone();
            script.push("field_from_path.lua");

            let config = ProgrammableFilterConfig {
                scripts_directory: Some(script_dir),
                script: Some(script),
                forwards: Vec::new(),
                config_path: Some("filters.field_from_path".to_string()),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let orig_log = metric::LogLine::new(
                "identity",
                "i am the very model of the modern major general",
            );
            let expected_log = metric::LogLine::new(
                "identity",
                "i am the very model of the modern major \
                 general",
            ).insert_field("foo", "identity");
            let orig_event = metric::Event::new_log(orig_log);
            let expected_event = metric::Event::new_log(expected_log);

            let mut events = Vec::new();
            let res = cs.process(orig_event.clone(), &mut events);
            assert!(res.is_ok());
            assert!(!events.is_empty());
            assert_eq!(events.len(), 1);
            assert_eq!(events[0], expected_event);
        }

        #[test]
        fn test_set_log_value() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts");
            let script_dir = script.clone();
            script.push("set_value.lua");

            let config = ProgrammableFilterConfig {
                scripts_directory: Some(script_dir),
                script: Some(script),
                forwards: Vec::new(),
                config_path: Some("filters.set_value".to_string()),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let orig_log = metric::LogLine::new(
                "identity",
                "i am the very model of the modern major general",
            );
            let expected_log = metric::LogLine::new(
                "identity",
                "foo"
            );
            let orig_event = metric::Event::new_log(orig_log);
            let expected_event = metric::Event::new_log(expected_log);

            let mut events = Vec::new();
            let res = cs.process(orig_event.clone(), &mut events);
            assert!(res.is_ok());
            assert!(!events.is_empty());
            assert_eq!(events.len(), 1);
            assert_eq!(events[0], expected_event);
        }

        #[test]
        fn test_remove_metric_tag_kv() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts");
            let script_dir = script.clone();
            script.push("remove_keys.lua");

            let config = ProgrammableFilterConfig {
                scripts_directory: Some(script_dir),
                script: Some(script),
                forwards: Vec::new(),
                config_path: Some("filters.remove_keys".to_string()),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let expected_metric = metric::Telemetry::new()
                .name("identity")
                .kind(metric::AggregationMethod::Set)
                .value(12.0)
                .harden()
                .unwrap()
                .overlay_tag("foo", "bar");
            let orig_metric = expected_metric.clone().overlay_tag("bizz", "bazz");
            let orig_event = metric::Event::new_telemetry(orig_metric);
            let expected_event = metric::Event::new_telemetry(expected_metric);

            let mut events = Vec::new();
            let res = cs.process(orig_event.clone(), &mut events);
            assert!(res.is_ok());
            assert!(!events.is_empty());
            assert_eq!(events.len(), 1);
            assert_eq!(events[0], expected_event);
        }

        #[test]
        fn test_insufficient_args_no_crash() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts");
            let script_dir = script.clone();
            script.push("insufficient_args.lua");

            let config = ProgrammableFilterConfig {
                scripts_directory: Some(script_dir),
                script: Some(script),
                forwards: Vec::new(),
                config_path: Some("filters.no_args_no_crash".to_string()),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let orig_metric = metric::Telemetry::new()
                .name("identity")
                .value(12.0)
                .kind(metric::AggregationMethod::Set)
                .harden()
                .unwrap()
                .overlay_tag("foo", "bar")
                .overlay_tag("bizz", "bazz");

            let orig_event = metric::Event::new_telemetry(orig_metric);

            let mut events = Vec::new();
            let res = cs.process(orig_event.clone(), &mut events);
            assert!(res.is_ok());
            assert!(!events.is_empty());
            assert_eq!(events.len(), 1);
            assert_eq!(events[0], orig_event);
        }

        #[test]
        fn test_missing_func() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts");
            let script_dir = script.clone();
            script.push("missing_func.lua");

            let config = ProgrammableFilterConfig {
                scripts_directory: Some(script_dir),
                script: Some(script),
                forwards: Vec::new(),
                config_path: Some("filters.missing_func".to_string()),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let orig_metric = metric::Telemetry::new()
                .name("identity")
                .value(12.0)
                .kind(metric::AggregationMethod::Set)
                .harden()
                .unwrap()
                .overlay_tag("foo", "bar")
                .overlay_tag("bizz", "bazz");

            let orig_event = metric::Event::new_telemetry(orig_metric);

            let mut events = Vec::new();
            let res = cs.process(orig_event, &mut events);
            assert!(res.is_err());
            assert!(events.is_empty());
        }

        #[test]
        fn test_lua_error() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts");
            let script_dir = script.clone();
            script.push("lua_error.lua");

            let config = ProgrammableFilterConfig {
                scripts_directory: Some(script_dir),
                script: Some(script),
                forwards: Vec::new(),
                config_path: Some("filters.lua_error".to_string()),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let orig_metric = metric::Telemetry::new()
                .name("identity")
                .value(12.0)
                .kind(metric::AggregationMethod::Set)
                .harden()
                .unwrap()
                .overlay_tag("foo", "bar")
                .overlay_tag("bizz", "bazz");

            let orig_event = metric::Event::new_telemetry(orig_metric);

            let mut events = Vec::new();
            let res = cs.process(orig_event, &mut events);
            assert!(res.is_err());
            assert!(events.is_empty());
        }

        #[test]
        fn test_demo_require() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts");
            let script_dir = script.clone();
            script.push("demonstrate_require.lua");

            let config = ProgrammableFilterConfig {
                scripts_directory: Some(script_dir),
                script: Some(script),
                forwards: Vec::new(),
                config_path: Some("filters.demonstrate_require".to_string()),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let expected_log = metric::LogLine::new(
                "identity",
                "i am the very model of the modern major \
                 general",
            ).overlay_tag("foo", "bar")
                .overlay_tag("bizz", "bazz");
            let orig_log = metric::LogLine::new(
                "identity",
                "i am the very model of the modern major general",
            ).overlay_tag("foo", "bar");
            let orig_event = metric::Event::new_log(orig_log);
            let expected_event = metric::Event::new_log(expected_log);

            let mut events = Vec::new();
            let res = cs.process(orig_event, &mut events);
            assert!(res.is_ok());
            assert!(!events.is_empty());
            assert_eq!(events.len(), 1);
            assert_eq!(events[0], expected_event);
        }

        #[test]
        fn test_add_log_tag_kv() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts");
            let script_dir = script.clone();
            script.push("add_keys.lua");

            let config = ProgrammableFilterConfig {
                scripts_directory: Some(script_dir),
                script: Some(script),
                forwards: Vec::new(),
                config_path: Some("filters.add_keys".to_string()),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let expected_log = metric::LogLine::new(
                "identity",
                "i am the very model of the modern major \
                 general",
            ).overlay_tag("foo", "bar")
                .overlay_tag("bizz", "bazz");
            let orig_log = metric::LogLine::new(
                "identity",
                "i am the very model of the modern major general",
            ).overlay_tag("foo", "bar");
            let orig_event = metric::Event::new_log(orig_log);
            let expected_event = metric::Event::new_log(expected_log);

            let mut events = Vec::new();
            let res = cs.process(orig_event, &mut events);
            assert!(res.is_ok());
            assert!(!events.is_empty());
            assert_eq!(events.len(), 1);
            assert_eq!(events[0], expected_event);
        }

        #[test]
        fn test_add_metric_tag_kv() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts");
            let script_dir = script.clone();
            script.push("add_keys.lua");

            let config = ProgrammableFilterConfig {
                scripts_directory: Some(script_dir),
                script: Some(script),
                forwards: Vec::new(),
                config_path: Some("filters.add_keys".to_string()),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let orig_metric = metric::Telemetry::new()
                .name("identity")
                .value(12.0)
                .kind(metric::AggregationMethod::Set)
                .harden()
                .unwrap()
                .overlay_tag("foo", "bar");
            let expected_metric = orig_metric.clone().overlay_tag("bizz", "bazz");
            let orig_event = metric::Event::new_telemetry(orig_metric);
            let expected_event = metric::Event::new_telemetry(expected_metric);

            let mut events = Vec::new();
            let res = cs.process(orig_event, &mut events);
            assert!(res.is_ok());
            assert!(!events.is_empty());
            assert_eq!(events.len(), 1);
            assert_eq!(events[0], expected_event);
        }

        #[test]
        fn test_tick_keeps_counts() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts");
            let script_dir = script.clone();
            script.push("keep_count.lua");

            let config = ProgrammableFilterConfig {
                scripts_directory: Some(script_dir),
                script: Some(script),
                forwards: Vec::new(),
                config_path: Some("filters.keep_count".to_string()),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let metric0 = metric::Event::new_telemetry(
                metric::Telemetry::new()
                    .name("identity")
                    .value(12.0)
                    .kind(metric::AggregationMethod::Set)
                    .harden()
                    .unwrap(),
            );
            let metric1 = metric::Event::new_telemetry(
                metric::Telemetry::new()
                    .name("identity")
                    .value(13.0)
                    .kind(metric::AggregationMethod::Set)
                    .harden()
                    .unwrap(),
            );
            let metric2 = metric::Event::new_telemetry(
                metric::Telemetry::new()
                    .name("identity")
                    .value(14.0)
                    .kind(metric::AggregationMethod::Set)
                    .harden()
                    .unwrap(),
            );

            let log0 =
                metric::Event::new_log(metric::LogLine::new("identity", "a log line"));
            let log1 =
                metric::Event::new_log(metric::LogLine::new("identity", "another"));
            let log2 =
                metric::Event::new_log(metric::LogLine::new("identity", "more"));
            let log3 =
                metric::Event::new_log(metric::LogLine::new("identity", "less"));

            let flush1 = metric::Event::TimerFlush(1);
            let flush2 = metric::Event::TimerFlush(2);

            let mut events = Vec::new();
            for ev in &[metric0, metric1, metric2, log0, log1] {
                let _ = cs.process(ev.clone(), &mut events);
            }
            events.clear();
            let res = cs.process(flush1, &mut events);
            assert!(res.is_ok());

            assert!(!events.is_empty());
            assert_eq!(events.len(), 3);
            println!("EVENTS: {:?}", events);
            assert_eq!(events[2], metric::Event::TimerFlush(1));
            match events[1] {
                metric::Event::Telemetry(ref mut p) => {
                    assert_eq!(p.name, "count_per_tick");
                    assert_eq!(p.set(), Some(5.0));
                }
                _ => {
                    assert!(false);
                }
            }
            assert_eq!(
                events[0],
                metric::Event::new_log(metric::LogLine::new(
                    "filters.keep_count",
                    "count_per_tick: 5"
                ))
            );

            events.clear();
            for ev in &[log2, log3] {
                let _ = cs.process(ev.clone(), &mut events);
            }
            events.clear();
            let res = cs.process(flush2, &mut events);
            assert!(res.is_ok());

            assert!(!events.is_empty());
            assert_eq!(events.len(), 3);
            println!("EVENTS: {:?}", events);
            assert_eq!(events[2], metric::Event::TimerFlush(2));
            match events[1] {
                metric::Event::Telemetry(ref mut p) => {
                    assert_eq!(p.name, "count_per_tick");
                    assert_eq!(p.set(), Some(2.0));
                }
                _ => {
                    assert!(false);
                }
            }
            assert_eq!(
                events[0],
                metric::Event::new_log(metric::LogLine::new(
                    "filters.keep_count",
                    "count_per_tick: 2"
                ))
            );
        }

        #[test]
        fn test_collectd_non_ip_extraction() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts");
            let script_dir = script.clone();
            script.push("collectd_scrub.lua");

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
            let expected =
                "collectd.protocols-TcpExt.protocol_counter-TCPFastOpenActive";

            let metric = metric::Telemetry::new()
                .name(orig)
                .value(12.0)
                .kind(metric::AggregationMethod::Set)
                .harden()
                .unwrap();
            let event = metric::Event::new_telemetry(metric);

            let mut events = Vec::new();
            let res = cs.process(event, &mut events);
            assert!(res.is_ok());
            assert!(!events.is_empty());
            assert_eq!(events.len(), 1);

            for event in events {
                match event {
                    metric::Event::Telemetry(mut met) => {
                        assert_eq!(met.name, expected);
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
            script.push("resources/tests/scripts");
            let script_dir = script.clone();
            script.push("collectd_scrub.lua");

            let config = ProgrammableFilterConfig {
                scripts_directory: Some(script_dir),
                script: Some(script),
                forwards: Vec::new(),
                config_path: Some("filters.collectd_scrub".to_string()),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let orig = "totally_fine.interface-lo.if_errors.tx 0 1478751126";
            let expected = "totally_fine.interface-lo.if_errors.tx 0 1478751126";

            let metric = metric::Telemetry::new()
                .name(orig)
                .value(12.0)
                .kind(AggregationMethod::Set)
                .harden()
                .unwrap();
            let event = metric::Event::new_telemetry(metric);

            let mut events = Vec::new();
            let res = cs.process(event, &mut events);
            assert!(res.is_ok());
            assert!(!events.is_empty());
            assert_eq!(events.len(), 1);

            for event in events {
                match event {
                    metric::Event::Telemetry(mut met) => {
                        assert_eq!(met.name, expected);
                    }
                    _ => {
                        assert!(false);
                    }
                }
            }
        }

        #[test]
        fn test_parse_json_add_fields() {
            let mut script = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            script.push("resources/tests/scripts");
            let script_dir = script.clone();
            script.push("json_parse.lua");

            let config = ProgrammableFilterConfig {
                scripts_directory: Some(script_dir),
                script: Some(script),
                forwards: Vec::new(),
                config_path: Some("filters.json_parse".to_string()),
                tags: Default::default(),
            };
            let mut cs = ProgrammableFilter::new(config);

            let expected_log = metric::LogLine::new("identity", "{\"foo\": \"bar\"}")
                .insert_field("foo", "bar");
            let orig_log = metric::LogLine::new("identity", "{\"foo\": \"bar\"}");

            let orig_event = metric::Event::new_log(orig_log);
            let expected_event = metric::Event::new_log(expected_log);

            let mut events = Vec::new();
            let res = cs.process(orig_event, &mut events);
            assert!(res.is_ok());
            assert!(!events.is_empty());
            assert_eq!(events.len(), 1);
            assert_eq!(events[0], expected_event);
        }

    }
}
