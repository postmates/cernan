use filter;
use libc::c_int;
use metric;
use mond;
use mond::{Function, State, ThreadStatus};
use mond::ffi::lua_State;
use std::path::PathBuf;

struct Payload<'a> {
    metrics: Vec<Box<metric::Telemetry>>,
    logs: Vec<Box<metric::LogLine>>,
    global_tags: &'a metric::TagMap,
    path: &'a str,
}

fn idx(n: i64, top: usize) -> usize {
    if n < 0 {
        (top - (n.abs() as usize))
    } else {
        (n - 1) as usize
    }
}

impl<'a> Payload<'a> {
    fn from_metric(
        m: metric::Telemetry,
        tags: &'a metric::TagMap,
        path: &'a str,
    ) -> Payload<'a> {
        Payload {
            metrics: vec![Box::new(m)],
            logs: Vec::new(),
            global_tags: tags,
            path: path,
        }
    }

    fn from_log(
        l: metric::LogLine,
        tags: &'a metric::TagMap,
        path: &'a str,
    ) -> Payload<'a> {
        Payload {
            metrics: Vec::new(),
            logs: vec![Box::new(l)],
            global_tags: tags,
            path: path,
        }
    }

    fn blank(tags: &'a metric::TagMap, path: &'a str) -> Payload<'a> {
        Payload {
            metrics: Vec::new(),
            logs: Vec::new(),
            global_tags: tags,
            path: path,
        }
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_metric_name(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let pyld = state.to_userdata(1) as *mut Payload;
        let idx = idx(state.to_integer(2), (*pyld).metrics.len());
        state.push_string(&(*pyld).metrics[idx].name);
        1
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_log_value(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let pyld = state.to_userdata(1) as *mut Payload;
        let idx = idx(state.to_integer(2), (*pyld).logs.len());
        state.push_string(&(*pyld).logs[idx].value);
        1
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_log_path(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let pyld = state.to_userdata(1) as *mut Payload;
        let idx = idx(state.to_integer(2), (*pyld).logs.len());
        state.push_string(&(*pyld).logs[idx].path);
        1
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_set_metric_name(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let pyld = state.to_userdata(1) as *mut Payload;
        let idx = idx(state.to_integer(2), (*pyld).metrics.len());
        (*pyld).metrics[idx].name = state.check_string(3).into();
        0
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_push_metric(L: *mut lua_State) -> c_int {
        let state = State::from_ptr(L);
        let pyld = state.to_userdata(1) as *mut Payload;
        let val = state.to_number(3);
        match state.to_str(2) {
            Some(name) => {
                let m = metric::Telemetry::new()
                    .name(name)
                    .value(val)
                    .harden()
                    .unwrap()
                    .overlay_tags_from_map((*pyld).global_tags);
                (*pyld).metrics.push(Box::new(m));
            }
            None => {
                error!("[push_metric] no name argument given");
            }
        }
        0
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_clear_metrics(L: *mut lua_State) -> c_int {
        let state = State::from_ptr(L);
        let pyld = state.to_userdata(1) as *mut Payload;
        (*pyld).metrics.clear();
        0
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_push_log(L: *mut lua_State) -> c_int {
        let state = State::from_ptr(L);
        let pyld = state.to_userdata(1) as *mut Payload;
        match state.to_str(2) {
            Some(line) => {
                let l = metric::LogLine::new((*pyld).path, line)
                    .overlay_tags_from_map((*pyld).global_tags);
                (*pyld).logs.push(Box::new(l));
            }
            None => {
                error!("[push_log] no line argument given");
            }
        };
        0
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_clear_logs(L: *mut lua_State) -> c_int {
        let state = State::from_ptr(L);
        let pyld = state.to_userdata(1) as *mut Payload;
        (*pyld).logs.clear();
        0
    }

    // #[allow(non_snake_case)]
    // unsafe extern "C" fn lua_metric_value(L: *mut lua_State) -> c_int {
    //     let mut state = State::from_ptr(L);
    //     let pyld = state.to_userdata(1) as *mut Payload;
    //     let idx = idx(state.to_integer(2), (*pyld).metrics.len());
    //     match (*pyld).metrics[idx].value() {
    //         Some(v) => {
    //             state.push_number(v);
    //         }
    //         None => {
    //             state.push_nil();
    //         }
    //     }
    //     1
    // }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_log_tag_value(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let pyld = state.to_userdata(1) as *mut Payload;
        let idx = idx(state.to_integer(2), (*pyld).logs.len());
        match state.to_str(3).map(|k| k.to_owned()) {
            Some(key) => match (*pyld).logs[idx].tags.get(&key) {
                Some(v) => {
                    state.push_string(v);
                }
                None => {
                    state.push_nil();
                }
            },
            None => {
                error!("[log_tag_value] no key provided");
                state.push_nil();
            }
        }
        1
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_log_field_value(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let pyld = state.to_userdata(1) as *mut Payload;
        let idx = idx(state.to_integer(2), (*pyld).logs.len());
        match state.to_str(3).map(|k| k.to_owned()) {
            Some(key) => match (*pyld).logs[idx].fields.get(&key) {
                Some(v) => {
                    state.push_string(v);
                }
                None => {
                    state.push_nil();
                }
            },
            None => {
                error!("[log_tag_value] no key provided");
                state.push_nil();
            }
        }
        1
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_metric_tag_value(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let pyld = state.to_userdata(1) as *mut Payload;
        let idx = idx(state.to_integer(2), (*pyld).metrics.len());
        match state.to_str(3).map(|k| k.to_owned()) {
            Some(key) => {
                match (*pyld).metrics[idx].get_from_tags(&key, (*pyld).global_tags) {
                    Some(v) => {
                        state.push_string(v);
                    }
                    None => {
                        state.push_nil();
                    }
                }
            }
            None => {
                error!("[log_tag_value] no key provided");
                state.push_nil();
            }
        }
        1
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_metric_set_tag(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let pyld = state.to_userdata(1) as *mut Payload;
        let idx = idx(state.to_integer(2), (*pyld).metrics.len());
        match state.to_str(3).map(|k| k.to_owned()) {
            Some(key) => match state.to_str(4).map(|v| v.to_owned()) {
                Some(val) => match (*pyld).metrics[idx].insert_tag(key, val) {
                    Some(old_v) => {
                        state.push_string(&old_v);
                    }
                    None => {
                        state.push_nil();
                    }
                },
                None => {
                    error!("[metric_set_tag] no key provided");
                    state.push_nil();
                }
            },
            None => {
                error!("[metric_set_tag] no val provided");
                state.push_nil();
            }
        }
        1
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_log_set_tag(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let pyld = state.to_userdata(1) as *mut Payload;
        let idx = idx(state.to_integer(2), (*pyld).logs.len());
        match state.to_str(3).map(|k| k.to_owned()) {
            Some(key) => match state.to_str(4).map(|v| v.to_owned()) {
                Some(val) => match (*pyld).logs[idx].tags.insert(key, val) {
                    Some(old_v) => {
                        state.push_string(&old_v);
                    }
                    None => {
                        state.push_nil();
                    }
                },
                None => {
                    error!("[log_set_tag] no key provided");
                    state.push_nil();
                }
            },
            None => {
                error!("[log_set_tag] no val provided");
                state.push_nil();
            }
        }
        1
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_log_set_field(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let pyld = state.to_userdata(1) as *mut Payload;
        let idx = idx(state.to_integer(2), (*pyld).logs.len());
        match state.to_str(3).map(|k| k.to_owned()) {
            Some(key) => match state.to_str(4).map(|v| v.to_owned()) {
                Some(val) => match (*pyld).logs[idx].fields.insert(key, val) {
                    Some(old_v) => {
                        state.push_string(&old_v);
                    }
                    None => {
                        state.push_nil();
                    }
                },
                None => {
                    error!("[log_set_field] no key provided");
                    state.push_nil();
                }
            },
            None => {
                error!("[log_set_field] no val provided");
                state.push_nil();
            }
        }
        1
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_metric_remove_tag(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let pyld = state.to_userdata(1) as *mut Payload;
        let idx = idx(state.to_integer(2), (*pyld).metrics.len());
        match state.to_str(3).map(|k| k.to_owned()) {
            Some(key) => match (*pyld).metrics[idx].remove_tag(&key) {
                Some(old_v) => {
                    state.push_string(&old_v);
                }
                None => {
                    state.push_nil();
                }
            },
            None => {
                error!("[metric_remove_tag] no val provided");
                state.push_nil();
            }
        }
        1
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_log_remove_tag(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let pyld = state.to_userdata(1) as *mut Payload;
        let idx = idx(state.to_integer(2), (*pyld).logs.len());
        match state.to_str(3).map(|k| k.to_owned()) {
            Some(key) => match (*pyld).logs[idx].tags.remove(&key) {
                Some(old_v) => {
                    state.push_string(&old_v);
                }
                None => {
                    state.push_nil();
                }
            },
            None => {
                error!("[log_remove_tag] no val provided");
                state.push_nil();
            }
        }
        1
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_metric_query(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let pyld = state.to_userdata(1) as *mut Payload;
        let prcnt = state.to_number(2);
        let idx = idx(state.to_integer(2), (*pyld).metrics.len());
        match (*pyld).metrics[idx].query(prcnt) {
            Some(v) => {
                state.push_number(v);
            }
            None => {
                state.push_nil();
            }
        }
        1
    }
}

const PAYLOAD_LIB: [(&str, Function); 17] = [
    ("set_metric_name", Some(Payload::lua_set_metric_name)),
    ("clear_logs", Some(Payload::lua_clear_logs)),
    ("clear_metrics", Some(Payload::lua_clear_metrics)),
    ("log_remove_tag", Some(Payload::lua_log_remove_tag)),
    ("log_set_tag", Some(Payload::lua_log_set_tag)),
    ("log_tag_value", Some(Payload::lua_log_tag_value)),
    ("log_set_field", Some(Payload::lua_log_set_field)),
    ("log_field_value", Some(Payload::lua_log_field_value)),
    ("log_value", Some(Payload::lua_log_value)),
    ("log_path", Some(Payload::lua_log_path)),
    ("metric_query", Some(Payload::lua_metric_query)),
    ("metric_remove_tag", Some(Payload::lua_metric_remove_tag)),
    ("metric_set_tag", Some(Payload::lua_metric_set_tag)),
    ("metric_tag_value", Some(Payload::lua_metric_tag_value)),
    // TODO
    //
    // The single 'value' for a Telemetry doesn't make sense and never did make
    // sense. We were just bad people and pretended that it did. What I'm
    // thinking we'll do is expose metric_value_sum(), metric_value_set() etc to
    // cover the needs of folks and remove metric_value.
    //
    // I haven't done any of that work yet.
    // ("metric_value", Some(Payload::lua_metric_value)),
    ("push_log", Some(Payload::lua_push_log)),
    ("push_metric", Some(Payload::lua_push_metric)),
    ("metric_name", Some(Payload::lua_metric_name)),
];

/// A filter programmable by end-users, in Lua.
///
/// The programmable filter is a general purpose filter that can be programmed
/// by end-users with a lua script.
pub struct ProgrammableFilter {
    state: mond::State,
    path: String,
    global_tags: metric::TagMap,
    last_flush_idx: u64,
}

/// Configuration for `ProgrammableFilter`.
#[derive(Debug, Deserialize, Clone)]
pub struct ProgrammableFilterConfig {
    /// Path on-disk for cernan to find scripts and script supporting libraries.
    pub scripts_directory: Option<PathBuf>,
    /// The script to load as a filter.
    pub script: Option<PathBuf>,
    /// The forwards to emit `metric::Event` stream into
    pub forwards: Vec<String>,
    /// The unique name of the filter in the routing topology.
    pub config_path: Option<String>,
    /// The tags that the filter may overlay on all its input `metric::Event`s.
    pub tags: metric::TagMap,
}

impl Default for ProgrammableFilterConfig {
    fn default() -> Self {
        ProgrammableFilterConfig {
            scripts_directory: None,
            script: None,
            forwards: Vec::default(),
            config_path: None,
            tags: metric::TagMap::default(),
        }
    }
}

impl ProgrammableFilter {
    /// Create a new ProgrammableFilter
    pub fn new(config: ProgrammableFilterConfig) -> ProgrammableFilter {
        let mut state = mond::State::new();
        state.open_libs();

        state.get_global("package");
        let mut path = String::new();
        path.push_str(
            config
                .scripts_directory
                .expect("must have a specified scripts_directory")
                .to_str()
                .expect("must have valid unicode scripts_directory"),
        );
        path.push_str("/?.lua");
        state.push_string(&path);
        state.set_field(-2, "path");
        state.pop(1);

        state.new_table();
        state.set_fns(&PAYLOAD_LIB, 0);
        state.set_global("payload");

        let script = &config.script.expect("must have a specified scripts config");
        let script_path = script.to_str().unwrap();
        match state.load_file(script_path) {
            ThreadStatus::Ok => trace!("was able to load script at {}", script_path),
            ThreadStatus::SyntaxError => {
                error!("syntax error in script at {}", script_path);
                panic!()
            }
            other => {
                error!("unknown status: {:?}", other);
                panic!()
            }
        }
        match state.pcall(0, 0, 0) {
            ThreadStatus::Ok => trace!("was able to load script at {}", script_path),
            ThreadStatus::SyntaxError => {
                error!("syntax error in script at {}", script_path);
                panic!()
            }
            ThreadStatus::RuntimeError => {
                error!("encountered a runtime error");
                println!("encountered a runtime error");
                panic!()
            }
            other => {
                error!("unknown status: {:?}", other);
                println!("unknown status: {:?}", other);
                panic!()
            }
        }

        ProgrammableFilter {
            state: state,
            path: config
                .config_path
                .expect("must have a config_path for ProgrammableFilter"),
            global_tags: config.tags,
            last_flush_idx: 0,
        }
    }
}

impl filter::Filter for ProgrammableFilter {
    fn process(
        &mut self,
        event: metric::Event,
        res: &mut Vec<metric::Event>,
    ) -> Result<(), filter::FilterError> {
        match event {
            metric::Event::Telemetry(m) => {
                self.state.get_global("process_metric");
                if !self.state.is_fn(-1) {
                    let filter_telem = metric::Telemetry::new()
                        .name(format!(
                            "cernan.filter.{}.\
                             process_metric.failure",
                            self.path
                        ))
                        .value(1.0)
                        .kind(metric::AggregationMethod::Sum)
                        .harden()
                        .unwrap();
                    let fail = metric::Event::Telemetry(filter_telem);
                    return Err(filter::FilterError::NoSuchFunction(
                        "process_metric",
                        fail,
                    ));
                }

                let mut pyld =
                    Payload::from_metric(m, &self.global_tags, self.path.as_str());
                unsafe {
                    self.state.push_light_userdata::<Payload>(&mut pyld);
                }
                self.state.get_metatable_from_registry("payload");
                self.state.set_metatable(-2);

                self.state.call(1, 0);

                for lg in pyld.logs {
                    res.push(metric::Event::new_log(*lg));
                }
                for mt in pyld.metrics {
                    res.push(metric::Event::new_telemetry(*mt));
                }
                Ok(())
            }
            metric::Event::TimerFlush(flush_idx)
                if self.last_flush_idx >= flush_idx =>
            {
                Ok(())
            }
            metric::Event::TimerFlush(flush_idx) => {
                self.state.get_global("tick");
                if !self.state.is_fn(-1) {
                    let fail = metric::Event::new_telemetry(
                        metric::Telemetry::new()
                            .name(format!(
                                "cernan.filter.\
                                 {}.tick.failure",
                                self.path
                            ))
                            .value(1.0)
                            .kind(metric::AggregationMethod::Sum)
                            .harden()
                            .unwrap(),
                    );
                    return Err(filter::FilterError::NoSuchFunction("tick", fail));
                }

                let mut pyld = Payload::blank(&self.global_tags, self.path.as_str());
                unsafe {
                    self.state.push_light_userdata::<Payload>(&mut pyld);
                }
                self.state.get_metatable_from_registry("payload");
                self.state.set_metatable(-2);

                self.state.call(1, 0);

                for lg in pyld.logs {
                    res.push(metric::Event::new_log(*lg));
                }
                for mt in pyld.metrics {
                    res.push(metric::Event::new_telemetry(*mt));
                }
                res.push(event);
                self.last_flush_idx = flush_idx;
                Ok(())
            }
            metric::Event::Log(l) => {
                self.state.get_global("process_log");
                if !self.state.is_fn(-1) {
                    let fail = metric::Event::new_telemetry(
                        metric::Telemetry::new()
                            .name(format!(
                                "cernan.filter.\
                                 {}.process_log.\
                                 failure",
                                self.path
                            ))
                            .value(1.0)
                            .kind(metric::AggregationMethod::Sum)
                            .harden()
                            .unwrap(),
                    );
                    return Err(filter::FilterError::NoSuchFunction(
                        "process_log",
                        fail,
                    ));
                }

                let mut pyld =
                    Payload::from_log(l, &self.global_tags, self.path.as_str());
                unsafe {
                    self.state.push_light_userdata::<Payload>(&mut pyld);
                }
                self.state.get_metatable_from_registry("payload");
                self.state.set_metatable(-2);

                self.state.call(1, 0);

                for lg in pyld.logs {
                    res.push(metric::Event::new_log(*lg));
                }
                for mt in pyld.metrics {
                    res.push(metric::Event::new_telemetry(*mt));
                }
                Ok(())
            }
            raw @ metric::Event::Raw { .. } => {
                res.push(raw);
                Ok(())
            }
            metric::Event::Shutdown => {
                res.push(metric::Event::Shutdown);
                Ok(())
            }
        }
    }
}
