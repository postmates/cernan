use filter;
use libc::c_int;

use lua;
use lua::{Function, State, ThreadStatus};
use lua::ffi::lua_State;
use metric;
use std::path::PathBuf;
use std::sync;

struct Payload<'a> {
    metrics: Vec<Box<metric::Telemetry>>, // TODO if we switch from Box to Arc we
    // might be better off
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
    fn from_metric(m: metric::Telemetry, tags: &'a metric::TagMap, path: &'a str) -> Payload<'a> {
        Payload {
            metrics: vec![Box::new(m)],
            logs: Vec::new(),
            global_tags: tags,
            path: path,
        }
    }

    fn from_log(l: metric::LogLine, tags: &'a metric::TagMap, path: &'a str) -> Payload<'a> {
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
    unsafe extern "C" fn lua_set_metric_name(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let pyld = state.to_userdata(1) as *mut Payload;
        let idx = idx(state.to_integer(2), (*pyld).metrics.len());
        (*pyld).metrics[idx].name = state.check_string(3).into();
        0
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_push_metric(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let pyld = state.to_userdata(1) as *mut Payload;
        let val = state.to_number(3);
        match state.to_str(2) {
            Some(name) => {
                let m = metric::Telemetry::new(name, val)
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
    unsafe extern "C" fn lua_push_log(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
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
    unsafe extern "C" fn lua_metric_value(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let pyld = state.to_userdata(1) as *mut Payload;
        let idx = idx(state.to_integer(2), (*pyld).metrics.len());
        match (*pyld).metrics[idx].value() {
            Some(v) => {
                state.push_number(v);
            }
            None => {
                state.push_nil();
            }
        }
        1
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_log_tag_value(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let pyld = state.to_userdata(1) as *mut Payload;
        let idx = idx(state.to_integer(2), (*pyld).logs.len());
        match state.to_str(3).map(|k| k.to_owned()) {
            Some(key) => {
                match (*pyld).logs[idx].tags.get(&key) {
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
    unsafe extern "C" fn lua_metric_tag_value(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let pyld = state.to_userdata(1) as *mut Payload;
        let idx = idx(state.to_integer(2), (*pyld).metrics.len());
        match state.to_str(3).map(|k| k.to_owned()) {
            Some(key) => {
                match (*pyld).metrics[idx].tags.get(&key) {
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
            Some(key) => {
                match state.to_str(4).map(|v| v.to_owned()) {
                    Some(val) => {
                        match sync::Arc::make_mut(&mut (*pyld).metrics[idx].tags).insert(key, val) {
                            Some(old_v) => {
                                state.push_string(&old_v);
                            }
                            None => {
                                state.push_nil();
                            }
                        }
                    }
                    None => {
                        error!("[metric_set_tag] no key provided");
                        state.push_nil();
                    }
                }
            }
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
            Some(key) => {
                match state.to_str(4).map(|v| v.to_owned()) {
                    Some(val) => {
                        match (*pyld).logs[idx].tags.insert(key, val) {
                            Some(old_v) => {
                                state.push_string(&old_v);
                            }
                            None => {
                                state.push_nil();
                            }
                        }
                    }
                    None => {
                        error!("[log_set_tag] no key provided");
                        state.push_nil();
                    }
                }
            }
            None => {
                error!("[log_set_tag] no val provided");
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
            Some(key) => {
                match sync::Arc::make_mut(&mut (*pyld).metrics[idx].tags).remove(&key) {
                    Some(old_v) => {
                        state.push_string(&old_v);
                    }
                    None => {
                        state.push_nil();
                    }
                }
            }
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
            Some(key) => {
                match (*pyld).logs[idx].tags.remove(&key) {
                    Some(old_v) => {
                        state.push_string(&old_v);
                    }
                    None => {
                        state.push_nil();
                    }
                }
            }
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

const PAYLOAD_LIB: [(&'static str, Function); 12] =
    [("metric_name", Some(Payload::lua_metric_name)),
     ("metric_query", Some(Payload::lua_metric_query)),
     ("log_remove_tag", Some(Payload::lua_log_remove_tag)),
     ("log_set_tag", Some(Payload::lua_log_set_tag)),
     ("log_tag_value", Some(Payload::lua_log_tag_value)),
     ("metric_remove_tag", Some(Payload::lua_metric_remove_tag)),
     ("metric_set_tag", Some(Payload::lua_metric_set_tag)),
     ("metric_tag_value", Some(Payload::lua_metric_tag_value)),
     ("metric_value", Some(Payload::lua_metric_value)),
     ("push_log", Some(Payload::lua_push_log)),
     ("push_metric", Some(Payload::lua_push_metric)),
     ("set_metric_name", Some(Payload::lua_set_metric_name))];

pub struct ProgrammableFilter {
    state: lua::State,
    path: String,
    global_tags: metric::TagMap,
    last_flush_idx: u32,
}

#[derive(Debug, Clone)]
pub struct ProgrammableFilterConfig {
    pub script: PathBuf,
    pub forwards: Vec<String>,
    pub config_path: String,
    pub tags: metric::TagMap,
}

impl ProgrammableFilter {
    pub fn new(config: ProgrammableFilterConfig) -> ProgrammableFilter {
        let mut state = lua::State::new();
        state.open_libs();

        state.new_table();
        state.set_fns(&PAYLOAD_LIB, 0);
        state.set_global("payload");

        let script_path = &config.script.to_str().unwrap();
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
            other => {
                error!("unknown status: {:?}", other);
                panic!()
            }
        }

        ProgrammableFilter {
            state: state,
            path: config.config_path,
            global_tags: config.tags,
            last_flush_idx: 0,
        }
    }
}

impl filter::Filter for ProgrammableFilter {
    fn process(&mut self,
               event: metric::Event,
               res: &mut Vec<metric::Event>)
               -> Result<(), filter::FilterError> {
        match event {
            metric::Event::Telemetry(mut m) => {
                self.state.get_global("process_metric");
                if !self.state.is_fn(-1) {
                    let filter_telem = metric::Telemetry::new(format!("cernan.filter.{}.\
                                                                       process_metric.failure",
                                                                      self.path),
                                                              1.0)
                        .aggr_sum();
                    let fail = metric::Event::Telemetry(sync::Arc::new(Some(filter_telem)));
                    return Err(filter::FilterError::NoSuchFunction("process_metric", fail));
                }

                let mut pyld = Payload::from_metric(sync::Arc::make_mut(&mut m).take().unwrap(),
                                                    &self.global_tags,
                                                    self.path.as_str());
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
            metric::Event::TimerFlush(flush_idx) if self.last_flush_idx >= flush_idx => Ok(()),
            metric::Event::TimerFlush(flush_idx) => {
                self.state.get_global("tick");
                if !self.state.is_fn(-1) {
                    let fail =
                        metric::Event::new_telemetry(metric::Telemetry::new(format!("cernan.filter.\
                                                                                  {}.tick.failure",
                                                                                    self.path),
                                                                            1.0)
                            .aggr_sum());
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
            metric::Event::Log(mut l) => {
                self.state.get_global("process_log");
                if !self.state.is_fn(-1) {
                    let fail =
                        metric::Event::new_telemetry(metric::Telemetry::new(format!("cernan.filter.\
                                                                                  {}.process_log.\
                                                                                  failure",
                                                                                    self.path),
                                                                            1.0)
                            .aggr_sum());
                    return Err(filter::FilterError::NoSuchFunction("process_log", fail));
                }

                let mut pyld = Payload::from_log(sync::Arc::make_mut(&mut l).take().unwrap(),
                                                 &self.global_tags,
                                                 self.path.as_str());
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
        }
    }
}
