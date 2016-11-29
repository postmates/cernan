use filter;
use metric;
use mpsc;

use lua;
use lua::ffi::lua_State;
use lua::{State, Function, ThreadStatus};
use libc::c_int;
use std::path::PathBuf;

struct Payload {
    metric: metric::Metric,
}

impl Payload {
    fn new(m: metric::Metric) -> Payload {
        Payload { metric: m }
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_metric_name(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let point = state.to_userdata(1) as *mut Payload;
        state.push_string(&(*point).metric.name);
        1
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_set_metric_name(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let point = state.to_userdata(1) as *mut Payload;
        (*point).metric.name = state.check_string(2).into();
        0
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_metric_value(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let point = state.to_userdata(1) as *mut Payload;
        match (*point).metric.value() {
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
    unsafe extern "C" fn lua_metric_query(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let point = state.to_userdata(1) as *mut Payload;
        let prcnt = state.to_number(2);
        match (*point).metric.query(prcnt) {
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

const PAYLOAD_LIB: [(&'static str, Function); 4] =
    [("metric_name", Some(Payload::lua_metric_name)),
     ("set_metric_name", Some(Payload::lua_set_metric_name)),
     ("metric_value", Some(Payload::lua_metric_value)),
     ("metric_query", Some(Payload::lua_metric_query))];

pub struct ProgrammableFilter {
    state: lua::State,
}

#[derive(Debug, Clone)]
pub struct ProgrammableFilterConfig {
    pub script: PathBuf,
    pub forwards: Vec<String>,
    pub config_path: String,
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

        ProgrammableFilter { state: state }
    }
}

impl filter::Filter for ProgrammableFilter {
    fn process<'a>(&mut self,
                   event: &'a mut metric::Event,
                   chans: &'a mut Vec<mpsc::Sender<metric::Event>>)
                   -> Vec<(&'a mut mpsc::Sender<metric::Event>, Vec<metric::Event>)> {
        trace!("received event: {:?}", event);
        let event = event.clone();
        match event {
            metric::Event::Graphite(m) => {
                self.state.get_global("process"); // function to be called 

                let mut point = Payload::new(m); // push first argument 
                unsafe {
                    self.state.push_light_userdata::<Payload>(&mut point);
                }
                self.state.get_metatable_from_registry("payload");
                self.state.set_metatable(-2);

                self.state.call(1, 0);

                let new_event = metric::Event::Graphite(point.metric);

                let mut emitts = Vec::new();
                for chan in chans {
                    emitts.push((chan, vec![new_event.clone()]))
                }
                emitts
            }
            other => {
                let mut emitts = Vec::new();
                for chan in chans {
                    emitts.push((chan, vec![other.clone()]))
                }
                emitts
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // extern crate tempdir;

    // use super::*;
    // use filter::Filter;
    // use metric;
    // use mpsc::channel;
    // use std::path::Path;

    // #[test]
    // fn test_collectd_non_ip_extraction() {
    //     let config = ProgrammableFilterConfig {
    //         script: Path::new("/Users/briantroutwine/postmates/cernan/scripts/cernan_bridge.lua")
    //             .to_path_buf(),
    //         forwards: Vec::new(),
    //         config_path: "filters.collectd_scrub".to_string(),
    //     };
    //     let mut cs = ProgrammableFilter::new(config);

    //     let orig = "collectd.totally_fine.interface-lo.if_errors.tx 0 1478751126";
    //     let expected = "collectd.interface-lo.if_errors.tx 0 1478751126";

    //     let metric = metric::Metric::new(orig, 12.0);
    //     let mut event = metric::Event::Graphite(metric);
    //     let dir = tempdir::TempDir::new("cernan").unwrap();
    //     let (snd, _) = channel("test_non_collectd_extraction", dir.path());
    //     let mut sends = vec![snd];
    //     let filtered = cs.process(&mut event, &mut sends);

    //     assert!(!filtered.is_empty());
    //     assert_eq!(filtered.len(), 1);
    //     let ref events = filtered[0].1;
    //     for event in events {
    //         match event {
    //             &metric::Event::Graphite(ref m) => {
    //                 assert_eq!(m.name, expected);
    //             }
    //             _ => {
    //                 assert!(false);
    //             }
    //         }
    //     }
    // }

    // #[test]
    // fn test_non_collectd_extraction() {
    //     let config = ProgrammableFilterConfig {
    //         script: Path::new("/Users/briantroutwine/postmates/cernan/scripts/cernan_bridge.lua")
    //             .to_path_buf(),
    //         forwards: Vec::new(),
    //         config_path: "filters.collectd_scrub".to_string(),
    //     };
    //     let mut cs = ProgrammableFilter::new(config);

    //     let orig = "totally_fine.interface-lo.if_errors.tx 0 1478751126";
    //     let expected = "totally_fine.interface-lo.if_errors.tx 0 1478751126";

    //     let metric = metric::Metric::new(orig, 12.0);
    //     let mut event = metric::Event::Graphite(metric);
    //     let dir = tempdir::TempDir::new("cernan").unwrap();
    //     let (snd, _) = channel("test_non_collectd_extraction", dir.path());
    //     let mut sends = vec![snd];
    //     let filtered = cs.process(&mut event, &mut sends);

    //     assert!(!filtered.is_empty());
    //     assert_eq!(filtered.len(), 1);
    //     let ref events = filtered[0].1;
    //     for event in events {
    //         match event {
    //             &metric::Event::Graphite(ref m) => {
    //                 assert_eq!(m.name, expected);
    //             }
    //             _ => {
    //                 assert!(false);
    //             }
    //         }
    //     }
    // }
}
