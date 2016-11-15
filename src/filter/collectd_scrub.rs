use filter;
use metric;
use mpsc;
use regex;

use lua;
use lua::ffi::lua_State;
use lua::{State, Function};
use libc::c_int;

struct Point2D {
    // using i64 for convenience since lua defaults to 64 bit integers
    x: i64,
    y: i64,
    metric: metric::Metric,
}

impl Point2D {
    fn new(x: i64, y: i64, m: metric::Metric) -> Point2D {
        return Point2D {
            x: x,
            y: y,
            metric: m,
        };
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_x(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let point = state.to_userdata(1) as *mut Point2D;
        state.push_integer((*point).x);
        1
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_set_x(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let point = state.to_userdata(1) as *mut Point2D;
        let new_x_val = state.check_integer(2);
        (*point).x = new_x_val;
        0
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_metric_name(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let point = state.to_userdata(1) as *mut Point2D;
        state.push_string(&(*point).metric.name);
        1
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_set_metric_name(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let point = state.to_userdata(1) as *mut Point2D;
        let new_name = state.check_string(2).clone();
        (*point).metric.name = new_name.into();
        0
    }

    #[allow(non_snake_case)]
    unsafe extern "C" fn lua_metric_value(L: *mut lua_State) -> c_int {
        let mut state = State::from_ptr(L);
        let point = state.to_userdata(1) as *mut Point2D;
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
        let point = state.to_userdata(1) as *mut Point2D;
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

const POINT2D_LIB: [(&'static str, Function); 6] =
    [("x", Some(Point2D::lua_x)),
     ("set_x", Some(Point2D::lua_set_x)),
     ("metric_name", Some(Point2D::lua_metric_name)),
     ("set_metric_name", Some(Point2D::lua_set_metric_name)),
     ("metric_value", Some(Point2D::lua_metric_value)),
     ("metric_query", Some(Point2D::lua_metric_query))];

pub struct CollectdScrub {
    scrub_pattern: regex::Regex,
}

impl CollectdScrub {
    pub fn extract(&self, orig: &str) -> Option<String> {
        self.scrub_pattern
            .captures(orig.into())
            .map(|caps| format!("{}{}", caps.at(1).unwrap(), caps.at(3).unwrap()))
    }
}

// local s = "collectd.totally_fine.interface-lo.if_errors.tx"

// print(string.format("%s%s", string.match(s, "^(collectd)[%.@][%w_]+(.*)")))

impl filter::Filter for CollectdScrub {
    fn new<S>(_name: S) -> CollectdScrub
        where S: Into<String>
    {
        // inspiration http://marek.vavrusa.com/2015/08/03/embedding-luajit/
        let mut state = lua::State::new();
        state.open_libs();

        state.new_table();
        state.set_fns(&POINT2D_LIB, 0);
        state.set_global("point2d");

        let prog = r#"function fact(n) print(point2d.set_metric_name(n, "foooorts!")) end"#;
        println!("{:?}", state.load_string(prog));
        state.pcall(0, 0, 0);

        state.get_global("fact");
        let mut point = Point2D::new(1, 2, metric::Metric::new("foo", 12.0));
        unsafe {
            state.push_light_userdata::<Point2D>(&mut point);
        }
        state.get_metatable_from_registry("point2d");
        state.set_metatable(-2);

        state.call(1, 1);
        println!("NAME: {}", point.metric.name);
        state.close();

        CollectdScrub {
            scrub_pattern: regex::Regex::new(r"^(collectd)[@|.]([[:alnum:]_-]+)(.*)").unwrap(),
        }
    }

    fn process<'a>(&mut self,
                   event: &'a mut metric::Event,
                   chans: &'a mut Vec<mpsc::Sender<metric::Event>>)
                   -> Vec<(&'a mut mpsc::Sender<metric::Event>, Vec<metric::Event>)> {
        trace!("received event: {:?}", event);
        let event = event.clone();
        match event {
            metric::Event::Graphite(mut m) => {
                if let Some(new_name) = self.extract(&m.name) {
                    m.name = new_name;
                }
                debug!("adjusted name: {}", m.name);
                let new_event = metric::Event::Graphite(m);
                debug!("new_event: {:?}", new_event);
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
    use super::*;
    use filter::Filter;

    #[test]
    fn test_collectd_ip_extraction() {
        let cs = CollectdScrub::new();

        let orig = "collectd.ip-10-1-21-239.interface-lo.if_errors.tx 0 1478751126";
        let filtered = cs.extract(orig);

        assert_eq!(Some("collectd.interface-lo.if_errors.tx 0 1478751126".into()),
                   filtered);
    }

    #[test]
    fn test_collectd_non_ip_extraction() {
        let cs = CollectdScrub::new();

        let orig = "collectd.totally_fine.interface-lo.if_errors.tx 0 1478751126";
        let filtered = cs.extract(orig);

        assert_eq!(Some("collectd.interface-lo.if_errors.tx 0 1478751126".into()),
                   filtered);
    }

    #[test]
    fn test_non_collectd_extraction() {
        let cs = CollectdScrub::new();

        let orig = "totally_fine.interface-lo.if_errors.tx 0 1478751126";
        let filtered = cs.extract(orig);

        assert_eq!(None, filtered);
    }
}
