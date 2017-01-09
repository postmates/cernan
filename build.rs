extern crate serde_codegen;

use std::env;
use std::path::Path;

fn main() {
    let out_dir = env::var_os("OUT_DIR").unwrap();

    let telemetry_src = Path::new("src/metric/telemetry_types.in.rs");
    let telemetry_dst = Path::new(&out_dir).join("telemetry_types.rs");
    let tagmap_src = Path::new("src/metric/tagmap_types.in.rs");
    let tagmap_dst = Path::new(&out_dir).join("tagmap_types.rs");
    let logline_src = Path::new("src/metric/logline_types.in.rs");
    let logline_dst = Path::new(&out_dir).join("logline_types.rs");
    let event_src = Path::new("src/metric/event_types.in.rs");
    let event_dst = Path::new(&out_dir).join("event_types.rs");
    serde_codegen::expand(&telemetry_src, &telemetry_dst).unwrap();
    serde_codegen::expand(&tagmap_src, &tagmap_dst).unwrap();
    serde_codegen::expand(&logline_src, &logline_dst).unwrap();
    serde_codegen::expand(&event_src, &event_dst).unwrap();
}
