extern crate serde_codegen;

use std::env;
use std::path::Path;

fn main() {
    let out_dir = env::var_os("OUT_DIR").unwrap();

    let metric_src = Path::new("src/metric/metric_types.in.rs");
    let metric_dst = Path::new(&out_dir).join("metric_types.rs");
    let tagmap_src = Path::new("src/metric/tagmap_types.in.rs");
    let tagmap_dst = Path::new(&out_dir).join("tagmap_types.rs");
    serde_codegen::expand(&metric_src, &metric_dst).unwrap();
    serde_codegen::expand(&tagmap_src, &tagmap_dst).unwrap();
}
