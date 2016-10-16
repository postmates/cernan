extern crate serde_codegen;

use std::env;
use std::path::Path;

fn main() {
    let out_dir = env::var_os("OUT_DIR").unwrap();

    let metric_src = Path::new("src/metric_types.in.rs");
    let metric_dst = Path::new(&out_dir).join("metric_types.rs");
    serde_codegen::expand(&metric_src, &metric_dst).unwrap();
}
