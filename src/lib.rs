//! Cernan is a telemetry and logging aggregation server. It exposes multiple
//! interfaces for ingestion and can emit to mutiple aggregation sources while
//! doing in-flight manipulation of data. Cernan has minimal CPU and memory
//! requirements and is intended to service bursty telemetry _without_ load
//! shedding. Cernan aims to be _reliable_ and _convenient_ to use, both for
//! application engineers and operations staff.
//!
//! Why you might choose to use cernan:
//!
//!  * You need to ingest telemetry from multiple protocols.
//!  * You need to multiplex telemetry over aggregation services.
//!  * You want to convert log lines into telemetry.
//!  * You want to convert telemetry into log lines.
//!  * You want to transform telemetry or log lines in-flight.
//!
//! If you'd like to learn more, please do have a look in
//! our [wiki](https://github.com/postmates/cernan/wiki/).
#![allow(unknown_lints)]
#![deny(trivial_numeric_casts, missing_docs, unstable_features, unused_import_braces)]
extern crate byteorder;
extern crate chrono;
extern crate clap;
extern crate coco;
extern crate elastic;
extern crate flate2;
extern crate glob;
extern crate hopper;
extern crate hyper;
extern crate libc;
extern crate lua;
extern crate protobuf;
extern crate quantiles;
extern crate regex;
extern crate rusoto_core;
extern crate rusoto_firehose;
extern crate seahash;
#[macro_use]
extern crate serde_json;
extern crate toml;
extern crate url;
extern crate uuid;
extern crate mio;

#[macro_use]
extern crate log;

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate serde_derive;

#[cfg(test)]
extern crate quickcheck;

pub mod sink;
pub mod buckets;
pub mod config;
pub mod metric;
pub mod time;
pub mod source;
pub mod filter;
pub mod util;
pub mod constants;
pub mod protocols;
