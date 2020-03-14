extern crate chrono;
extern crate regex;
extern crate sha1;
extern crate sqlparser;
extern crate structopt;
extern crate proc_macro;

mod aggregate;
mod canonicalize;
mod log_parser;
mod summarize;

use std::fs::File;
use std::io;
use std::process;
use structopt::StructOpt;

use aggregate::aggregate;
use canonicalize::{canonicalize, CanonicalLogEntry};
use log_parser::parse_log;
use summarize::{summarize_aggregates};

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short = "i", long = "infile", default_value = "")]
    /// The path to the logfile. If not given, will try reading from stdin
    filename: String,
    #[structopt(short = "v", long = "version")]
    /// Prints the program's version number
    version: bool,
}

fn main() {
    let opt = Opt::from_args();
    if opt.version {
        print_version();
        process::exit(0);
    }

    let log_entries = {
        if opt.filename.is_empty() {
            parse_log(io::stdin())
        } else {
            let file = File::open(opt.filename).expect("Unable to read from file");
            parse_log(file)
        }
    };

    let canonical_log_entries: Vec<CanonicalLogEntry> = log_entries
        .clone()
        .into_iter()
        .map(canonicalize)
        .collect();

    let aggregate_log_entries = aggregate(canonical_log_entries);

    for aggregate_entry in aggregate_log_entries.values() {
        let canonical_entry = aggregate_entry.entries.first().unwrap();
        println!(
            "{} queries, avg time {:.3} seconds, max time {:.3} seconds",
            aggregate_entry.count,
            aggregate_entry.avg_query_time as f64 / 1_000_000.0,
            aggregate_entry.max_query_time as f64 / 1_000_000.0,
        );
        println!("{}", canonical_entry.canonical_query);
        println!();
    }

    let summary = summarize_aggregates(&aggregate_log_entries);
    println!("Summary\n=======");
    println!(
        "{} unique queries ({} total), average execution time {:.3} seconds",
        summary.unique_queries,
        summary.total_queries,
        summary.avg_execution_time.num_microseconds().unwrap() as f64 / 1_000_000.0,
    );
    println!(
        "Execution time: average {:.3} seconds, maximum {:.3} seconds",
        summary.avg_execution_time.num_microseconds().unwrap() as f64 / 1_000_000.0,
        summary.max_execution_time.num_microseconds().unwrap() as f64 / 1_000_000.0,
    );
}

fn print_version() {
    println!(
        "{} v{} by {} - {}",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
        env!("CARGO_PKG_AUTHORS"),
        env!("CARGO_PKG_HOMEPAGE"),
    );
}
