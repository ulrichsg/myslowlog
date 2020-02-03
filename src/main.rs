extern crate chrono;
extern crate regex;
extern crate structopt;

mod log_parser;
mod summarize;

use std::io;
use std::fs::File;
use std::process;
use structopt::StructOpt;

use log_parser::parse_log;
use summarize::summarize;

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

    for entry in &log_entries {
        println!(
            "Executed in {:.3} seconds returning {} row(s) for {}@{}",
            entry.query_time.num_microseconds().unwrap() as f64 / 1_000_000.0,
            entry.rows_sent,
            entry.user,
            entry.host
        );
        let q = if entry.query.len() < 120 { &entry.query } else { &entry.query[..120] };
        println!("{}", q);
        println!();
    }

    let summary = summarize(&log_entries);
    println!("Summary\n=======");
    println!(
        "{} queries total, average execution time {:.3} seconds",
        summary.num_queries,
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
