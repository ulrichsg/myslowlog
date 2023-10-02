use std::fs::File;
use std::io::Write;
use std::{io, process};

use rayon::prelude::*;

use crate::aggregate::{aggregate_entries, aggregate_normalized, AggregateLogEntry};
use crate::filters::Filter;
use crate::log_parser::{parse_log, LogEntry};
use crate::normalize::{normalize, NormalizedLogEntry};
use crate::opt::{parse_opts, Opt, SortOrder};

mod aggregate;
mod filters;
mod log_parser;
mod normalize;
mod opt;

fn main() {
    let (opt, filters) = parse_opts();
    if opt.version {
        print_version();
        process::exit(0);
    }

    let all_entries = {
        if let Some(filename) = &opt.filename {
            let file = File::open(filename).expect("Unable to read from file");
            parse_log(file)
        } else {
            parse_log(io::stdin())
        }
    };

    match (opt.aggregate, opt.normalize) {
        (_, true) => render_normalized(all_entries, &filters, &opt),
        (true, _) => render_aggregated(all_entries, &filters, &opt),
        _ => render_individual(all_entries, &filters, &opt),
    };
}

fn render_individual(entries: Vec<LogEntry>, filters: &[Box<dyn Filter>], options: &Opt) {
    let mut filtered: Vec<LogEntry> = entries
        .into_par_iter()
        .filter(|entry| filters.is_empty() || filters.iter().all(|filter| filter.matches(entry)))
        .collect();

    match options.order {
        None | Some(SortOrder::Count) => (),
        _ => filtered.sort_unstable_by_key(|e| e.query_time),
    };

    let mut stdout = io::stdout().lock();

    filtered.iter().rev().take(options.limit).enumerate().for_each(|(i, entry)| {
        writeln!(
            stdout,
            "#{}: [{}] {}@{}, query_time {:.3} s, lock_time {}, rows_examined {}, rows_sent {}",
            i + 1,
            entry.timestamp,
            entry.user,
            entry.host,
            entry.query_time.as_seconds_f64(),
            entry.lock_time,
            entry.rows_examined,
            entry.rows_sent,
        )
        .unwrap();
        writeln!(stdout, "{}", entry.query).unwrap();
    });
}

fn render_aggregated(entries: Vec<LogEntry>, filters: &[Box<dyn Filter>], options: &Opt) {
    let filtered: Vec<LogEntry> = entries
        .into_par_iter()
        .filter(|entry| filters.is_empty() || filters.iter().all(|filter| filter.matches(entry)))
        .collect();

    let aggregated = aggregate_entries(filtered);
    print_aggregated(aggregated, options);
}

fn render_normalized(entries: Vec<LogEntry>, filters: &[Box<dyn Filter>], options: &Opt) {
    let normalized: Vec<NormalizedLogEntry> = entries
        .into_par_iter()
        .filter(|entry| filters.is_empty() || filters.iter().all(|filter| filter.matches(entry)))
        .map(normalize)
        .collect();

    let aggregated = aggregate_normalized(normalized);
    print_aggregated(aggregated, options);
}

fn print_aggregated(entries: ahash::HashMap<String, AggregateLogEntry>, options: &Opt) {
    let mut entries = entries.values().cloned().collect::<Vec<AggregateLogEntry>>();

    match options.order {
        Some(SortOrder::Count) => entries.sort_unstable_by_key(|e| e.count),
        Some(SortOrder::TotalTime) => entries.sort_unstable_by_key(|e| e.total_query_time),
        Some(SortOrder::MaxTime) => entries.sort_unstable_by_key(|e| e.max_query_time),
        Some(SortOrder::AvgTime) => entries.sort_unstable_by_key(|e| e.avg_query_time),
        None => (),
    };

    let mut stdout = io::stdout().lock();

    entries.iter().rev().enumerate().take(options.limit).for_each(|(i, entry)| {
        writeln!(
            stdout,
            "#{}: count {}, total: {:.3} s, avg {:.3} s, max {:.3} s",
            i + 1,
            entry.count,
            entry.total_query_time as f64 / 1_000_000.0,
            entry.avg_query_time as f64 / 1_000_000.0,
            entry.max_query_time as f64 / 1_000_000.0,
        )
        .unwrap();
        writeln!(stdout, "{}", entry.query).unwrap();
    });
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
