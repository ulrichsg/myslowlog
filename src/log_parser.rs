use chrono::{DateTime, Duration, FixedOffset};
use regex::{Match, Regex};
use std::io::{BufRead, BufReader, Read};
use std::iter::Peekable;

trait AdvanceWhile<I: Iterator> {
    fn advance_while<P>(&mut self, predicate: P) -> Option<I::Item>
    where
        P: Fn(&I::Item) -> bool;
}

impl<I: Iterator> AdvanceWhile<I> for Peekable<I> {
    fn advance_while<P>(&mut self, predicate: P) -> Option<I::Item>
    where
        P: Fn(&I::Item) -> bool,
    {
        let mut result: Option<I::Item> = None;
        while let Some(true) = self.peek().map(&predicate) {
            result = self.next();
        }
        result
    }
}

#[derive(Clone, Debug)]
pub struct LogEntry {
    pub timestamp: DateTime<FixedOffset>,
    pub user: String,
    pub host: String,
    pub query_time: Duration,
    pub lock_time: Duration,
    pub rows_sent: i32,
    pub rows_examined: i32,
    pub query: String,
}

pub fn parse_log(log: impl Read) -> Vec<LogEntry> {
    let reader = BufReader::new(log);
    let mut lines = reader.lines().peekable();
    let mut entries = Vec::new();

    let time_regex = Regex::new(r"^# Time: (\S+)").unwrap();
    let user_regex = Regex::new(r"^# User@Host: (\w+)\[[^\]]+\] @\s+\[([\d.]+)\]").unwrap();
    let metric_regex = Regex::new(
        r"^# Query_time: ([\d.]+)\s+Lock_time: ([\d.]+)\s+Rows_sent: (\d+)\s+Rows_examined: (\d+)",
    )
    .unwrap();

    while let Some(l) = lines.next() {
        let line = l.unwrap();
        if !line.starts_with("# Time") {
            continue;
        }

        let time_caps = time_regex
            .captures(&line)
            .expect("Timestamp matching failed");
        let time_cap = time_caps.get(1);
        let timestamp = match time_cap {
            Some(cap) => DateTime::parse_from_str(cap.as_str(), "%+").unwrap(),
            _ => break,
        };

        let line = match lines.next() {
            Some(l) => l.unwrap(),
            _ => break,
        };
        let user_caps = user_regex
            .captures(&line)
            .expect("User/Host matching failed");
        let user = user_caps.get(1).unwrap().as_str().to_string();
        let host = user_caps.get(2).unwrap().as_str().to_string();

        let line = match lines.next() {
            Some(l) => l.unwrap(),
            _ => break,
        };
        let metric_caps = metric_regex
            .captures(&line)
            .expect("Metric matching failed");
        let query_time = microseconds_to_duration(metric_caps.get(1).unwrap());
        let lock_time = microseconds_to_duration(metric_caps.get(2).unwrap());
        let rows_sent = metric_caps.get(3).unwrap().as_str().parse::<i32>().unwrap();
        let rows_examined = metric_caps.get(4).unwrap().as_str().parse::<i32>().unwrap();

        let maybe_query = lines
            .by_ref()
            .advance_while(|next| !next.as_ref().unwrap().starts_with('#'));
        let query = match maybe_query {
            Some(q) => q.unwrap(),
            _ => break,
        };

        entries.push(LogEntry {
            timestamp,
            user,
            host,
            query_time,
            lock_time,
            rows_sent,
            rows_examined,
            query,
        });
    }

    entries
}

fn microseconds_to_duration(cap: Match) -> Duration {
    let us = cap.as_str().parse::<f64>().unwrap() * 1_000_000.0;
    Duration::microseconds(us as i64)
}
