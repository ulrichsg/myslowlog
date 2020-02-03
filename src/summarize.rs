use crate::chrono::Duration;
use log_parser::LogEntry;
use std::cmp::max;

pub struct Summary {
    pub num_queries: usize,
    pub max_execution_time: Duration,
    pub avg_execution_time: Duration,
}

pub fn summarize(entries: &[LogEntry]) -> Summary {
    let max_execution_time = entries.iter().fold(0, |acc, val| {
        max(acc, val.query_time.num_microseconds().unwrap())
    });

    let total_execution_time = entries.iter().fold(0, |acc, val| {
        acc + val.query_time.num_microseconds().unwrap()
    });

    let avg_execution_time = total_execution_time / entries.len() as i64;

    Summary {
        num_queries: entries.len(),
        max_execution_time: Duration::microseconds(max_execution_time),
        avg_execution_time: Duration::microseconds(avg_execution_time),
    }
}
