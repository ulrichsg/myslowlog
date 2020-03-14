use chrono::Duration;
use log_parser::LogEntry;
use std::cmp::max;
use aggregate::AggregateLogEntry;
use std::collections::HashMap;

pub struct Summary {
    pub total_queries: i64,
    pub unique_queries: i64,
    pub max_execution_time: Duration,
    pub avg_execution_time: Duration,
}

pub fn summarize_aggregates(entries: &HashMap<String, AggregateLogEntry>) -> Summary {
    let total_queries = entries.values().fold(0, |acc, val| {
        acc + val.count
    });

    let max_execution_time = entries.values().fold(0, |acc, val| {
        max(acc, val.max_query_time)
    });

    let total_execution_time = entries.values().fold(0, |acc, val| {
        acc + val.avg_query_time * val.count
    });

    let avg_execution_time = total_execution_time / total_queries;

    Summary {
        total_queries,
        unique_queries: entries.len() as i64,
        max_execution_time: Duration::microseconds(max_execution_time),
        avg_execution_time: Duration::microseconds(avg_execution_time),
    }
}
