use std::cmp::max;

use ahash::{HashMap, HashMapExt};

use crate::log_parser::LogEntry;
use crate::normalize::NormalizedLogEntry;

#[derive(Clone, Debug)]
pub struct AggregateLogEntry {
    pub query: String,
    pub count: i64,
    pub total_query_time: i128,
    pub avg_query_time: i128,
    pub max_query_time: i128,
}

impl AggregateLogEntry {
    fn new(query: String, query_time: i128) -> Self {
        AggregateLogEntry {
            query,
            count: 1,
            total_query_time: query_time,
            avg_query_time: query_time,
            max_query_time: query_time,
        }
    }

    fn update_with(&mut self, query_time: i128) {
        self.total_query_time += query_time;
        self.max_query_time = max(self.max_query_time, query_time);
        self.avg_query_time =
            (self.avg_query_time * (self.count as i128) + query_time) / (self.count as i128 + 1);
        self.count += 1;
    }
}

pub fn aggregate_entries(entries: Vec<LogEntry>) -> HashMap<String, AggregateLogEntry> {
    let mut result: HashMap<String, AggregateLogEntry> = HashMap::new();
    entries.into_iter().for_each(|entry| {
        let query_time = entry.query_time.whole_microseconds();
        if result.contains_key(&entry.query) {
            result.get_mut(&entry.query).unwrap().update_with(query_time);
        } else {
            result.insert(entry.query.clone(), AggregateLogEntry::new(entry.query, query_time));
        }
    });
    result
}

pub fn aggregate_normalized(
    entries: Vec<NormalizedLogEntry>,
) -> HashMap<String, AggregateLogEntry> {
    let mut result: HashMap<String, AggregateLogEntry> = HashMap::new();
    entries.into_iter().for_each(|entry| {
        let query_time = entry.entry.query_time.whole_microseconds();
        if result.contains_key(&entry.normalized_query) {
            result.get_mut(&entry.normalized_query).unwrap().update_with(query_time);
        } else {
            result.insert(
                entry.normalized_query.clone(),
                AggregateLogEntry::new(entry.normalized_query, query_time),
            );
        }
    });
    result
}
