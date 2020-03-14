use canonicalize::CanonicalLogEntry;
use std::collections::HashMap;
use std::cmp::max;

pub struct AggregateLogEntry {
    pub entries: Vec<CanonicalLogEntry>,
    pub count: i64,
    pub avg_query_time: i64,
    pub max_query_time: i64,
}

impl AggregateLogEntry {
    fn from(entry: &CanonicalLogEntry) -> AggregateLogEntry {
        AggregateLogEntry {
            entries: vec![entry.clone()],
            count: 1,
            avg_query_time: entry.entry.query_time.num_microseconds().unwrap_or(0),
            max_query_time: entry.entry.query_time.num_microseconds().unwrap_or(0),
        }
    }

    fn update_with(&mut self, entry: &CanonicalLogEntry) {
        let other_query_time = entry.entry.query_time.num_microseconds().unwrap_or(0);
        self.max_query_time = max(self.max_query_time, other_query_time);
        self.avg_query_time = (self.avg_query_time * self.count + other_query_time)
            / (self.count + 1);
        self.count = self.count + 1;
    }
}

pub fn aggregate(entries: Vec<CanonicalLogEntry>) -> HashMap<String, AggregateLogEntry> {
    let mut result: HashMap<String, AggregateLogEntry> = HashMap::new();
    entries
        .iter()
        .for_each(|entry| {
            if result.contains_key(&entry.hash) {
                result.get_mut(&entry.hash).unwrap().update_with(&entry);
            } else {
                result.insert(entry.hash.clone(), AggregateLogEntry::from(&entry));
            }
        });
    result
}
