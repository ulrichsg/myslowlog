use crate::filters::Filter;
use crate::log_parser::LogEntry;

pub struct QueryTimeLessThan {
    msec: i64,
}

impl QueryTimeLessThan {
    pub fn new(msec: i64) -> QueryTimeLessThan { QueryTimeLessThan { msec } }
}

impl Filter for QueryTimeLessThan {
    fn matches(&self, log_entry: &LogEntry) -> bool {
        log_entry.query_time.whole_milliseconds() <= self.msec as i128
    }
}
