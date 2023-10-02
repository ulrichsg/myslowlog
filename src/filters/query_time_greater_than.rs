use crate::filters::Filter;
use crate::log_parser::LogEntry;

pub struct QueryTimeGreaterThan {
    msec: i64,
}

impl QueryTimeGreaterThan {
    pub fn new(msec: i64) -> QueryTimeGreaterThan {
        QueryTimeGreaterThan { msec }
    }
}

impl Filter for QueryTimeGreaterThan {
    fn matches(&self, log_entry: &LogEntry) -> bool {
        log_entry.query_time.whole_milliseconds() >= self.msec as i128
    }
}
