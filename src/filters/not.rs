use crate::filters::Filter;
use crate::log_parser::LogEntry;

pub struct Not {
    filter: Box<dyn Filter>,
}

impl Not {
    pub fn new(filter: Box<dyn Filter>) -> Not { Not { filter } }
}

impl Filter for Not {
    fn matches(&self, log_entry: &LogEntry) -> bool { !self.filter.matches(log_entry) }
}
