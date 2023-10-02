use crate::filters::Filter;
use crate::log_parser::LogEntry;

pub struct UserEquals {
    name: String,
}

impl UserEquals {
    pub fn new(name: String) -> UserEquals {
        UserEquals { name }
    }
}

impl Filter for UserEquals {
    fn matches(&self, log_entry: &LogEntry) -> bool {
        self.name == log_entry.user
    }
}
