use regex::Regex;

use crate::filters::Filter;
use crate::log_parser::LogEntry;

pub struct QueryMatches {
    regex: Regex,
}

impl QueryMatches {
    pub fn new(pattern: String) -> Result<QueryMatches, String> {
        let regex = Regex::new(&pattern)
            .map_err(|_err| format!("Invalid regular expression: '{}'", &pattern))?;
        Ok(QueryMatches { regex })
    }
}

impl Filter for QueryMatches {
    fn matches(&self, log_entry: &LogEntry) -> bool { self.regex.is_match(&log_entry.query) }
}
