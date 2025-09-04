use regex::Regex;

use crate::filters::Filter;
use crate::log_parser::LogEntry;

pub struct UserMatches {
    regex: Regex,
}

impl UserMatches {
    pub fn new(pattern: String) -> Result<UserMatches, String> {
        let regex = Regex::new(&pattern)
            .map_err(|_err| format!("Invalid regular expression: '{}'", &pattern))?;
        Ok(UserMatches { regex })
    }
}

impl Filter for UserMatches {
    fn matches(&self, log_entry: &LogEntry) -> bool { self.regex.is_match(&log_entry.user) }
}
