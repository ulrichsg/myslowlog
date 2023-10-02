mod not;
mod query_matches;
mod query_time_greater_than;
mod query_time_less_than;
mod user_equals;
mod user_matches;

use crate::log_parser::LogEntry;

pub trait Filter: Sync {
    fn matches(&self, log_entry: &LogEntry) -> bool;
}

pub use self::not::Not;
pub use self::query_matches::QueryMatches;
pub use self::query_time_greater_than::QueryTimeGreaterThan;
pub use self::query_time_less_than::QueryTimeLessThan;
pub use self::user_equals::UserEquals;
pub use self::user_matches::UserMatches;
