use std::io::{BufRead, BufReader, Read};
use std::iter::Peekable;

use regex::{Match, Regex};
use time::format_description::well_known::Iso8601;
use time::{Duration, OffsetDateTime};

trait AdvanceWhile<I: Iterator> {
    fn advance_while<P>(&mut self, predicate: P) -> Option<I::Item>
    where
        P: Fn(&I::Item) -> bool;
}

impl<I: Iterator> AdvanceWhile<I> for Peekable<I> {
    fn advance_while<P>(&mut self, predicate: P) -> Option<I::Item>
    where
        P: Fn(&I::Item) -> bool,
    {
        let mut result: Option<I::Item> = None;
        while let Some(true) = self.peek().map(&predicate) {
            result = self.next();
        }
        result
    }
}

#[derive(Clone, Debug)]
pub struct LogEntry {
    pub timestamp: OffsetDateTime,
    pub user: String,
    pub host: String,
    pub query_time: Duration,
    pub lock_time: Duration,
    pub rows_sent: i32,
    pub rows_examined: i32,
    pub query: String,
}

pub fn parse_log(log: impl Read) -> Vec<LogEntry> {
    let reader = BufReader::new(log);
    let mut lines = reader.lines().peekable();
    let mut entries = Vec::new();

    let time_regex = Regex::new(r"# Time: (\S+)").unwrap();
    let user_regex = Regex::new(r"^# User@Host: ([\w-]+)\[[^]]+] @ (\w*) \[([\d.]*)]").unwrap();
    let metric_regex = Regex::new(
        r"^# Query_time: ([\d.]+)\s+Lock_time: ([\d.]+)\s+Rows_sent: (\d+)\s+Rows_examined: (\d+)",
    )
    .unwrap();
    let whitespace_regex = Regex::new(r"\t|\s\s+").unwrap();

    while let Some(l) = lines.next() {
        let line = l.unwrap();

        if !line.starts_with("# Time") {
            continue;
        }

        let time_caps = time_regex.captures(&line);
        if time_caps.is_none() {
            panic!("Could not parse time from line:\n{line}");
        }

        let time_cap = time_caps.unwrap().get(1);
        let timestamp = match time_cap {
            Some(cap) => OffsetDateTime::parse(cap.as_str(), &Iso8601::DEFAULT).unwrap(),
            _ => break,
        };

        let line = match lines.next() {
            Some(l) => l.unwrap(),
            _ => break,
        };
        let user_caps = user_regex.captures(&line);
        if user_caps.is_none() {
            panic!("Could not parse user info from line:\n{line}");
        }
        let user_caps = user_caps.unwrap();
        let user = user_caps.get(1).unwrap().as_str().to_string();
        let mut host = user_caps.get(2).unwrap().as_str().to_string();
        if host.is_empty() {
            host = user_caps.get(3).unwrap().as_str().to_string();
        }

        let line = match lines.next() {
            Some(l) => l.unwrap(),
            _ => break,
        };
        let metric_caps = metric_regex.captures(&line).expect("Metric matching failed");
        let query_time = microseconds_to_duration(metric_caps.get(1).unwrap());
        let lock_time = microseconds_to_duration(metric_caps.get(2).unwrap());
        let rows_sent = metric_caps.get(3).unwrap().as_str().parse::<i32>().unwrap();
        let rows_examined = metric_caps.get(4).unwrap().as_str().parse::<i32>().unwrap();

        let _ = lines.by_ref().advance_while(|next| {
            let q = next.as_ref().unwrap();
            q.starts_with("SET timestamp") || q.starts_with("use")
        });

        let mut query = match lines.next() {
            Some(q) => q.unwrap(),
            _ => break,
        };

        while !query.ends_with(';') {
            let next_line = match lines.next() {
                Some(l) => l.unwrap(),
                _ => break,
            };

            // In general, if a query stretches across multiple log lines, we insert a space
            // to avoid accidentally breaking the syntax. However, in pathological cases
            // where a line is wrapped because it is overly long, the break may occur in the middle
            // of a word or number, so adding the space would be counterproductive.
            // There is no easy way to make completely sure whether we need it or not,
            // but this heuristic is good enough for the cases I've seen.
            let padding = if query.ends_with(|c: char| c.is_ascii_digit())
                && next_line.starts_with(|c: char| c.is_ascii_digit())
            {
                ""
            } else {
                " "
            };

            query = format!("{}{}{}", query, padding, next_line);
        }

        entries.push(LogEntry {
            timestamp,
            user,
            host,
            query_time,
            lock_time,
            rows_sent,
            rows_examined,
            query: whitespace_regex.replace_all(&query, " ").to_string(),
        });
    }

    entries
}

fn microseconds_to_duration(cap: Match) -> Duration {
    let usec = cap.as_str().parse::<f64>().unwrap() * 1_000_000.0;
    Duration::microseconds(usec as i64)
}

#[cfg(test)]
mod tests {
    use indoc::indoc;

    use super::*;

    #[test]
    fn it_parses_logs_correctly() {
        let log = indoc!(
            b"
            # Time: 2019-07-30T13:01:34.887103Z
            # User@Host: foo[bar] @  [127.0.0.1]  Id: 1337
            # Query_time: 1.289039  Lock_time: 0.000061 Rows_sent: 50000  Rows_examined: 100000
            use foo;
            SET timestamp=1000000000;
            SELECT * FROM baz WHERE quux = 1;
            # Time: 2019-07-30T13:01:34.887103Z
            # User@Host: foo[bar] @  [127.0.0.1]  Id: 1337
            # Query_time: 0.123456  Lock_time: 0.000009 Rows_sent: 1  Rows_examined: 1
            SET timestamp=1000000001;
            UPDATE baz SET quux = 2 WHERE id = 42;
        "
        );
        let entries = parse_log(log as &[u8]);
        assert_eq!(2, entries.len());

        let e1 = entries.first().expect("we know this exists");
        assert_eq!("foo", e1.user);
        assert_eq!("127.0.0.1", e1.host);
        assert_eq!(1289039, e1.query_time.whole_microseconds());
        assert_eq!(61, e1.lock_time.whole_microseconds());
        assert_eq!(50000, e1.rows_sent);
        assert_eq!(100000, e1.rows_examined);
        assert_eq!("SELECT * FROM baz WHERE quux = 1;", e1.query);

        let e2 = entries.last().expect("we also know this exists");
        assert_eq!("UPDATE baz SET quux = 2 WHERE id = 42;", e2.query);
    }

    #[test]
    fn it_skips_additional_lines_at_the_start() {
        // something we might see in AWS RDS
        let log = indoc!(b"
            /rdsdbbin/oscar/bin/mysqld, Version: 5.7.12-log (MySQL Community Server (GPL)). started with:
            Tcp port: 3306  Unix socket: /tmp/mysql.sock
            Time                 Id Command    Argument
            # Time: 2019-07-30T13:01:34.887103Z
            # User@Host: foo[bar] @  [127.0.0.1]  Id: 1337
            # Query_time: 1.289039  Lock_time: 0.000061 Rows_sent: 50000  Rows_examined: 100000
            SELECT * FROM baz WHERE quux = 1;
        ");

        let entries = parse_log(log as &[u8]);
        assert_eq!(1, entries.len());

        let e1 = entries.first().expect("we know this exists");
        assert_eq!("SELECT * FROM baz WHERE quux = 1;", e1.query);
    }

    #[test]
    fn it_handles_empty_logs() {
        let log = b"";
        let entries = parse_log(log as &[u8]);
        assert_eq!(0, entries.len());
    }

    #[test]
    fn it_handles_nonempty_files_without_log_entries() {
        let log = indoc!(b"
            /rdsdbbin/oscar/bin/mysqld, Version: 5.7.12-log (MySQL Community Server (GPL)). started with:
            Tcp port: 3306  Unix socket: /tmp/mysql.sock
            Time                 Id Command    Argument
        ");

        let entries = parse_log(log as &[u8]);
        assert_eq!(0, entries.len());
    }

    #[test]
    fn it_handles_concatenated_log_files() {
        // Two redacted slowlogs from AWS Aurora MySQL
        let log = indoc!(b"
            /rdsdbbin/oscar/bin/mysqld, Version: 5.7.12-log (MySQL Community Server (GPL)). started with:
            Tcp port: 3306  Unix socket: /tmp/mysql.sock
            Time                 Id Command    Argument
            # Time: 2021-05-11T07:00:13.212839Z
            # User@Host: foo[foo] @  [127.0.0.1]  Id: 127461241
            # Query_time: 0.778443  Lock_time: 0.000027 Rows_sent: 1  Rows_examined: 1801663
            use foo;
            SET timestamp=1620716413;
            SELECT foo FROM bar WHERE baz = 'quux';
            /rdsdbbin/oscar/bin/mysqld, Version: 5.7.12-log (MySQL Community Server (GPL)). started with:
            Tcp port: 3306  Unix socket: /tmp/mysql.sock
            Time                 Id Command    Argument
            /rdsdbbin/oscar/bin/mysqld, Version: 5.7.12-log (MySQL Community Server (GPL)). started with:
            Tcp port: 3306  Unix socket: /tmp/mysql.sock
            Time                 Id Command    Argument
            # Time: 2021-05-11T08:00:13.307203Z
            # User@Host: foo[foo] @  [127.0.0.1]  Id: 127563196
            # Query_time: 2.150780  Lock_time: 0.000046 Rows_sent: 25  Rows_examined: 9233384
            use foo;
            SET timestamp=1620720013;
            SELECT foo FROM bar WHERE baz = 'quux';
            /rdsdbbin/oscar/bin/mysqld, Version: 5.7.12-log (MySQL Community Server (GPL)). started with:
            Tcp port: 3306  Unix socket: /tmp/mysql.sock
            Time                 Id Command    Argument
            /rdsdbbin/oscar/bin/mysqld, Version: 5.7.12-log (MySQL Community Server (GPL)). started with:
            Tcp port: 3306  Unix socket: /tmp/mysql.sock
            Time                 Id Command    Argument
        ");

        let entries = parse_log(log as &[u8]);
        assert_eq!(2, entries.len());

        entries.iter().for_each(|entry| {
            assert_eq!(entry.query, "SELECT foo FROM bar WHERE baz = 'quux';");
        });
    }

    #[test]
    fn it_handles_multiline_queries() {
        let log = indoc!(b"
            /rdsdbbin/oscar/bin/mysqld, Version: 5.7.12-log (MySQL Community Server (GPL)). started with:
            Tcp port: 3306  Unix socket: /tmp/mysql.sock
            Time                 Id Command    Argument
            # Time: 2019-07-30T13:01:34.887103Z
            # User@Host: foo[bar] @  [127.0.0.1]  Id: 1337
            # Query_time: 1.289039  Lock_time: 0.000061 Rows_sent: 50000  Rows_examined: 100000
            SELECT *
                FROM baz
                WHERE quux = 1;
        ");

        let entries = parse_log(log as &[u8]);
        assert_eq!(1, entries.len());

        let e1 = entries.first().expect("we know this exists");
        assert_eq!("SELECT * FROM baz WHERE quux = 1;", e1.query);
    }
}
