use clap::{Parser, ValueEnum};
use once_cell::sync::OnceCell;
use regex::{Captures, Regex};

use crate::filters::{
    Filter, Not, QueryMatches, QueryTimeGreaterThan, QueryTimeLessThan, UserEquals, UserMatches,
};

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum SortOrder {
    Count,
    AvgTime,
    MaxTime,
    TotalTime,
}

#[derive(Parser)]
pub struct Opt {
    #[arg(short = 'i', long = "infile")]
    /// The path to the logfile. If not given, will try reading from stdin
    pub filename: Option<String>,
    #[arg(short = 'F', long = "filter", number_of_values = 1)]
    pub filters: Vec<String>,
    #[arg(short, long)]
    pub order: Option<SortOrder>,
    #[arg(short, long)]
    /// Combine identical queries
    pub aggregate: bool,
    #[arg(short, long)]
    /// Replace values with placeholders
    pub normalize: bool,
    #[arg(short, long, default_value = "10")]
    pub limit: usize,
    #[arg(short, long)]
    /// Prints the program's version number
    pub version: bool,
}

pub fn parse_opts() -> (Opt, Vec<Box<dyn Filter>>) {
    let opt = Opt::parse();
    let mut filters = Vec::with_capacity(opt.filters.len());
    for filter_def in &opt.filters {
        let filter = parse_filter(filter_def).unwrap_or_else(|error| panic!("{}", error));
        filters.push(filter);
    }
    (opt, filters)
}

fn parse_filter(arg: &str) -> Result<Box<dyn Filter>, String> {
    static REGEX: OnceCell<Regex> = OnceCell::new();
    let regex = REGEX
        .get_or_init(|| Regex::new(r"^(?P<name>\w+)\s*(?P<op>[=<>!~]+)\s*(?P<value>.+)$").unwrap());

    regex.captures(arg).ok_or(format!("Invalid filter format: '{}'", arg)).and_then(
        |caps: Captures| {
            create_filter(
                caps.name("name").unwrap().as_str(),
                caps.name("op").unwrap().as_str(),
                caps.name("value").unwrap().as_str(),
            )
        },
    )
}

fn create_filter(name: &str, op: &str, value: &str) -> Result<Box<dyn Filter>, String> {
    match name {
        "user" => match op {
            "=" => Ok(Box::new(UserEquals::new(value.to_string()))),
            "!=" => {
                let equals = Box::new(UserEquals::new(value.to_string()));
                Ok(Box::new(Not::new(equals)))
            }
            "~=" => Ok(Box::new(UserMatches::new(value.to_string())?)),
            _ => Err(format!("User filter expects one of '=', '!=' or '~=', found '{}'", op)),
        },
        "query" => match op {
            "~=" => Ok(Box::new(QueryMatches::new(value.to_string())?)),
            _ => Err(format!("Query filter only supports '~=', found '{}'", op)),
        },
        "query_time" => {
            let time: f64 = value.parse().expect("Query time filter requires a numeric argument");
            let msec = (1000.0 * time) as i64;
            match op {
                "<" | "<=" => Ok(Box::new(QueryTimeLessThan::new(msec))),
                ">" | ">=" => Ok(Box::new(QueryTimeGreaterThan::new(msec))),
                _ => Err(format!(
                    "Query time filter expects one of '<', '<=', '>' or '>=', found '{}'",
                    op
                )),
            }
        }
        _ => Err(format!("Unknown filter name: '{}'", name)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_parses_filters() {
        let opt: Opt = Opt::parse_from(&["test", "-Fuser!=foo", "--filter", "query~=SELECT foo"]);
        assert_eq!(opt.filters.len(), 2);
        let first = opt.filters.first().unwrap();
        assert_eq!(first, "user!=foo");
        let second = opt.filters.last().unwrap();
        assert_eq!(second, "query~=SELECT foo");
    }
}
