# myslowlog

myslowlog is a CLI tool for analyzing MySQL slow query logs. Written in Rust,
it is able to process even gigabyte-sized logfiles quickly.

## Usage

### Basic

    myslowlog -i <filename>
    myslowlog --infile <filename>

When invoked without the `-i/--infile` argument, myslowlog will attempt to read from stdin.

### Filtering

    -F<filter>, --filter <filter>

Select only certain statements from the log based on the given filter expression.
This argument can be given multiple times.
The following filter criteria are available:

#### Filter by query string

Select statements that match a regular expression with the filter `query~=<pattern>`.

#### Filter by user name

Select statements issued by a specific user with the filter `user=<name>`,
or those issued by any user whose name matches a regex with `query~=<pattern>`.
Exclude statements issued by a specific user with `user!=<name>`.

#### Filter by execution time

Select statements by minimum execution time with `query_time<=<msec>`,
or by maximum execution time with `query_time>=<msec>`.

The operators `<` and `>` are also accepted and treated as aliases of `<=` and `>=`, respectively.

### Sorting

    -o <order>, --order <order>

Determine the sort order for the displayed log entries.
The following values are accepted (highest values are always displayed first):

- `count`: sort by number of occurrence
- `avg-time`: sort by average execution time
- `max-time`: sort by highest execution time
- `total-time`: sort by combined execution time

When invoked without the `--aggregate` flag, `--order=count` does nothing, and the
other three have the identical effect of sorting by the individual queries' execution time.

### Limiting

    -l <n>, --limit <n>

Display only the `n` first (after filtering and sorting) entries from the log.

### Aggregation

    -a, --aggregate

By default, myslowlog displays each individual query from the (filtered and sorted) log.
With this flag, it instead combines identical queries into a single record and displays the
number of individual queries as well as the average, maximum and total time for each record.

### Normalization

    -n, --normalize

With this flag, myslowlog will replace any actual values in the queries by placeholders
before aggregating them. Implies `--aggregate`.

## Limitations

The [SQL parser](https://crates.io/crates/sqlparser) used by myslowlog's normalization
feature does not support the full MySQL syntax yet, so you may encounter statements
that it cannot handle.
