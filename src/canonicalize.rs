use sha1::Sha1;
use sqlparser::parser::Parser;
use sqlparser::dialect::MySqlDialect;
use sqlparser::ast::{Statement, OrderByExpr, Expr, Value, DateTimeField, SetExpr, Values, Select, SelectItem, TableWithJoins, Join, JoinOperator, JoinConstraint, TableFactor, Cte};
use sqlparser::ast::Query;
use log_parser::LogEntry;
use std::fmt;

#[derive(Clone, Debug)]
pub struct CanonicalLogEntry {
    pub entry: LogEntry,
    pub canonical_query: String,
    pub hash: String,
}

impl fmt::Display for CanonicalLogEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let entry = &self.entry;
        write!(
            f,
            "Executed in {:.3} seconds returning {} row(s) for {}@{}\n{}",
            entry.query_time.num_microseconds().unwrap() as f64 / 1_000_000.0,
            entry.rows_sent,
            entry.user,
            entry.host,
            &self.canonical_query
        )
    }
}

pub fn canonicalize(entry: LogEntry) -> CanonicalLogEntry {
    let dialect = MySqlDialect {};
    let parser_result = Parser::parse_sql(&dialect, entry.query.clone());
    let canonical_query = match parser_result {
        Err(_) => String::from("Unparseable statement"),
        Ok(ast) => canonicalize_ast(ast)
    };

    CanonicalLogEntry {
        entry: entry.clone(),
        canonical_query: canonical_query.clone(),
        hash: Sha1::from(canonical_query).digest().to_string(),
    }
}

fn canonicalize_ast(ast: Vec<Statement>) -> String {
    ast
        .into_iter()
        .map(canonicalize_stmt)
        .fold(String::new(), |acc, item| acc + " " + &item)
}

fn canonicalize_stmt(stmt: Statement) -> String {
    let result = match stmt {
        Statement::Query(query) =>
            Statement::Query(Box::new(canonicalize_query(*query))),
        default @ _ => default,
    };
    format!("{};", result)
}

fn canonicalize_query(query: Query) -> Query {
    let ctes = query.ctes
        .into_iter()
        .map(canonicalize_cte)
        .collect();
    let order_by = query.order_by
        .into_iter()
        .map(canonicalize_order_by)
        .collect();
    Query {
        ctes,
        body: canonicalize_set_expr(query.body),
        order_by,
        limit: query.limit,
        offset: query.offset.map(canonicalize_expr),
        fetch: query.fetch,
    }
}

fn canonicalize_cte(cte: Cte) -> Cte {
    Cte {
        alias: cte.alias,
        query: canonicalize_query(cte.query)
    }
}

fn canonicalize_set_expr(set_expr: SetExpr) -> SetExpr {
    match set_expr {
        SetExpr::Select(select) => SetExpr::Select(Box::new(canonicalize_select(*select))),
        SetExpr::Query(query) => SetExpr::Query(Box::new(canonicalize_query(*query))),
        SetExpr::SetOperation { op, all, left, right } =>
            SetExpr::SetOperation {
                op,
                all,
                left: Box::new(canonicalize_set_expr(*left)),
                right: Box::new(canonicalize_set_expr(*right)),
            },
        SetExpr::Values(values) => SetExpr::Values(canonicalize_values(values)),
    }
}

fn canonicalize_select(select: Select) -> Select {
    let projection = select.projection
        .into_iter()
        .map(canonicalize_select_item)
        .collect();
    let from = select.from
        .into_iter()
        .map(canonicalize_table_with_joins)
        .collect();
    let group_by = select.group_by
        .into_iter()
        .map(canonicalize_expr)
        .collect();

    Select {
        distinct: select.distinct,
        projection,
        from,
        selection: select.selection.map(canonicalize_expr),
        group_by,
        having: select.having.map(canonicalize_expr),
    }
}

fn canonicalize_select_item(item: SelectItem) -> SelectItem {
    match item {
        SelectItem::UnnamedExpr(expr) => SelectItem::UnnamedExpr(canonicalize_expr(expr)),
        SelectItem::ExprWithAlias { expr, alias } =>
            SelectItem::ExprWithAlias {
                expr: canonicalize_expr(expr),
                alias,
            },
        qw @ SelectItem::QualifiedWildcard(_) => qw,
        SelectItem::Wildcard => SelectItem::Wildcard,
    }
}

fn canonicalize_table_with_joins(twj: TableWithJoins) -> TableWithJoins {
    let joins = twj.joins
        .into_iter()
        .map(canonicalize_join)
        .collect();
    TableWithJoins {
        relation: canonicalize_table_factor(twj.relation),
        joins,
    }
}

fn canonicalize_join(join: Join) -> Join {
    Join {
        relation: canonicalize_table_factor(join.relation),
        join_operator: canonicalize_join_operator(join.join_operator),
    }
}

fn canonicalize_table_factor(tf: TableFactor) -> TableFactor {
    match tf {
        TableFactor::NestedJoin(table_with_joins) =>
            TableFactor::NestedJoin(Box::new(canonicalize_table_with_joins(*table_with_joins))),
        TableFactor::Derived { lateral, subquery, alias } =>
            TableFactor::Derived {
                lateral,
                subquery: Box::new(canonicalize_query(*subquery)),
                alias,
            },
        default @ _ => default,
    }
}

fn canonicalize_join_operator(operator: JoinOperator) -> JoinOperator {
    match operator {
        JoinOperator::Inner(constraint) => JoinOperator::Inner(canonicalize_join_constraint(constraint)),
        JoinOperator::LeftOuter(constraint) => JoinOperator::LeftOuter(canonicalize_join_constraint(constraint)),
        JoinOperator::RightOuter(constraint) => JoinOperator::RightOuter(canonicalize_join_constraint(constraint)),
        JoinOperator::FullOuter(constraint) => JoinOperator::FullOuter(canonicalize_join_constraint(constraint)),
        default @ _ => default,
    }
}

fn canonicalize_join_constraint(constraint: JoinConstraint) -> JoinConstraint {
    match constraint {
        JoinConstraint::On(expr) => JoinConstraint::On(canonicalize_expr(expr)),
        default @ _ => default,
    }
}

fn canonicalize_values(values: Values) -> Values {
    let result = values.0
        .into_iter()
        .map(|vec| {
            vec
                .into_iter()
                .map(canonicalize_expr)
                .collect()
        })
        .collect();
    Values(result)
}

fn canonicalize_order_by(order_by: OrderByExpr) -> OrderByExpr {
    OrderByExpr {
        expr: canonicalize_expr(order_by.expr),
        asc: order_by.asc,
    }
}

fn canonicalize_expr(expr: Expr) -> Expr {
    let map_exprs = |exprs: Vec<Expr>| {
        exprs
            .into_iter()
            .map(canonicalize_expr)
            .collect()
    };
    let map_boxed_expr = |boxed: Box<Expr>| {
        Box::new(canonicalize_expr(*boxed))
    };
    let map_boxed_query = |boxed: Box<Query>| {
        Box::new(canonicalize_query(*boxed))
    };
    match expr {
        Expr::IsNull(e) => Expr::IsNull(map_boxed_expr(e)),
        Expr::IsNotNull(e) => Expr::IsNotNull(map_boxed_expr(e)),
        // reduce all lists down to 1 element
        Expr::InList { expr, list, negated } =>
            Expr::InList {
                expr: map_boxed_expr(expr),
                list: match list.first() {
                    Some(expr) => vec![canonicalize_expr(expr.to_owned())],
                    None => Vec::new(),
                },
                negated,
            },
        // Expr::InList { expr, list, negated } =>
        //     Expr::InList {
        //         expr: map_boxed_expr(expr),
        //         list: map_exprs(list),
        //         negated,
        Expr::InSubquery { expr, subquery, negated } =>
            Expr::InSubquery {
                expr: map_boxed_expr(expr),
                subquery: map_boxed_query(subquery),
                negated,
            },
        Expr::Between { expr, negated, low, high } =>
            Expr::Between {
                expr: map_boxed_expr(expr),
                negated,
                low: map_boxed_expr(low),
                high: map_boxed_expr(high),
            },
        Expr::BinaryOp { left, op, right } =>
            Expr::BinaryOp {
                left: map_boxed_expr(left),
                op,
                right: map_boxed_expr(right),
            },
        Expr::UnaryOp { op, expr } =>
            Expr::UnaryOp {
                op,
                expr: map_boxed_expr(expr),
            },
        Expr::Cast { expr, data_type } =>
            Expr::Cast {
                expr: map_boxed_expr(expr),
                data_type,
            },
        Expr::Extract { field, expr } =>
            Expr::Extract {
                field,
                expr: map_boxed_expr(expr),
            },
        Expr::Collate { expr, collation } =>
            Expr::Collate {
                expr: map_boxed_expr(expr),
                collation,
            },
        Expr::Nested(expr) => Expr::Nested(map_boxed_expr(expr)),
        Expr::Exists(query) => Expr::Exists(map_boxed_query(query)),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result
        } =>
            Expr::Case {
                operand: operand.map(map_boxed_expr),
                conditions: map_exprs(conditions),
                results: map_exprs(results),
                else_result: else_result.map(map_boxed_expr),
            },
        Expr::Subquery(query) => Expr::Subquery(map_boxed_query(query)),
        Expr::Value(v) => Expr::Value(canonicalize_value(v)),
        default @ _ => default,
    }
}

fn canonicalize_value(value: Value) -> Value {
    match value {
        #[cfg(not(feature = "bigdecimal"))]
        Value::Number(_) => Value::Number(String::from("0")),
        #[cfg(feature = "bigdecimal")]
        Value::Number(_) => Value::Number(0),
        Value::SingleQuotedString(_) => Value::SingleQuotedString(String::from("")),
        Value::NationalStringLiteral(_) => Value::NationalStringLiteral(String::from("")),
        Value::HexStringLiteral(_) => Value::HexStringLiteral(String::from("")),
        Value::Boolean(_) => Value::Boolean(true),
        Value::Date(_) => Value::Date(String::from("1970-01-01")),
        Value::Time(_) => Value::Time(String::from("00:00:00")),
        Value::Timestamp(_) => Value::Timestamp(String::from("1970-01-01 00:00:00")),
        Value::Interval {
            value: _,
            leading_field: _,
            leading_precision: _,
            last_field: _,
            fractional_seconds_precision: _,
        } => Value::Interval {
            value: String::from("1"),
            leading_field: DateTimeField::Second,
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        },
        Value::Null => Value::Null,
    }
}
