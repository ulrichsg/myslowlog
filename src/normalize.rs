use std::fmt;

use sqlparser::ast::{
    Assignment, Distinct, Expr, GroupByExpr, Join, JoinConstraint, JoinOperator, Offset,
    OrderByExpr, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value,
    Values,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use crate::log_parser::LogEntry;

#[derive(Clone, Debug)]
pub struct NormalizedLogEntry {
    pub entry: LogEntry,
    pub normalized_query: String,
}

impl fmt::Display for NormalizedLogEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let entry = &self.entry;
        write!(
            f,
            "Executed in {:.3} seconds returning {} row(s) for {}@{}\n{}",
            entry.query_time.whole_microseconds() as f64 / 1_000_000.0,
            entry.rows_sent,
            entry.user,
            entry.host,
            &self.normalized_query
        )
    }
}

pub fn normalize(entry: LogEntry) -> NormalizedLogEntry {
    let dialect = MySqlDialect {};
    let parser_result = Parser::parse_sql(&dialect, &entry.query);
    let normalized_query = match parser_result {
        Ok(ast) => normalize_ast(&ast),
        Err(err) => format!("Unparseable statement: {} ({})", &entry.query, &err),
    };

    NormalizedLogEntry { entry, normalized_query: normalized_query.clone() }
}

fn normalize_ast(ast: &[Statement]) -> String {
    ast.iter()
        .map(normalize_stmt)
        .map(|stmt| format!("{};", stmt))
        .fold(String::new(), |acc, item| acc + " " + &item)
}

fn normalize_stmt(stmt: &Statement) -> Statement {
    match stmt {
        Statement::Query(query) => Statement::Query(Box::new(normalize_query(query))),
        Statement::Insert { .. } => normalize_insert(stmt),
        Statement::Update { .. } => normalize_update(stmt),
        Statement::Delete { .. } => normalize_delete(stmt),
        default => default.clone(),
    }
}

fn normalize_query(query: &Query) -> Query {
    let order_by = query.order_by.iter().map(normalize_order_by).collect();
    Query {
        with: query.with.clone(),
        body: Box::new(normalize_set_expr(&query.body)),
        order_by,
        limit: query.limit.clone(),
        offset: query.offset.as_ref().map(normalize_offset),
        fetch: query.fetch.clone(),
        locks: query.locks.clone(),
    }
}

fn normalize_insert(stmt: &Statement) -> Statement {
    match stmt {
        Statement::Insert { into, table_name, columns, source, on, .. } => Statement::Insert {
            into: *into,
            table_name: table_name.to_owned(),
            columns: columns.clone(),
            source: Box::new(normalize_query(source)),
            on: on.clone(),
            returning: None,
            partitioned: None,
            or: None,
            after_columns: vec![],
            overwrite: false,
            table: false,
        },
        _ => panic!("A glitch in the matrix has occurred"),
    }
}

fn normalize_update(stmt: &Statement) -> Statement {
    match stmt {
        Statement::Update { table, assignments, from, selection, returning } => Statement::Update {
            table: normalize_table_with_joins(table),
            assignments: assignments.iter().map(normalize_assignment).collect(),
            from: from.as_ref().map(normalize_table_with_joins),
            selection: selection.as_ref().map(normalize_expr),
            returning: returning.clone(),
        },
        _ => panic!("A glitch in the matrix has occurred"),
    }
}

fn normalize_assignment(assignment: &Assignment) -> Assignment {
    Assignment { id: assignment.id.clone(), value: normalize_expr(&assignment.value) }
}

fn normalize_delete(stmt: &Statement) -> Statement {
    match stmt {
        Statement::Delete { tables, from, using, selection, returning, order_by, limit } => {
            Statement::Delete {
                tables: tables.clone(),
                from: from.clone(),
                using: using.clone(),
                selection: selection.as_ref().map(normalize_expr),
                returning: returning.clone(),
                order_by: order_by.iter().map(normalize_order_by).collect(),
                limit: limit.clone(),
            }
        }
        _ => panic!("A glitch in the matrix has occurred"),
    }
}

fn normalize_offset(offset: &Offset) -> Offset {
    Offset { value: normalize_expr(&offset.value), rows: offset.rows }
}

fn normalize_set_expr(set_expr: &SetExpr) -> SetExpr {
    match set_expr {
        SetExpr::Select(select) => SetExpr::Select(Box::new(normalize_select(select))),
        SetExpr::Query(query) => SetExpr::Query(Box::new(normalize_query(query))),
        SetExpr::SetOperation { op, set_quantifier, left, right } => SetExpr::SetOperation {
            op: *op,
            set_quantifier: *set_quantifier,
            left: Box::new(normalize_set_expr(left)),
            right: Box::new(normalize_set_expr(right)),
        },
        SetExpr::Values(values) => SetExpr::Values(normalize_values(values)),
        SetExpr::Insert(stmt) => SetExpr::Insert(normalize_stmt(stmt)),
        SetExpr::Update(stmt) => SetExpr::Update(normalize_stmt(stmt)),
        SetExpr::Table(table) => SetExpr::Table(table.clone()),
    }
}

fn normalize_select(select: &Select) -> Select {
    let projection = select.projection.iter().map(normalize_select_item).collect();
    let from = select.from.iter().map(normalize_table_with_joins).collect();

    Select {
        distinct: select.distinct.as_ref().map(normalize_distinct),
        top: select.top.clone(),
        projection,
        into: select.into.clone(),
        from,
        lateral_views: select.lateral_views.clone(),
        selection: select.selection.as_ref().map(normalize_expr),
        group_by: normalize_group_by(&select.group_by),
        cluster_by: select.cluster_by.clone(),
        distribute_by: select.distribute_by.clone(),
        sort_by: select.sort_by.clone(),
        having: select.having.as_ref().map(normalize_expr),
        qualify: select.qualify.as_ref().map(normalize_expr),
        named_window: select.named_window.clone(),
    }
}

fn normalize_distinct(distinct: &Distinct) -> Distinct {
    match distinct {
        Distinct::Distinct => Distinct::Distinct,
        Distinct::On(exprs) => {
            let normalized_exprs = exprs.iter().map(normalize_expr).collect();
            Distinct::On(normalized_exprs)
        }
    }
}

fn normalize_select_item(item: &SelectItem) -> SelectItem {
    match item {
        SelectItem::UnnamedExpr(expr) => SelectItem::UnnamedExpr(normalize_expr(expr)),
        SelectItem::ExprWithAlias { expr, alias } => {
            SelectItem::ExprWithAlias { expr: normalize_expr(expr), alias: alias.clone() }
        }
        qw @ SelectItem::QualifiedWildcard(_, _) => qw.clone(),
        w @ SelectItem::Wildcard(_) => w.clone(),
    }
}

fn normalize_table_with_joins(twj: &TableWithJoins) -> TableWithJoins {
    let joins = twj.joins.iter().map(normalize_join).collect();
    TableWithJoins { relation: normalize_table_factor(&twj.relation), joins }
}

fn normalize_join(join: &Join) -> Join {
    Join {
        relation: normalize_table_factor(&join.relation),
        join_operator: normalize_join_operator(&join.join_operator),
    }
}

fn normalize_table_factor(tf: &TableFactor) -> TableFactor {
    match tf {
        TableFactor::NestedJoin { table_with_joins, alias } => TableFactor::NestedJoin {
            table_with_joins: Box::new(normalize_table_with_joins(table_with_joins)),
            alias: alias.clone(),
        },
        TableFactor::Derived { lateral, subquery, alias } => TableFactor::Derived {
            lateral: *lateral,
            subquery: Box::new(normalize_query(subquery)),
            alias: alias.clone(),
        },
        default => default.clone(),
    }
}

fn normalize_join_operator(operator: &JoinOperator) -> JoinOperator {
    match operator {
        JoinOperator::Inner(constraint) => {
            JoinOperator::Inner(normalize_join_constraint(constraint))
        }
        JoinOperator::LeftOuter(constraint) => {
            JoinOperator::LeftOuter(normalize_join_constraint(constraint))
        }
        JoinOperator::RightOuter(constraint) => {
            JoinOperator::RightOuter(normalize_join_constraint(constraint))
        }
        JoinOperator::FullOuter(constraint) => {
            JoinOperator::FullOuter(normalize_join_constraint(constraint))
        }
        default => default.clone(),
    }
}

fn normalize_join_constraint(constraint: &JoinConstraint) -> JoinConstraint {
    match constraint {
        JoinConstraint::On(expr) => JoinConstraint::On(normalize_expr(expr)),
        default => default.clone(),
    }
}

fn normalize_values(values: &Values) -> Values {
    let rows = values.rows.iter().map(|vec| vec.iter().map(normalize_expr).collect()).collect();
    Values { explicit_row: values.explicit_row, rows }
}

fn normalize_order_by(order_by: &OrderByExpr) -> OrderByExpr {
    OrderByExpr {
        expr: normalize_expr(&order_by.expr),
        asc: order_by.asc,
        nulls_first: order_by.nulls_first,
    }
}

fn normalize_group_by(group_by: &GroupByExpr) -> GroupByExpr {
    match group_by {
        GroupByExpr::All => GroupByExpr::All,
        GroupByExpr::Expressions(exprs) => {
            let normalized_exprs = exprs.iter().map(normalize_expr).collect();
            GroupByExpr::Expressions(normalized_exprs)
        }
    }
}

fn normalize_expr(expr: &Expr) -> Expr {
    let map_exprs = |exprs: &Vec<Expr>| exprs.iter().map(normalize_expr).collect();
    let map_boxed_expr = |boxed: &Expr| Box::new(normalize_expr(boxed));
    let map_boxed_query = |boxed: &Query| Box::new(normalize_query(boxed));
    match expr {
        Expr::IsNull(e) => Expr::IsNull(map_boxed_expr(e)),
        Expr::IsNotNull(e) => Expr::IsNotNull(map_boxed_expr(e)),
        // reduce all lists down to 1 element
        Expr::InList { expr, list, negated } => Expr::InList {
            expr: map_boxed_expr(expr),
            list: match list.first() {
                Some(expr) => vec![normalize_expr(expr)],
                None => Vec::new(),
            },
            negated: *negated,
        },
        Expr::InSubquery { expr, subquery, negated } => Expr::InSubquery {
            expr: map_boxed_expr(expr),
            subquery: map_boxed_query(subquery),
            negated: *negated,
        },
        Expr::Between { expr, negated, low, high } => Expr::Between {
            expr: map_boxed_expr(expr),
            negated: *negated,
            low: map_boxed_expr(low),
            high: map_boxed_expr(high),
        },
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: map_boxed_expr(left),
            op: op.clone(),
            right: map_boxed_expr(right),
        },
        Expr::UnaryOp { op, expr } => Expr::UnaryOp { op: *op, expr: map_boxed_expr(expr) },
        Expr::Cast { expr, data_type } => {
            Expr::Cast { expr: map_boxed_expr(expr), data_type: data_type.clone() }
        }
        Expr::Extract { field, expr } => {
            Expr::Extract { field: *field, expr: map_boxed_expr(expr) }
        }
        Expr::Collate { expr, collation } => {
            Expr::Collate { expr: map_boxed_expr(expr), collation: collation.clone() }
        }
        Expr::Nested(expr) => Expr::Nested(map_boxed_expr(expr)),
        Expr::Exists { subquery, negated } => {
            Expr::Exists { subquery: map_boxed_query(subquery), negated: *negated }
        }
        Expr::Case { operand, conditions, results, else_result } => Expr::Case {
            operand: operand.as_ref().map(|expr| map_boxed_expr(expr)),
            conditions: map_exprs(conditions),
            results: map_exprs(results),
            else_result: else_result.as_ref().map(|expr| map_boxed_expr(expr)),
        },
        Expr::Like { expr, negated, escape_char, pattern } => Expr::Like {
            expr: map_boxed_expr(expr),
            negated: *negated,
            escape_char: escape_char.to_owned(),
            pattern: map_boxed_expr(pattern),
        },
        Expr::ILike { expr, negated, escape_char, pattern } => Expr::ILike {
            expr: map_boxed_expr(expr),
            negated: *negated,
            escape_char: escape_char.to_owned(),
            pattern: map_boxed_expr(pattern),
        },
        Expr::Subquery(query) => Expr::Subquery(map_boxed_query(query)),
        Expr::Value(v) => Expr::Value(normalize_value(v)),
        default => default.clone(),
    }
}

fn normalize_value(_value: &Value) -> Value {
    Value::Placeholder("?".to_owned())
}
