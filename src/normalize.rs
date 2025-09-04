use std::fmt;

use sqlparser::ast::{
    Assignment, CaseWhen, Delete, Distinct, Expr, GroupByExpr, Insert, Join, JoinConstraint,
    JoinOperator, OrderByExpr, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins, Value, ValueWithSpan, Values,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Span;

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
        Statement::Insert(insert) => Statement::Insert(normalize_insert(insert)),
        Statement::Update { .. } => normalize_update(stmt),
        Statement::Delete(delete) => Statement::Delete(normalize_delete(delete)),
        default => default.clone(),
    }
}

fn normalize_query(query: &Query) -> Query {
    Query {
        with: query.with.clone(),
        body: Box::new(normalize_set_expr(&query.body)),
        order_by: query.order_by.to_owned(),
        limit_clause: query.limit_clause.to_owned(),
        fetch: query.fetch.clone(),
        locks: query.locks.clone(),
        for_clause: query.for_clause.to_owned(),
        settings: query.settings.to_owned(),
        format_clause: query.format_clause.to_owned(),
        pipe_operators: query.pipe_operators.to_owned(),
    }
}

fn normalize_insert(insert: &Insert) -> Insert {
    Insert {
        into: insert.into,
        columns: insert.columns.to_owned(),
        source: insert.source.to_owned(),
        on: insert.on.to_owned(),
        returning: insert.returning.to_owned(),
        replace_into: insert.replace_into,
        priority: insert.priority.to_owned(),
        insert_alias: insert.insert_alias.to_owned(),
        settings: insert.settings.to_owned(),
        partitioned: insert.partitioned.to_owned(),
        or: insert.or.to_owned(),
        after_columns: insert.after_columns.to_owned(),
        overwrite: insert.overwrite.to_owned(),
        table: insert.table.to_owned(),
        ignore: false,
        table_alias: None,
        assignments: vec![],
        has_table_keyword: false,
        format_clause: None,
    }
}

fn normalize_update(stmt: &Statement) -> Statement {
    match stmt {
        Statement::Update { table, assignments, from, selection, returning, or } => {
            Statement::Update {
                table: normalize_table_with_joins(table),
                assignments: assignments.iter().map(normalize_assignment).collect(),
                from: from.to_owned(),
                selection: selection.as_ref().map(normalize_expr),
                returning: returning.clone(),
                or: or.to_owned(),
            }
        }
        _ => panic!("A glitch in the matrix has occurred"),
    }
}

fn normalize_assignment(assignment: &Assignment) -> Assignment {
    Assignment { target: assignment.target.to_owned(), value: normalize_expr(&assignment.value) }
}

fn normalize_delete(delete: &Delete) -> Delete {
    Delete {
        tables: delete.tables.clone(),
        from: delete.from.clone(),
        using: delete.using.clone(),
        selection: delete.selection.as_ref().map(normalize_expr),
        returning: delete.returning.clone(),
        order_by: delete.order_by.iter().map(normalize_order_by).collect(),
        limit: delete.limit.clone(),
    }
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
        SetExpr::Delete(stmt) => SetExpr::Delete(normalize_stmt(stmt)),
    }
}

fn normalize_select(select: &Select) -> Select {
    let projection = select.projection.iter().map(normalize_select_item).collect();
    let from = select.from.iter().map(normalize_table_with_joins).collect();

    Select {
        select_token: select.select_token.to_owned(),
        distinct: select.distinct.as_ref().map(normalize_distinct),
        top: select.top.clone(),
        top_before_distinct: select.top_before_distinct,
        projection,
        exclude: select.exclude.to_owned(),
        into: select.into.clone(),
        from,
        lateral_views: select.lateral_views.clone(),
        prewhere: select.prewhere.as_ref().map(normalize_expr),
        selection: select.selection.as_ref().map(normalize_expr),
        group_by: normalize_group_by(&select.group_by),
        cluster_by: select.cluster_by.clone(),
        distribute_by: select.distribute_by.clone(),
        sort_by: select.sort_by.clone(),
        having: select.having.as_ref().map(normalize_expr),
        qualify: select.qualify.as_ref().map(normalize_expr),
        window_before_qualify: select.window_before_qualify,
        value_table_mode: select.value_table_mode.to_owned(),
        connect_by: None,
        named_window: select.named_window.clone(),
        flavor: select.flavor.to_owned(),
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
        qw @ SelectItem::QualifiedWildcard(..) => qw.clone(),
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
        global: join.global,
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
        options: order_by.options.to_owned(),
        with_fill: order_by.with_fill.to_owned(),
    }
}

fn normalize_group_by(group_by: &GroupByExpr) -> GroupByExpr {
    match group_by {
        GroupByExpr::All(modifiers) => GroupByExpr::All(modifiers.to_owned()),
        GroupByExpr::Expressions(exprs, modifiers) => {
            let normalized_exprs = exprs.iter().map(normalize_expr).collect();
            GroupByExpr::Expressions(normalized_exprs, modifiers.to_owned())
        }
    }
}

fn normalize_expr(expr: &Expr) -> Expr {
    // let map_exprs = |exprs: &Vec<Expr>|
    // exprs.iter().map(normalize_expr).collect();
    let map_boxed_expr = |boxed: &Expr| Box::new(normalize_expr(boxed));
    let map_boxed_query = |boxed: &Query| Box::new(normalize_query(boxed));

    let map_case_when = |case_when: &CaseWhen| CaseWhen {
        condition: normalize_expr(&case_when.condition),
        result: normalize_expr(&case_when.result),
    };

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
        Expr::Cast { kind, expr, data_type, format } => Expr::Cast {
            kind: kind.to_owned(),
            expr: map_boxed_expr(expr),
            data_type: data_type.to_owned(),
            format: format.to_owned(),
        },
        Expr::Extract { field, syntax, expr } => Expr::Extract {
            field: field.to_owned(),
            syntax: syntax.to_owned(),
            expr: map_boxed_expr(expr),
        },
        Expr::Collate { expr, collation } => {
            Expr::Collate { expr: map_boxed_expr(expr), collation: collation.clone() }
        }
        Expr::Nested(expr) => Expr::Nested(map_boxed_expr(expr)),
        Expr::Exists { subquery, negated } => {
            Expr::Exists { subquery: map_boxed_query(subquery), negated: *negated }
        }
        Expr::Case { case_token, end_token, operand, conditions, else_result } => Expr::Case {
            case_token: case_token.to_owned(),
            end_token: end_token.to_owned(),
            operand: operand.as_ref().map(|expr| map_boxed_expr(expr)),
            conditions: conditions.iter().map(map_case_when).collect(),
            else_result: else_result.as_ref().map(|expr| map_boxed_expr(expr)),
        },
        Expr::Like { expr, negated, any, escape_char, pattern } => Expr::Like {
            expr: map_boxed_expr(expr),
            negated: *negated,
            any: *any,
            escape_char: escape_char.to_owned(),
            pattern: map_boxed_expr(pattern),
        },
        Expr::ILike { expr, negated, any, escape_char, pattern } => Expr::ILike {
            expr: map_boxed_expr(expr),
            negated: *negated,
            any: *any,
            escape_char: escape_char.to_owned(),
            pattern: map_boxed_expr(pattern),
        },
        Expr::Subquery(query) => Expr::Subquery(map_boxed_query(query)),
        Expr::Value(v) => Expr::Value(normalize_value_with_span(v)),
        default => default.clone(),
    }
}

fn normalize_value(_value: &Value) -> Value { Value::Placeholder("?".to_owned()) }

fn normalize_value_with_span(_value: &ValueWithSpan) -> ValueWithSpan {
    ValueWithSpan { value: normalize_value(&_value.value), span: Span::empty() }
}
