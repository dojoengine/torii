use async_graphql::connection::PageInfo;
use starknet::core::types::Felt;
use torii_proto::{
    Clause, CompositeClause, KeysClause, LogicalOperator, OrderBy, OrderDirection, Page,
    Pagination, PaginationDirection, PatternMatching, Query,
};

use crate::object::connection::ConnectionArguments;
use crate::query::filter::Filter;
use crate::query::order::{Direction, Order};

pub fn connection_args_to_pagination(
    connection: &ConnectionArguments,
    order: &Option<Order>,
) -> Pagination {
    let limit = connection
        .first
        .or(connection.last)
        .or(connection.limit)
        .map(|l| l as u32);

    let cursor = connection.after.clone().or(connection.before.clone());

    let direction = match (&connection.first, &connection.last, order) {
        (Some(_), _, _) => PaginationDirection::Forward,
        (_, Some(_), _) => PaginationDirection::Backward,
        (_, _, Some(order)) => match order.direction {
            Direction::Asc => PaginationDirection::Forward,
            Direction::Desc => PaginationDirection::Backward,
        },
        _ => PaginationDirection::Forward,
    };

    let order_by = order.as_ref().map(|o| OrderBy {
        model: String::new(),
        member: o.field.clone(),
        direction: match o.direction {
            Direction::Asc => OrderDirection::Asc,
            Direction::Desc => OrderDirection::Desc,
        },
    });

    Pagination {
        limit,
        cursor,
        direction,
        order_by: order_by.map_or(vec![], |o| vec![o]),
    }
}

pub fn filters_to_clause(filters: &Option<Vec<Filter>>) -> Option<Clause> {
    if let Some(filters) = filters {
        if filters.is_empty() {
            return None;
        }

        let clauses: Vec<Clause> = filters
            .iter()
            .map(|filter| {
                let key_str = match &filter.value {
                    crate::query::filter::FilterValue::Int(i) => i.to_string(),
                    crate::query::filter::FilterValue::String(s) => s.clone(),
                    crate::query::filter::FilterValue::List(list) => list
                        .iter()
                        .map(|v| match v {
                            crate::query::filter::FilterValue::Int(i) => i.to_string(),
                            crate::query::filter::FilterValue::String(s) => s.clone(),
                            crate::query::filter::FilterValue::List(_) => unreachable!(),
                        })
                        .collect::<Vec<_>>()
                        .join(","),
                };
                Clause::Keys(KeysClause {
                    keys: vec![Some(Felt::from_hex(&key_str).unwrap_or_default())],
                    pattern_matching: PatternMatching::FixedLen,
                    models: vec![],
                })
            })
            .collect();

        if clauses.len() == 1 {
            Some(clauses.into_iter().next().unwrap())
        } else {
            Some(Clause::Composite(CompositeClause {
                operator: LogicalOperator::And,
                clauses,
            }))
        }
    } else {
        None
    }
}

pub fn keys_to_clause(keys: &Option<Vec<String>>) -> Option<Clause> {
    if let Some(keys) = keys {
        if keys.is_empty() {
            return None;
        }

        let keys_pattern = keys
            .iter()
            .map(|key| {
                if key == "*" {
                    "([^/]*)".to_string()
                } else {
                    regex::escape(key)
                }
            })
            .collect::<Vec<_>>()
            .join("/");

        Some(Clause::Keys(KeysClause {
            keys: vec![Some(
                Felt::from_hex(&format!("^{}.*", keys_pattern)).unwrap_or_default(),
            )],
            pattern_matching: PatternMatching::VariableLen,
            models: vec![],
        }))
    } else {
        None
    }
}

pub fn page_to_connection<T>(
    page: Page<T>,
    connection: &ConnectionArguments,
    _total_count: i64,
) -> (Vec<T>, PageInfo) {
    let has_next_page = page.next_cursor.is_some();
    let has_previous_page = connection.after.is_some() || connection.before.is_some();

    let start_cursor = if !page.items.is_empty() {
        page.next_cursor.clone()
    } else {
        None
    };

    let end_cursor = page.next_cursor;

    let page_info = PageInfo {
        has_previous_page,
        has_next_page,
        start_cursor,
        end_cursor,
    };

    (page.items, page_info)
}

pub fn build_query(
    keys: &Option<Vec<String>>,
    filters: &Option<Vec<Filter>>,
    connection: &ConnectionArguments,
    order: &Option<Order>,
    models: Option<Vec<String>>,
    historical: bool,
) -> Query {
    let pagination = connection_args_to_pagination(connection, order);

    let mut clauses = Vec::new();

    if let Some(keys_clause) = keys_to_clause(keys) {
        clauses.push(keys_clause);
    }

    if let Some(filters_clause) = filters_to_clause(filters) {
        clauses.push(filters_clause);
    }

    let clause = if clauses.is_empty() {
        None
    } else if clauses.len() == 1 {
        Some(clauses.into_iter().next().unwrap())
    } else {
        Some(Clause::Composite(CompositeClause {
            operator: LogicalOperator::And,
            clauses,
        }))
    };

    Query {
        clause,
        pagination,
        models: models.unwrap_or_default(),
        historical,
        no_hashed_keys: false,
    }
}
