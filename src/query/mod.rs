use crate::types::{DataType, Value};
use serde::{Deserialize, Serialize};

pub mod evaluator;
pub mod parser;
pub mod planner;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Condition {
    Equal(String, Value),
    GreaterThan(String, Value),
    LessThan(String, Value),
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Aggregation {
    Count,
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Query {
    Select {
        table: String,
        columns: Vec<String>,
        condition: Option<Condition>,
    },
    SelectAggregate {
        table: String,
        aggregations: Vec<Aggregation>,
        condition: Option<Condition>,
    },
    Join {
        left_table: String,
        right_table: String,
        left_column: String,
        right_column: String,
        columns: Vec<String>,
        condition: Option<Condition>,
    },
    Insert {
        table: String,
        values: Vec<Value>,
    },
    CreateTable {
        table: String,
        columns: Vec<(String, DataType)>,
    },
}

pub fn collect_condition_columns(condition: &Condition) -> std::collections::HashSet<String> {
    let mut columns = std::collections::HashSet::new();
    match condition {
        Condition::Equal(col, _) | Condition::GreaterThan(col, _) | Condition::LessThan(col, _) => {
            columns.insert(col.clone());
        }
        Condition::And(left, right) | Condition::Or(left, right) => {
            columns.extend(collect_condition_columns(left));
            columns.extend(collect_condition_columns(right));
        }
    }
    columns
}