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
    LessThanOrEqual(String, Value),
    GreaterThanOrEqual(String, Value),
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
    Delete {
        table: String,
        condition: Option<Condition>,
    },
    DropTable {
        table: String,
    },
    MakeIndex {
        table: String,
        column: String,
    },
    DropIndex {
        table: String,
        column: String,
    },
    StartTransaction,
    Commit,
    Rollback,
}

pub fn collect_condition_columns(condition: &Condition) -> std::collections::HashSet<String> {
    let mut columns = std::collections::HashSet::new();
    match condition {
        Condition::Equal(col, _) | 
        Condition::GreaterThan(col, _) | 
        Condition::LessThan(col, _) | 
        Condition::LessThanOrEqual(col, _) | 
        Condition::GreaterThanOrEqual(col, _) => {
            columns.insert(col.clone());
        }
        Condition::And(left, right) | Condition::Or(left, right) => {
            columns.extend(collect_condition_columns(left));
            columns.extend(collect_condition_columns(right));
        }
    }
    columns
}