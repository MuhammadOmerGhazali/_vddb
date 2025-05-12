use crate::types::{Value, DbError};

#[derive(Clone, Debug)]
pub enum Condition {
    GreaterThan(String, Value),
    Equal(String, Value),
    LessThan(String, Value),
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
}