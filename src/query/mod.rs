use crate::types::Value;

#[derive(Debug, Clone)]
pub enum Condition {
    Equal(String, Value),
    GreaterThan(String, Value),
    LessThan(String, Value),
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
}