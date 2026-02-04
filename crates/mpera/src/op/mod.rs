pub mod op;
pub mod oppool;
pub mod opref;

pub use op::*;
pub use oppool::OpPool;
pub use opref::OpRef;

#[derive(Debug, Clone)]
pub enum BinaryOpKind {
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    // Comparison
    LesserThan,
    GreaterThan,
    LesserEquals,
    GreaterEquals,
    Equals,
    NotEquals,
    And,
    Or,
}

#[derive(Debug, Clone)]
pub enum ReduceOpKind {
    Sum,
    Product,
    Mean,
    Count,
    Stdev,
}

impl ReduceOpKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ReduceOpKind::Sum => "sum",
            ReduceOpKind::Product => "product",
            ReduceOpKind::Mean => "mean",
            ReduceOpKind::Count => "len",
            ReduceOpKind::Stdev => "std",
        }
    }
}

#[derive(Debug, Clone)]
pub enum JoinKind {
    Inner,
    LeftOuter,
}

impl JoinKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            JoinKind::Inner => "inner",
            JoinKind::LeftOuter => "left_outer",
        }
    }
}
