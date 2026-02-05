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
pub enum ReduceKind {
    Sum,
    Product,
    Mean,
    Count,
    Stdev,
}

impl ReduceKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ReduceKind::Sum => "sum",
            ReduceKind::Product => "product",
            ReduceKind::Mean => "mean",
            ReduceKind::Count => "len",
            ReduceKind::Stdev => "std",
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
