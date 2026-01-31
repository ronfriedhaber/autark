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
}

#[derive(Debug, Clone)]
pub enum ReduceOpKind {
    Sum,
    Product,
    Mean,
    Count,
    Stdev,
}

#[derive(Debug, Clone)]
pub enum JoinKind {
    Left,
}
