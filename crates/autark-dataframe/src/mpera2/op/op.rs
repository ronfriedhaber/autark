use std::fmt;

use crate::op::{BinaryOpKind, OpRef, ReduceOpKind};

#[derive(Debug, Clone)]
pub enum Op {
    BinaryOp {
        kind: BinaryOpKind,
        lhs: OpRef,
        rhs: OpRef,
    },

    Reduce {
        kind: ReduceOpKind,
        on: OpRef,
    },

    Filter {
        on: OpRef,
        mask: OpRef,
    },

    Column {
        dataframe: Option<usize>,
        column: String,
    },

    Output {
        name: String,
        value: OpRef,
    },

    ConstantF64 {
        value: f64,
    },
}

// impl fmt::Display for Op {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         match self {
//             Op::BinaryOp { kind, lhs, rhs } => {
//                 write!(f, "BIN {:?} {}, {}", kind, lhs.0, rhs.0)
//             }
//             Op::Reduce { kind, on } => {
//                 write!(f, "REDUCE {:?} {}", kind, on.0)
//             }
//             Op::Filter { on, mask } => {
//                 write!(f, "FILTER ON {:?} MASK{:?}", on, mask)
//             }
//             Op::Column { dataframe, column } => {
//                 if let Some(df) = dataframe {
//                     write!(f, "COLUMN df{}.{}", df, column)
//                 } else {
//                     write!(f, "COLUMN {}", column)
//                 }
//             }
//         }
//     }
// }
