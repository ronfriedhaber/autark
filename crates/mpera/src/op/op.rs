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
        dataframe: OpRef,
        column: String,
    },

    DataFrame {
        index: Option<usize>,
    },

    Slice {
        on: OpRef,
        start: isize,
        end: isize,
    },

    Output {
        name: String,
        value: OpRef,
    },

    ConstantF64 {
        value: f64,
    },

    Rolling {
        on: OpRef,
        n: usize,
    },

    Concat {
        who: Vec<OpRef>, // todo: Remove Alloc,
    }, // Join {
    //     left: OpRef,
    //     right: OpRef,

    //     kind: JoinKind,
    // }, // GroupBy {
    //     keys: OpRef,
    //     values: OpRef,
    // },
    //
    OrderBy {
        what: OpRef,
        by: OpRef,
        ascending: bool,
    },
}

// im
// pl fmt::Display for Op {
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
