use crate::op::{BinaryOpKind, JoinKind, OpRef, ReduceKind};

#[derive(Debug, Clone)]
pub enum Op {
    BinaryOp {
        kind: BinaryOpKind,
        lhs: OpRef,
        rhs: OpRef,
    },

    Reduce {
        kind: ReduceKind,
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
    },

    GroupBy {
        keys: OpRef,
        values: OpRef,
        kind: ReduceKind,
    },

    Join {
        left: OpRef,
        right: OpRef,
        left_on: OpRef,
        right_on: OpRef,
        kind: JoinKind,
    },

    OrderBy {
        what: OpRef,
        by: OpRef,
        ascending: bool,
    },
}
