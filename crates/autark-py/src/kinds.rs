use autark::mpera::op::{JoinKind, ReduceKind};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

pub(crate) fn reduce_kind(kind: &str) -> PyResult<ReduceKind> {
    Ok(match kind {
        "sum" => ReduceKind::Sum,
        "product" => ReduceKind::Product,
        "mean" => ReduceKind::Mean,
        "count" | "len" => ReduceKind::Count,
        "stdev" | "std" => ReduceKind::Stdev,
        _ => {
            return Err(PyValueError::new_err(
                "reduce kind must be one of: sum, product, mean, count, stdev",
            ));
        }
    })
}

pub(crate) fn join_kind(kind: &str) -> PyResult<JoinKind> {
    Ok(match kind {
        "inner" => JoinKind::Inner,
        "left_outer" => JoinKind::LeftOuter,
        _ => {
            return Err(PyValueError::new_err(
                "join kind must be one of: inner, left_outer",
            ));
        }
    })
}
