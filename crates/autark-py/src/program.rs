use autark::mpera::op::BinaryOpKind;
use autark::mpera::program::Program;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::error::py_err;
use crate::kinds::{join_kind, reduce_kind};

#[pyclass(name = "Program", unsendable)]
#[derive(Clone)]
pub(crate) struct PyProgram {
    frame_id: u64,
    inner: Program,
}

impl PyProgram {
    pub(crate) fn new(frame_id: u64, inner: Program) -> PyProgram {
        PyProgram { frame_id, inner }
    }

    fn ensure_same(&self, other: &PyProgram) -> PyResult<()> {
        if self.frame_id != other.frame_id {
            return Err(PyValueError::new_err(
                "programs must belong to the same OnceFrame",
            ));
        }
        Ok(())
    }

    fn map<F>(&self, f: F) -> PyResult<PyProgram>
    where
        F: FnOnce(&Program) -> PyResult<Program>,
    {
        Ok(PyProgram::new(self.frame_id, f(&self.inner)?))
    }

    fn rhs(&self, rhs: &Bound<'_, PyAny>) -> PyResult<Program> {
        if let Ok(other) = rhs.extract::<PyRef<'_, PyProgram>>() {
            self.ensure_same(&other)?;
            return Ok(other.inner.clone());
        }
        if let Ok(value) = rhs.extract::<f64>() {
            return self.inner.const_f64(value).map_err(py_err);
        }
        Err(PyValueError::new_err("expected Program or number"))
    }

    fn binary(&self, rhs: &Bound<'_, PyAny>, kind: BinaryOpKind) -> PyResult<PyProgram> {
        let rhs = self.rhs(rhs)?;
        self.map(|p| p.binaryop(rhs, kind).map_err(py_err))
    }

    fn reverse_scalar(&self, lhs: f64, kind: BinaryOpKind) -> PyResult<PyProgram> {
        let lhs = self.inner.const_f64(lhs).map_err(py_err)?;
        Ok(PyProgram::new(
            self.frame_id,
            lhs.binaryop(self.inner.clone(), kind).map_err(py_err)?,
        ))
    }
}

#[pymethods]
impl PyProgram {
    fn col(&self, name: &str) -> PyResult<PyProgram> {
        self.map(|p| p.col(name).map_err(py_err))
    }

    fn concat(&self, others: Vec<PyRef<'_, PyProgram>>) -> PyResult<PyProgram> {
        if others.is_empty() {
            return Err(PyValueError::new_err(
                "concat requires at least one program",
            ));
        }
        let mut programs = Vec::with_capacity(others.len());
        for other in others {
            self.ensure_same(&other)?;
            programs.push(other.inner.clone());
        }
        self.map(|p| p.concat(&programs).map_err(py_err))
    }

    fn slice(&self, start: isize, end: isize) -> PyResult<PyProgram> {
        self.map(|p| p.slice(start, end).map_err(py_err))
    }

    fn rolling(&self, n: usize) -> PyResult<PyProgram> {
        self.map(|p| p.rolling(n).map_err(py_err))
    }

    #[pyo3(signature = (by, ascending = true))]
    fn order_by(&self, by: PyRef<'_, PyProgram>, ascending: bool) -> PyResult<PyProgram> {
        self.ensure_same(&by)?;
        self.map(|p| p.order_by(by.inner.clone(), ascending).map_err(py_err))
    }

    fn alias(&self, name: &str) -> PyResult<PyProgram> {
        self.map(|p| p.alias(name, None).map_err(py_err))
    }

    fn filter(&self, mask: PyRef<'_, PyProgram>) -> PyResult<PyProgram> {
        self.ensure_same(&mask)?;
        self.map(|p| p.filter(mask.inner.clone()).map_err(py_err))
    }

    fn reduce(&self, kind: &str) -> PyResult<PyProgram> {
        self.map(|p| p.reduce(reduce_kind(kind)?).map_err(py_err))
    }

    fn group_by(&self, keys: PyRef<'_, PyProgram>, kind: &str) -> PyResult<PyProgram> {
        self.ensure_same(&keys)?;
        self.map(|p| {
            p.group_by(keys.inner.clone(), reduce_kind(kind)?)
                .map_err(py_err)
        })
    }

    #[pyo3(signature = (right, left_on, right_on, kind = "inner"))]
    fn join(
        &self,
        right: PyRef<'_, PyProgram>,
        left_on: PyRef<'_, PyProgram>,
        right_on: PyRef<'_, PyProgram>,
        kind: &str,
    ) -> PyResult<PyProgram> {
        self.ensure_same(&right)?;
        self.ensure_same(&left_on)?;
        self.ensure_same(&right_on)?;
        self.map(|p| {
            p.join(
                right.inner.clone(),
                left_on.inner.clone(),
                right_on.inner.clone(),
                join_kind(kind)?,
            )
            .map_err(py_err)
        })
    }

    fn __add__(&self, rhs: &Bound<'_, PyAny>) -> PyResult<PyProgram> {
        self.binary(rhs, BinaryOpKind::Add)
    }

    fn __sub__(&self, rhs: &Bound<'_, PyAny>) -> PyResult<PyProgram> {
        self.binary(rhs, BinaryOpKind::Sub)
    }

    fn __mul__(&self, rhs: &Bound<'_, PyAny>) -> PyResult<PyProgram> {
        self.binary(rhs, BinaryOpKind::Mul)
    }

    fn __truediv__(&self, rhs: &Bound<'_, PyAny>) -> PyResult<PyProgram> {
        self.binary(rhs, BinaryOpKind::Div)
    }

    fn __lt__(&self, rhs: &Bound<'_, PyAny>) -> PyResult<PyProgram> {
        self.binary(rhs, BinaryOpKind::LesserThan)
    }

    fn __gt__(&self, rhs: &Bound<'_, PyAny>) -> PyResult<PyProgram> {
        self.binary(rhs, BinaryOpKind::GreaterThan)
    }

    fn __le__(&self, rhs: &Bound<'_, PyAny>) -> PyResult<PyProgram> {
        self.binary(rhs, BinaryOpKind::LesserEquals)
    }

    fn __ge__(&self, rhs: &Bound<'_, PyAny>) -> PyResult<PyProgram> {
        self.binary(rhs, BinaryOpKind::GreaterEquals)
    }

    fn __eq__(&self, rhs: &Bound<'_, PyAny>) -> PyResult<PyProgram> {
        self.binary(rhs, BinaryOpKind::Equals)
    }

    fn __ne__(&self, rhs: &Bound<'_, PyAny>) -> PyResult<PyProgram> {
        self.binary(rhs, BinaryOpKind::NotEquals)
    }

    fn __and__(&self, rhs: &Bound<'_, PyAny>) -> PyResult<PyProgram> {
        self.binary(rhs, BinaryOpKind::And)
    }

    fn __or__(&self, rhs: &Bound<'_, PyAny>) -> PyResult<PyProgram> {
        self.binary(rhs, BinaryOpKind::Or)
    }

    fn __radd__(&self, lhs: f64) -> PyResult<PyProgram> {
        self.reverse_scalar(lhs, BinaryOpKind::Add)
    }

    fn __rsub__(&self, lhs: f64) -> PyResult<PyProgram> {
        self.reverse_scalar(lhs, BinaryOpKind::Sub)
    }

    fn __rmul__(&self, lhs: f64) -> PyResult<PyProgram> {
        self.reverse_scalar(lhs, BinaryOpKind::Mul)
    }

    fn __rtruediv__(&self, lhs: f64) -> PyResult<PyProgram> {
        self.reverse_scalar(lhs, BinaryOpKind::Div)
    }
}
