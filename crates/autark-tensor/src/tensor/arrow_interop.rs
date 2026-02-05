use std::sync::Arc;

use arrow::array::{ArrayRef, *};
use arrow::datatypes::{DataType, *};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::{Error, Result};
use with_tinygrad::with_tinygrad;

impl super::Tensor {
    pub fn try_from_arrow_1d(arr: &ArrayRef, name: &str) -> Result<Self> {
        with_tinygrad(|py| {
            if arr.null_count() != 0 {
                return Err(PyErr::new::<PyValueError, _>(
                    "Arrow array has nulls; tinygrad Tensor has no nulls",
                )
                .into());
            }

            let tensor_cls = py.import("tinygrad")?.getattr("Tensor")?;
            let py_list = Self::arrow_to_py_list(py, arr).map_err(|_| {
                PyErr::new::<PyValueError, _>(format!(
                    "unsupported Arrow dtype for '{}': {:?}",
                    name,
                    arr.data_type()
                ))
            })?;

            Ok(Self {
                inner: Arc::new(tensor_cls.call((py_list,), None)?.into()),
            })
        })
    }

    fn arrow_to_py_list(py: Python, arr: &dyn Array) -> Result<Py<PyList>> {
        match arr.data_type() {
            DataType::Int8 => {
                let a = arr.as_primitive::<Int8Type>();
                Ok(PyList::new(py, a.values().iter().copied())?.unbind())
            }
            DataType::Int16 => {
                let a = arr.as_primitive::<Int16Type>();
                Ok(PyList::new(py, a.values().iter().copied())?.unbind())
            }
            DataType::Int32 => {
                let a = arr.as_primitive::<Int32Type>();
                Ok(PyList::new(py, a.values().iter().copied())?.unbind())
            }
            DataType::Int64 => {
                let a = arr.as_primitive::<Int64Type>();
                Ok(PyList::new(py, a.values().iter().copied())?.unbind())
            }
            DataType::Float32 => {
                let a = arr.as_primitive::<Float32Type>();
                Ok(PyList::new(py, a.values().iter().copied())?.unbind())
            }
            DataType::Float64 => {
                let a = arr.as_primitive::<Float64Type>();
                Ok(PyList::new(py, a.values().iter().copied())?.unbind())
            }
            DataType::Boolean => {
                let a = arr.as_boolean();
                let values: Vec<bool> = (0..a.len()).map(|i| a.value(i)).collect();
                Ok(PyList::new(py, values)?.unbind())
            }
            _ => Err(Error::UnsupportedArrowDataType),
        }
    }
}
