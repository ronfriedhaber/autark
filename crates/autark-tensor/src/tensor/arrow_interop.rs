use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, *};
use arrow::datatypes::{DataType, Field, *};
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::with_tinygrad::with_tinygrad;
use crate::{Error, Result};

// use arrow_array::{Array, AsArray, types::*};
// use arrow_schema::DataType;
// use pyo3::exceptions::PyValueError;
// use pyo3::prelude::*;
// use pyo3::types::PyList;
// use std::sync::Arc;

impl super::Tensor {
    pub fn try_from_arrow_1d(arr: &ArrayRef, name: &str) -> Result<Self> {
        with_tinygrad(|py| {
            if arr.null_count() != 0 {
                return Err(PyErr::new::<PyValueError, _>(
                    "Arrow array has nulls; tinygrad Tensor has no nulls",
                ));
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

    pub fn try_into_arrow_1d(&self) -> Result<ArrayRef> {
        with_tinygrad(|py| {
            let t = self.inner.bind(py);

            let mv0 = t.call_method0("data")?;
            let fmt: String = mv0.getattr("format")?.extract()?;
            let item: usize = mv0.getattr("itemsize")?.extract()?;

            let mvb = mv0.call_method1("cast", ("B",))?;
            let raw = PyBuffer::<u8>::get(&mvb)?
                .as_slice(py)
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("non-contiguous buffer")
                })?
                .iter()
                .map(|c| c.get())
                .collect::<Vec<u8>>();

            let code = fmt.bytes().find(|b| !b"<>=@!".contains(b)).ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("empty memoryview format")
            })? as char;

            fn parse<const N: usize, T>(raw: &[u8], f: impl Fn([u8; N]) -> T) -> Vec<T> {
                raw.chunks_exact(N)
                    .map(|c| f(c.try_into().unwrap()))
                    .collect()
            }

            Ok(match (code, item) {
                ('f', 4) => Arc::new(Float32Array::from(parse::<4, _>(&raw, f32::from_le_bytes)))
                    as ArrayRef,
                ('i', 4) => {
                    Arc::new(Int32Array::from(parse::<4, _>(&raw, i32::from_le_bytes))) as ArrayRef
                }
                ('q', 8) => {
                    Arc::new(Int64Array::from(parse::<8, _>(&raw, i64::from_le_bytes))) as ArrayRef
                }
                ('B', 1) => Arc::new(UInt8Array::from(raw)) as ArrayRef,
                ('?', 1) => Arc::new(BooleanArray::from(
                    raw.into_iter().map(|b| b != 0).collect::<Vec<_>>(),
                )) as ArrayRef,
                _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                    "unsupported memoryview: format={fmt:?}, itemsize={item}"
                )))?,
            })
        })
    }
    pub fn try_into_arrow_1d_or_2d_2(&self) -> Result<Vec<ArrayRef>> {
        use arrow::array::{
            ArrayData, BooleanArray, Float32Array, Int32Array, Int64Array, UInt8Array,
        };
        use arrow::buffer::Buffer;
        use arrow::datatypes::DataType;
        use arrow::util::bit_util;
        use pyo3::exceptions::{PyTypeError, PyValueError};
        use pyo3::types::PyBytes;

        with_tinygrad(|py| {
            let t = self.inner.bind(py);

            let shape: Vec<usize> = t.getattr("shape")?.extract()?;
            let (rows, cols) = match shape.as_slice() {
                [n] => (*n, 1usize),
                [n, m] => (*n, *m),
                _ => {
                    return Err(PyErr::new::<PyValueError, _>(format!(
                        "expected tensor shape (n,) or (n,m), got {shape:?}"
                    )));
                }
            };

            let nelems = rows
                .checked_mul(cols)
                .ok_or_else(|| PyErr::new::<PyValueError, _>("shape overflow"))?;

            let mv0 = t.call_method0("data")?; // memoryview
            let fmt: String = mv0.getattr("format")?.extract()?;
            let itemsize: usize = mv0.getattr("itemsize")?.extract()?;

            let code = fmt
                .bytes()
                .find(|b| !b"<>=@!".contains(b))
                .ok_or_else(|| PyErr::new::<PyTypeError, _>("empty memoryview format"))?
                as char;
            let mvb = mv0.call_method1("cast", ("B",))?;
            let buf = PyBuffer::<u8>::get(&mvb)?;
            if buf.as_slice(py).is_none() {
                return Err(PyErr::new::<PyValueError, _>("non-contiguous buffer"));
            }
            let binding = mvb.call_method0("tobytes")?;
            let pybytes = binding.downcast::<PyBytes>()?;
            let raw: Vec<u8> = pybytes.as_bytes().to_vec();

            let expected = nelems
                .checked_mul(itemsize)
                .ok_or_else(|| PyErr::new::<PyValueError, _>("byte length overflow"))?;
            if raw.len() != expected {
                return Err(PyErr::new::<PyValueError, _>(format!(
                    "buffer byte length mismatch: got {}, expected {} (nelems={}, itemsize={})",
                    raw.len(),
                    expected,
                    nelems,
                    itemsize
                )));
            }

            fn make_row_arrays(array: ArrayRef, rows: usize, cols: usize) -> Vec<ArrayRef> {
                if cols == 1 {
                    vec![array]
                } else {
                    (0..rows).map(|i| array.slice(i * cols, cols)).collect()
                }
            }

            let out: Vec<ArrayRef> = match (code, itemsize) {
                ('f', 4) => {
                    let buffer = Buffer::from(raw); // takes ownership, no extra copy
                    let data = ArrayData::builder(DataType::Float32)
                        .len(nelems)
                        .add_buffer(buffer)
                        .build()
                        .map_err(|e| {
                            PyErr::new::<PyValueError, _>(format!("arrow build error: {e}"))
                        })?;
                    let arr = Arc::new(Float32Array::from(data)) as ArrayRef;
                    make_row_arrays(arr, rows, cols)
                }

                ('i', 4) => {
                    let buffer = Buffer::from(raw);
                    let data = ArrayData::builder(DataType::Int32)
                        .len(nelems)
                        .add_buffer(buffer)
                        .build()
                        .map_err(|e| {
                            PyErr::new::<PyValueError, _>(format!("arrow build error: {e}"))
                        })?;
                    let arr = Arc::new(Int32Array::from(data)) as ArrayRef;
                    make_row_arrays(arr, rows, cols)
                }

                ('q', 8) => {
                    let buffer = Buffer::from(raw);
                    let data = ArrayData::builder(DataType::Int64)
                        .len(nelems)
                        .add_buffer(buffer)
                        .build()
                        .map_err(|e| {
                            PyErr::new::<PyValueError, _>(format!("arrow build error: {e}"))
                        })?;
                    let arr = Arc::new(Int64Array::from(data)) as ArrayRef;
                    make_row_arrays(arr, rows, cols)
                }

                ('B', 1) => {
                    let arr = Arc::new(UInt8Array::from(raw)) as ArrayRef;
                    make_row_arrays(arr, rows, cols)
                }

                ('?', 1) => {
                    let mut bits = vec![0u8; (nelems + 7) / 8];
                    for (i, &b) in raw.iter().enumerate() {
                        if b != 0 {
                            bit_util::set_bit(&mut bits, i);
                        }
                    }
                    let values = Buffer::from(bits);
                    let data = ArrayData::builder(DataType::Boolean)
                        .len(nelems)
                        .add_buffer(values)
                        .build()
                        .map_err(|e| {
                            PyErr::new::<PyValueError, _>(format!("arrow build error: {e}"))
                        })?;
                    let arr = Arc::new(BooleanArray::from(data)) as ArrayRef;
                    make_row_arrays(arr, rows, cols)
                }

                _ => {
                    return Err(PyErr::new::<PyTypeError, _>(format!(
                        "unsupported memoryview: format={fmt:?}, itemsize={itemsize}"
                    )));
                }
            };

            Ok(out)
        })
    }
}
