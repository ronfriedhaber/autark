use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, *};
use arrow::datatypes::Field;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::Result;
use crate::with_tinygrad::with_tinygrad;

use arrow::array::{ArrayData, BooleanArray, Float32Array, Int32Array, Int64Array, UInt8Array};
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;
use arrow::util::bit_util;

use pyo3::types::PyBytes;

impl super::Tensor {
    pub fn try_from_arrow_1d(arr: &ArrayRef, name: &str) -> Result<Self> {
        use pyo3::exceptions::PyValueError;

        with_tinygrad(|py| {
            if arr.null_count() != 0 {
                return Err(PyErr::new::<PyValueError, _>(
                    "Arrow array has nulls; tinygrad Tensor has no nulls",
                ));
            }

            let tensor_cls = py.import("tinygrad")?.getattr("Tensor")?;

            macro_rules! try_cast {
                ($ty:ty) => {
                    arr.as_any()
                        .downcast_ref::<$ty>()
                        .map(|a| PyList::new(py, a.values().iter().copied()))
                };
            }

            let list = try_cast!(Int8Array)
                .or_else(|| try_cast!(Int16Array))
                .or_else(|| try_cast!(Int32Array))
                .or_else(|| try_cast!(Int64Array))
                .or_else(|| try_cast!(Float32Array))
                .or_else(|| try_cast!(Float64Array))
                .or_else(|| {
                    arr.as_any()
                        .downcast_ref::<BooleanArray>()
                        .map(|a| PyList::new(py, (0..a.len()).map(|i| a.value(i))))
                })
                .ok_or_else(|| {
                    PyErr::new::<PyValueError, _>(format!(
                        "unsupported Arrow dtype: {:?}",
                        arr.data_type()
                    ))
                })?;

            Ok(Self {
                inner: Arc::new(tensor_cls.call((list.unwrap(),), None)?.into()),
            })
        })
    }

    pub fn try_into_arrow_1d(&self) -> Result<ArrayRef> {
        with_tinygrad(|py| {
            let t = self.inner.bind(py);

            let mv0 = t.call_method0("data")?; // memoryview
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

            // strip optional endian/alignment prefix (< > = @ !)
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
        with_tinygrad(|py| {
            let t = self.inner.bind(py);

            let shape: Vec<usize> = t.getattr("shape")?.extract()?;
            let (rows, cols) = match shape.as_slice() {
                [n] => (*n, 1),
                [n, m] => (*n, *m),
                _ => {
                    return Err(PyValueError::new_err(format!(
                        "expected tensor shape (n,) or (n,m), got {shape:?}"
                    )));
                }
            };
            let nelems = rows
                .checked_mul(cols)
                .ok_or_else(|| PyValueError::new_err("shape overflow"))?;

            let mv = t.call_method0("data")?;
            let fmt: String = mv.getattr("format")?.extract()?;
            let itemsize: usize = mv.getattr("itemsize")?.extract()?;
            let code = fmt
                .bytes()
                .find(|b| !b"<>=@!".contains(b))
                .ok_or_else(|| PyTypeError::new_err("empty memoryview format"))?
                as char;

            let raw: Vec<u8> = mv
                .call_method1("cast", ("B",))?
                .call_method0("tobytes")?
                .downcast::<PyBytes>()?
                .as_bytes()
                .to_vec();

            let expected = nelems
                .checked_mul(itemsize)
                .ok_or_else(|| PyValueError::new_err("byte length overflow"))?;
            if raw.len() != expected {
                return Err(PyValueError::new_err(format!(
                    "buffer byte length mismatch: got {}, expected {} (nelems={}, itemsize={})",
                    raw.len(),
                    expected,
                    nelems,
                    itemsize
                )));
            }

            let chunk = |arr: ArrayRef| -> Vec<ArrayRef> {
                if cols == 1 {
                    vec![arr]
                } else {
                    (0..rows).map(|i| arr.slice(i * cols, cols)).collect()
                }
            };

            macro_rules! primitive {
                ($arr:ty, $dtype:expr) => {{
                    let data = ArrayData::builder($dtype)
                        .len(nelems)
                        .add_buffer(Buffer::from(raw))
                        .build()
                        .map_err(|e| PyValueError::new_err(format!("arrow build error: {e}")))?;
                    Ok(chunk(Arc::new(<$arr>::from(data)) as ArrayRef))
                }};
            }

            match (code, itemsize) {
                ('f', 4) => primitive!(Float32Array, DataType::Float32),
                ('i', 4) => primitive!(Int32Array, DataType::Int32),
                ('q', 8) => primitive!(Int64Array, DataType::Int64),
                ('B', 1) => Ok(chunk(Arc::new(UInt8Array::from(raw)) as ArrayRef)),

                ('?', 1) => {
                    // Boolean requires bit-packing
                    let mut bits = vec![0u8; (nelems + 7) / 8];
                    for (i, &b) in raw.iter().enumerate() {
                        if b != 0 {
                            bit_util::set_bit(&mut bits, i);
                        }
                    }
                    let data = ArrayData::builder(DataType::Boolean)
                        .len(nelems)
                        .add_buffer(Buffer::from(bits))
                        .build()
                        .map_err(|e| PyValueError::new_err(format!("arrow build error: {e}")))?;
                    Ok(chunk(Arc::new(BooleanArray::from(data)) as ArrayRef))
                }

                _ => Err(PyTypeError::new_err(format!(
                    "unsupported memoryview: format={fmt:?}, itemsize={itemsize}"
                ))),
            }
        })
    }
}
