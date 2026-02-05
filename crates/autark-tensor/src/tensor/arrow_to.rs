use crate::Tensor;

use std::sync::Arc;

use arrow::array::{ArrayRef, *};
use arrow::datatypes::DataType;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;

use crate::Result;
use with_tinygrad::with_tinygrad;

use arrow::array::{ArrayData, BooleanArray, Float32Array, Int32Array, Int64Array, UInt8Array};
use arrow::buffer::Buffer;
use arrow::util::bit_util;
use pyo3::types::PyBytes;

impl Tensor {
    pub fn try_into_arrow_1d_or_2d_2(&self) -> Result<Vec<ArrayRef>> {
        with_tinygrad(|py| {
            let t = self.inner.bind(py);
            let (rows, cols) = rows_cols(&t).unwrap();
            let nelems = rows
                .checked_mul(cols)
                .ok_or_else(|| PyErr::new::<PyValueError, _>("shape overflow"))?;

            let (code, itemsize, raw) = raw_bytes(&t, py).unwrap();
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
                ))
                .into());
            }

            let arr = build_array(code, itemsize, raw, nelems).unwrap();
            Ok(split_rows(arr, rows, cols))
        })
    }
}

fn split_rows(array: ArrayRef, rows: usize, cols: usize) -> Vec<ArrayRef> {
    if cols == 1 {
        vec![array]
    } else {
        (0..rows).map(|i| array.slice(i * cols, cols)).collect()
    }
}

fn fmt_code(fmt: &str) -> Result<char> {
    fmt.bytes()
        .find(|b| !b"<>=@!".contains(b))
        .map(|b| b as char)
        .ok_or_else(|| PyErr::new::<PyTypeError, _>("empty memoryview format").into())
}

fn raw_bytes(t: &Bound<'_, PyAny>, py: Python<'_>) -> Result<(char, usize, Vec<u8>)> {
    let mv0 = t.call_method0("data")?;
    let fmt: String = mv0.getattr("format")?.extract()?;
    let itemsize: usize = mv0.getattr("itemsize")?.extract()?;
    let code = fmt_code(&fmt)?;

    let mvb = mv0.call_method1("cast", ("B",))?;
    let buf = PyBuffer::<u8>::get(&mvb)?;
    if buf.as_slice(py).is_none() {
        return Err(PyErr::new::<PyValueError, _>("non-contiguous buffer").into());
    }

    let pybytes = mvb.call_method0("tobytes")?;
    let pybytes = pybytes.downcast::<PyBytes>().unwrap();
    Ok((code, itemsize, pybytes.as_bytes().to_vec()))
}

fn arrow_build_err(e: impl std::fmt::Display) -> PyErr {
    PyErr::new::<PyValueError, _>(format!("arrow build error: {e}"))
}

fn prim<T>(dt: DataType, raw: Vec<u8>, len: usize) -> Result<ArrayRef>
where
    T: Array + From<ArrayData> + 'static,
{
    let data = ArrayData::builder(dt)
        .len(len)
        .add_buffer(Buffer::from(raw))
        .build()
        .map_err(arrow_build_err)?;
    Ok(Arc::new(T::from(data)) as ArrayRef)
}

fn bools(raw: Vec<u8>, len: usize) -> Result<ArrayRef> {
    let mut bits = vec![0u8; (len + 7) / 8];
    for (i, &b) in raw.iter().enumerate() {
        if b != 0 {
            bit_util::set_bit(&mut bits, i);
        }
    }
    let data = ArrayData::builder(DataType::Boolean)
        .len(len)
        .add_buffer(Buffer::from(bits))
        .build()
        .map_err(arrow_build_err)?;
    Ok(Arc::new(BooleanArray::from(data)) as ArrayRef)
}

fn build_array(code: char, itemsize: usize, raw: Vec<u8>, len: usize) -> Result<ArrayRef> {
    match (code, itemsize) {
        ('f', 4) => prim::<Float32Array>(DataType::Float32, raw, len),
        ('i', 4) => prim::<Int32Array>(DataType::Int32, raw, len),
        ('q', 8) => prim::<Int64Array>(DataType::Int64, raw, len),
        ('B', 1) => Ok(Arc::new(UInt8Array::from(raw)) as ArrayRef),
        ('?', 1) => bools(raw, len),
        _ => Err(PyErr::new::<PyTypeError, _>(format!(
            "unsupported memoryview: format_code={code:?}, itemsize={itemsize}"
        ))
        .into()),
    }
}

fn rows_cols(t: &Bound<'_, PyAny>) -> Result<(usize, usize)> {
    let shape: Vec<usize> = t.getattr("shape")?.extract()?;
    match shape.as_slice() {
        [n] => Ok((*n, 1)),
        [n, m] => Ok((*n, *m)),
        _ => Err(PyErr::new::<PyValueError, _>(format!(
            "expected tensor shape (n,) or (n,m), got {shape:?}"
        ))
        .into()),
    }
}
