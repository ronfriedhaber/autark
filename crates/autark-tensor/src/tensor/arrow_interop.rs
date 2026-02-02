use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, *};
use arrow::datatypes::Field;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::Result;
use crate::with_tinygrad::with_tinygrad;

use arrow::array::{
    ArrayData, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, LargeStringArray,
    StringArray, UInt8Array,
};
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;
use arrow::util::bit_util;

use pyo3::types::PyBytes;

impl super::Tensor {
    pub fn try_from_arrow_1d(arr: &ArrayRef, _name: &str, data_aux: &mut Vec<u8>) -> Result<Self> {
        use pyo3::exceptions::PyValueError;

        with_tinygrad(|py| {
            let tensor_cls = py.import("tinygrad")?.getattr("Tensor")?;

            if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
                let mut end: usize = data_aux.len();
                let mut offsets: Vec<i32> = Vec::with_capacity(a.len());
                for v in a.iter() {
                    if let Some(s) = v {
                        data_aux.extend_from_slice(s.as_bytes());
                        end = end.checked_add(s.len()).ok_or_else(|| {
                            PyErr::new::<PyValueError, _>("string buffer overflow")
                        })?;
                    }
                    if end > i32::MAX as usize {
                        return Err(PyErr::new::<PyValueError, _>(
                            "string buffer exceeds i32 offsets",
                        )
                        .into());
                    }
                    offsets.push(end as i32);
                }
                let dtypes = py.import("tinygrad")?.getattr("dtypes")?;
                let kwargs = PyDict::new(py);
                kwargs.set_item("dtype", dtypes.getattr("int32")?)?;
                return Ok(Self {
                    inner: Arc::new(tensor_cls.call((offsets,), Some(&kwargs))?.into()),
                });
            }

            if let Some(a) = arr.as_any().downcast_ref::<LargeStringArray>() {
                let mut end: usize = data_aux.len();
                let mut offsets: Vec<i64> = Vec::with_capacity(a.len());
                for v in a.iter() {
                    if let Some(s) = v {
                        data_aux.extend_from_slice(s.as_bytes());
                        end = end.checked_add(s.len()).ok_or_else(|| {
                            PyErr::new::<PyValueError, _>("string buffer overflow")
                        })?;
                    }
                    offsets.push(end as i64);
                }
                let dtypes = py.import("tinygrad")?.getattr("dtypes")?;
                let kwargs = PyDict::new(py);
                kwargs.set_item("dtype", dtypes.getattr("int64")?)?;
                return Ok(Self {
                    inner: Arc::new(tensor_cls.call((offsets,), Some(&kwargs))?.into()),
                });
            }

            macro_rules! try_cast {
                ($ty:ty, $default:expr) => {
                    arr.as_any()
                        .downcast_ref::<$ty>()
                        .map(|a| PyList::new(py, a.iter().map(|v| v.unwrap_or($default))))
                };
            }

            let list = try_cast!(Int8Array, 0)
                .or_else(|| try_cast!(Int16Array, 0))
                .or_else(|| try_cast!(Int32Array, 0))
                .or_else(|| try_cast!(Int64Array, 0))
                .or_else(|| try_cast!(Float32Array, 0.0))
                .or_else(|| try_cast!(Float64Array, 0.0))
                .or_else(|| try_cast!(BooleanArray, false))
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

    pub fn try_into_arrow_1d_or_2d_with_aux(
        &self,
        data_aux: &super::Tensor,
        string_cols: &[usize],
    ) -> Result<Vec<ArrayRef>> {
        if string_cols.is_empty() {
            return self.try_into_arrow_1d_or_2d_2();
        }

        with_tinygrad(|py| {
            let t = self.inner.bind(py);
            let shape: Vec<usize> = t.getattr("shape")?.extract()?;
            let (rows, cols) = match shape.as_slice() {
                [n] => (*n, 1),
                [n, m] => (*n, *m),
                _ => {
                    return Err(PyValueError::new_err(format!(
                        "expected tensor shape (n,) or (n,m), got {shape:?}"
                    ))
                    .into());
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
                .downcast::<PyBytes>()
                .unwrap()
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
                ))
                .into());
            }

            let aux_mv = data_aux.inner.bind(py).call_method0("data")?;
            let aux_raw: Vec<u8> = aux_mv
                .call_method1("cast", ("B",))?
                .call_method0("tobytes")?
                .downcast::<PyBytes>()
                .unwrap()
                .as_bytes()
                .to_vec();

            let mut is_string = vec![false; rows];
            for &ix in string_cols {
                if ix >= rows {
                    return Err(PyValueError::new_err(format!(
                        "string column index out of range: {ix} (rows={rows})"
                    ))
                    .into());
                }
                is_string[ix] = true;
            }

            fn parse<const N: usize, T>(raw: &[u8], f: impl Fn([u8; N]) -> T) -> Vec<T> {
                raw.chunks_exact(N)
                    .map(|c| f(c.try_into().unwrap()))
                    .collect()
            }

            let (vals_i64, kind): (Vec<i64>, u8) = match (code, itemsize) {
                ('i', 4) => (
                    parse::<4, _>(&raw, i32::from_le_bytes)
                        .into_iter()
                        .map(|v| v as i64)
                        .collect(),
                    0,
                ),
                ('q', 8) => (parse::<8, _>(&raw, i64::from_le_bytes), 1),
                ('f', 4) => (
                    parse::<4, _>(&raw, f32::from_le_bytes)
                        .into_iter()
                        .map(|v| v as i64)
                        .collect(),
                    2,
                ),
                ('d', 8) => (
                    parse::<8, _>(&raw, f64::from_le_bytes)
                        .into_iter()
                        .map(|v| v as i64)
                        .collect(),
                    3,
                ),
                _ => {
                    return Err(PyTypeError::new_err(format!(
                        "unsupported memoryview for string offsets: format={fmt:?}, itemsize={itemsize}"
                    ))
                    .into());
                }
            };

            let mut out: Vec<ArrayRef> = Vec::with_capacity(rows);
            let mut prev_end: i64 = 0;

            let mut take_string = |slice: &[i64], large: bool| -> Result<ArrayRef> {
                let mut last_end = prev_end;
                if large {
                    let mut offsets: Vec<i64> = Vec::with_capacity(cols + 1);
                    offsets.push(0);
                    for &end_abs in slice {
                        if end_abs < last_end {
                            return Err(PyValueError::new_err("string offsets not monotonic").into());
                        }
                        last_end = end_abs;
                        offsets.push(end_abs - prev_end);
                    }
                    let base = prev_end as usize;
                    let last = last_end as usize;
                    if last > aux_raw.len() {
                        return Err(PyValueError::new_err("string buffer out of bounds").into());
                    }
                    let values = aux_raw[base..last].to_vec();
                    let data = ArrayData::builder(DataType::LargeUtf8)
                        .len(cols)
                        .add_buffer(Buffer::from(offsets))
                        .add_buffer(Buffer::from(values))
                        .build()
                        .map_err(|e| PyValueError::new_err(format!("arrow build error: {e}")))?;
                    prev_end = last_end;
                    Ok(Arc::new(LargeStringArray::from(data)) as ArrayRef)
                } else {
                    let mut offsets: Vec<i32> = Vec::with_capacity(cols + 1);
                    offsets.push(0);
                    for &end_abs in slice {
                        if end_abs < last_end {
                            return Err(PyValueError::new_err("string offsets not monotonic").into());
                        }
                        last_end = end_abs;
                        let rel = end_abs - prev_end;
                        if rel > i32::MAX as i64 {
                            return Err(
                                PyValueError::new_err("string buffer exceeds i32 offsets").into()
                            );
                        }
                        offsets.push(rel as i32);
                    }
                    let base = prev_end as usize;
                    let last = last_end as usize;
                    if last > aux_raw.len() {
                        return Err(PyValueError::new_err("string buffer out of bounds").into());
                    }
                    let values = aux_raw[base..last].to_vec();
                    let data = ArrayData::builder(DataType::Utf8)
                        .len(cols)
                        .add_buffer(Buffer::from(offsets))
                        .add_buffer(Buffer::from(values))
                        .build()
                        .map_err(|e| PyValueError::new_err(format!("arrow build error: {e}")))?;
                    prev_end = last_end;
                    Ok(Arc::new(StringArray::from(data)) as ArrayRef)
                }
            };

            for col in 0..rows {
                let start = col * cols;
                let end = start + cols;
                let slice_i64 = &vals_i64[start..end];
                if is_string[col] {
                    out.push(take_string(slice_i64, matches!(kind, 1 | 3))?);
                } else {
                    let arr: ArrayRef = match kind {
                        0 => Arc::new(Int32Array::from(
                            slice_i64.iter().map(|&v| v as i32).collect::<Vec<_>>(),
                        )),
                        1 => Arc::new(Int64Array::from(slice_i64.to_vec())),
                        2 => Arc::new(Float32Array::from(
                            slice_i64.iter().map(|&v| v as f32).collect::<Vec<_>>(),
                        )),
                        _ => Arc::new(Float64Array::from(
                            slice_i64.iter().map(|&v| v as f64).collect::<Vec<_>>(),
                        )),
                    };
                    out.push(arr);
                }
            }

            Ok(out)
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
                    ))
                    .into());
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
                .downcast::<PyBytes>()
                .unwrap()
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
                ))
                .into());
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
                ))
                .into()),
            }
        })
    }
}
