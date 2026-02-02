use pyo3::{
    prelude::*,
    types::{PyDict, PyTuple},
};
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float32Array, Int32Array, Int64Array, UInt8Array};
use pyo3::{buffer::PyBuffer, prelude::*};

use crate::{Result, tensor::Tensor, with_tinygrad::with_tinygrad};

impl Tensor {
    pub fn new(obj: Py<PyAny>) -> Tensor {
        Tensor {
            inner: Arc::new(obj),
        }
    }

    pub fn inner_cloned(&self) -> Arc<Py<PyAny>> {
        self.inner.clone()
    }

    pub fn from_slice<T: Clone + for<'a> IntoPyObject<'a>>(x: &[T]) -> Result<Self> {
        let obj = with_tinygrad(|py| {
            let tg = py.import("tinygrad").unwrap();
            let mut kwargs = PyDict::new(py);
            // kwargs.

            let dtypes = py.import("tinygrad")?.getattr("dtypes")?;
            kwargs.set_item("dtype", dtypes.getattr("uint8")?)?;
            let obj = tg
                .getattr("Tensor")
                .unwrap()
                .call((x.to_vec(),), Some(&kwargs))
                .unwrap();
            Ok(obj.unbind())
        })
        .unwrap();

        Ok(Self {
            inner: Arc::new(obj),
        })
    }

    pub fn from_slice_of_string(x: &[Option<&str>]) -> Result<Self> {
        let max_length: usize = x.iter().fold(0, |v, x| {
            if x.unwrap_or("").len() > v {
                x.unwrap_or("").len()
            } else {
                v
            }
        });

        let content_bytes: Vec<u8> = x
            .iter()
            .filter(|x| x.is_some())
            .flat_map(|x| {
                let mut xs = x.as_ref().unwrap().as_bytes().to_vec();
                if xs.len() < max_length {
                    let delta = max_length - xs.len();
                    xs.extend_from_slice((0..delta).map(|_| 0).collect::<Vec<u8>>().as_slice());
                }
                xs
            })
            .collect();

        let obj = with_tinygrad(|py| {
            let tg = py.import("tinygrad").unwrap();
            let obj = tg
                .getattr("Tensor")
                .unwrap()
                .call1((content_bytes,))
                .unwrap();
            Ok(obj.unbind())
        })
        .unwrap();

        Ok(Self {
            inner: Arc::new(obj),
        })
    }

    pub fn stack(tensors: &[Tensor]) -> Result<Self> {
        let inner = with_tinygrad(|py| {
            let tg = py.import("tinygrad").unwrap();
            let obj = tg
                .getattr("Tensor")
                .unwrap()
                .getattr("stack")
                .unwrap()
                .call1(PyTuple::new(py, tensors.iter().map(|x| &*x.inner)).unwrap())
                .unwrap();

            Ok(obj.unbind())
        })
        .unwrap();

        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    pub fn index(&self, xs: &[Option<isize>]) -> Result<Self> {
        let inner = with_tinygrad(|py| {
            let obj = self
                .inner
                .bind(py)
                .getattr("__getitem__")
                .unwrap()
                .call1(PyTuple::new(py, xs.iter().map(|x| x)).unwrap())
                .unwrap();

            Ok(obj.unbind())
        })
        .unwrap();

        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    // use std::sync::Arc;

    // use arrow::array::{ArrayRef, BooleanArray, Float32Array, Int32Array, Int64Array, UInt8Array};
    // use pyo3::{buffer::PyBuffer, prelude::*};

    pub fn to_arrow_array(&self) -> ArrayRef {
        with_tinygrad(|py| {
            let mv = self.inner.bind(py).call_method0("data")?;

            let fmt: String = mv.getattr("format")?.extract()?;
            let item: usize = mv.getattr("itemsize")?.extract()?;

            let mvb = mv.call_method1("cast", ("B",))?;
            let buf = PyBuffer::<u8>::get(&mvb)?;

            // let buf = PyBuffer::<u8>::get(&mv)?;
            let raw = buf
                .as_slice(py)
                .unwrap()
                .iter()
                .map(|c| c.get())
                .collect::<Vec<u8>>();

            let code = fmt.bytes().find(|b| !b"<>=@!".contains(b)).unwrap() as char;

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
                _ => {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                        "unsupported memoryview: format={fmt:?}, itemsize={item}"
                    ))
                    .into());
                }
            })
        })
        .unwrap()
    }

    pub fn to_u8_vec(&self) -> Result<Vec<u8>> {
        with_tinygrad(|py| {
            let mv = self.inner.bind(py).call_method0("data")?;
            let raw: Vec<u8> = mv
                .call_method1("cast", ("B",))?
                .call_method0("tobytes")?
                .downcast::<pyo3::types::PyBytes>()
                .unwrap()
                .as_bytes()
                .to_vec();
            Ok(raw)
        })
    }

    pub fn tolist_string(&self) -> String {
        let obj = with_tinygrad(|py| {
            let obj = self
                .inner
                .bind(py)
                .getattr("tolist")
                .unwrap()
                .call0()
                .unwrap();

            let obj = obj.to_string();
            Ok(obj)
        })
        .unwrap();

        obj
    }

    fn py_to_json_vec(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Vec<serde_json::Value>> {
        let json = py.import("json")?;
        let s: String = json.call_method1("dumps", (obj,))?.extract()?;
        let v: Vec<serde_json::Value> = serde_json::from_str(&s)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(v)
    }

    pub fn tolist_serialized_json(&self) -> Vec<serde_json::Value> {
        let obj = with_tinygrad(|py| {
            let obj = self
                .inner
                .bind(py)
                .getattr("tolist")
                .unwrap()
                .call0()
                .unwrap();

            Ok(Self::py_to_json_vec(py, &obj)?)
        })
        .unwrap();

        obj
    }

    pub fn shape(&self) -> Vec<usize> {
        let obj = with_tinygrad(|py| {
            let obj = self.inner.bind(py).getattr("shape").unwrap();
            Ok(obj.extract::<Vec<usize>>().unwrap())
        })
        .unwrap();

        obj
    }
}
