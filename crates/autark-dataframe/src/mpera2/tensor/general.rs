use pyo3::{prelude::*, types::PyTuple};
use std::sync::Arc;

use crate::{Result, tensor::Tensor, with_tinygrad::with_tinygrad};

impl Tensor {
    pub(super) fn new(obj: Py<PyAny>) -> Tensor {
        Tensor {
            inner: Arc::new(obj),
        }
    }

    pub fn from_slice<T: Clone + for<'a> IntoPyObject<'a>>(x: &[T]) -> Result<Self> {
        let obj = with_tinygrad(|py| {
            let tg = py.import("tinygrad").unwrap();
            let obj = tg.getattr("Tensor").unwrap().call1((x.to_vec(),)).unwrap();
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
