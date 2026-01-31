use autark::Value;
use pyo3::PyResult;
use pyo3::prelude::*;

#[pyclass(name = "Value")]
pub struct PyValue {
    pub(crate) inner: Option<Value>,
}

impl PyValue {
    pub(crate) fn from_inner(inner: Value) -> PyValue {
        PyValue { inner: Some(inner) }
    }
}

// impl<'py> FromPyObject<'py> for PyValue {
//     fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
//         if let Ok(x) = ob.extract::<f64>() {
//             return Ok(PyValue::from_inner(Value::Constant(x)));
//         }

//         // if let Ok(i) = ob.extract::<i64>() {
//         //     return Ok(Value(i as f64));

//         //     return Ok(PyValue::from_inner(Value::Constant(x)));
//         // }

//         Err(pyo3::exceptions::PyTypeError::new_err(
//             "expected a number for Value",
//         ))
//     }
// }

#[pymethods]
impl PyValue {
    pub fn __add__(&self, rhs: &PyValue) -> PyResult<PyValue> {
        Ok(PyValue::from_inner(
            self.inner.as_ref().unwrap() + rhs.inner.as_ref().unwrap(),
        ))
    }

    pub fn rolling_mean(&self, window: usize) -> PyResult<PyValue> {
        Ok(PyValue::from_inner(
            self.inner
                .as_ref()
                .unwrap()
                .rolling_mean(&Value::Constant(window as f64)),
        ))
    }
}
