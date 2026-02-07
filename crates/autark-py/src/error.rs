use pyo3::exceptions::PyRuntimeError;
use pyo3::PyErr;

pub(crate) fn py_err<E: std::error::Error>(err: E) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}
