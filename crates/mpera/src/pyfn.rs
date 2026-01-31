use std::ffi::CString;
use std::str::FromStr;

use pyo3::types::{PyModule, PyTuple};
use pyo3::{BoundObject, prelude::*};

#[derive(Debug)]
pub struct PyFn {
    run: Py<PyAny>,
}

impl PyFn {
    pub fn new(py: Python<'_>, code: &str) -> PyResult<Self> {
        let module = PyModule::from_code(
            py,
            CString::from_str(code).unwrap().as_c_str(),
            CString::from_str("transformation.py").unwrap().as_c_str(),
            CString::from_str("transformation").unwrap().as_c_str(),
        )?;

        let run = module.getattr("transform")?;
        if !run.is_callable() {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "`run` exists but is not callable",
            ));
        }

        Ok(Self { run: run.unbind() })
    }

    #[inline]
    pub fn call1<'py, A>(&self, py: Python<'py>, args: A) -> PyResult<Bound<'py, PyAny>>
    where
        A: IntoPyObject<'py, Target = PyTuple> + pyo3::call::PyCallArgs<'py>, // accepts tuples; see below for ergonomic usage
    {
        let run = self.run.bind(py);
        run.call1(args)
    }

    #[inline]
    pub fn call<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, pyo3::types::PyTuple>,
        kwargs: Option<&Bound<'py, pyo3::types::PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.run.bind(py).call(args, kwargs)
    }
}
