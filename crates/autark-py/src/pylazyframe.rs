use autark::{LazyFrame, Value, lazyreader::LazyReader};

use pyo3::prelude::*;

use crate::{pylazyreader::PyLazyReader, pyvalue::PyValue};
#[pyclass(name = "DataFrame")]
pub struct PyLazyFrame {
    inner: Option<LazyFrame<PyLazyReader>>,
}

#[pymethods]
impl PyLazyFrame {
    #[new]
    pub fn new(datareader: &PyLazyReader) -> PyResult<PyLazyFrame> {
        Ok(PyLazyFrame {
            inner: Some(LazyFrame::new(datareader.clone())),
        })
    }

    pub fn realize(&mut self) -> PyResult<()> {
        self.inner.take().unwrap().realize().unwrap();
        Ok(())
    }

    pub fn with_column(&mut self, name: &str, value: &mut PyValue) -> PyResult<PyLazyFrame> {
        Ok(PyLazyFrame {
            inner: Some(
                self.inner
                    .take()
                    .unwrap()
                    .with_column(name, value.inner.take().unwrap())
                    .unwrap(),
            ),
        })
    }
}
