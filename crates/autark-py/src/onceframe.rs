use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

use autark::onceframe::OnceFrame;
use autark::prelude::readers::csv::CsvReader;
use autark::prelude::readers::json::JsonReader;
use autark::prelude::sink::void::SinkVoid;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::error::py_err;
use crate::program::PyProgram;
use crate::reader::{reader_spec, ReaderSpec};

static NEXT_FRAME_ID: AtomicU64 = AtomicU64::new(1);

#[pyclass(name = "OnceFrame", unsendable)]
pub(crate) struct PyOnceFrame {
    id: u64,
    frame: Option<OnceFrame<SinkVoid>>,
}

impl PyOnceFrame {
    fn frame_ref(&self) -> PyResult<&OnceFrame<SinkVoid>> {
        self.frame
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("OnceFrame already realized"))
    }
}

#[pymethods]
impl PyOnceFrame {
    #[new]
    fn new(reader: &Bound<'_, PyAny>) -> PyResult<PyOnceFrame> {
        let frame = match reader_spec(reader)? {
            ReaderSpec::Csv(uri) => {
                OnceFrame::new(CsvReader::new(&uri).map_err(py_err)?, SinkVoid {})
            }
            ReaderSpec::Json(uri) => {
                OnceFrame::new(JsonReader::new(&uri).map_err(py_err)?, SinkVoid {})
            }
        };
        Ok(PyOnceFrame {
            id: NEXT_FRAME_ID.fetch_add(1, Ordering::Relaxed),
            frame: Some(frame),
        })
    }

    #[pyo3(signature = (index = None))]
    fn dataframe(&self, index: Option<usize>) -> PyResult<PyProgram> {
        Ok(PyProgram::new(
            self.id,
            self.frame_ref()?.p.dataframe(index).map_err(py_err)?,
        ))
    }

    fn lit(&self, value: f64) -> PyResult<PyProgram> {
        Ok(PyProgram::new(
            self.id,
            self.frame_ref()?.p.const_f64(value).map_err(py_err)?,
        ))
    }

    #[pyo3(signature = (index = None))]
    fn schema(&self, index: Option<usize>) -> PyResult<Vec<String>> {
        let schema = self.frame_ref()?.schema(index).map_err(py_err)?;
        Ok(schema
            .fields()
            .iter()
            .map(|x| x.name().to_string())
            .collect())
    }

    fn realize(&mut self) -> PyResult<u64> {
        let frame = self
            .frame
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("OnceFrame already realized"))?;
        let realized = frame.realize().map_err(py_err)?;
        let mut hasher = DefaultHasher::new();
        realized.hash(&mut hasher);
        Ok(hasher.finish())
    }
}
