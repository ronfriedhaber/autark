use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[pyclass(name = "CsvReader")]
#[derive(Clone)]
pub(crate) struct PyCsvReader {
    pub(crate) uri: String,
}

#[pymethods]
impl PyCsvReader {
    #[new]
    fn new(uri: String) -> PyCsvReader {
        PyCsvReader { uri }
    }
}

#[pyclass(name = "JsonReader")]
#[derive(Clone)]
pub(crate) struct PyJsonReader {
    pub(crate) uri: String,
}

#[pymethods]
impl PyJsonReader {
    #[new]
    fn new(uri: String) -> PyJsonReader {
        PyJsonReader { uri }
    }
}

pub(crate) enum ReaderSpec {
    Csv(String),
    Json(String),
}

pub(crate) fn reader_spec(reader: &Bound<'_, PyAny>) -> PyResult<ReaderSpec> {
    if let Ok(csv) = reader.extract::<PyRef<'_, PyCsvReader>>() {
        return Ok(ReaderSpec::Csv(csv.uri.clone()));
    }
    if let Ok(json) = reader.extract::<PyRef<'_, PyJsonReader>>() {
        return Ok(ReaderSpec::Json(json.uri.clone()));
    }
    Err(PyValueError::new_err("expected CsvReader or JsonReader"))
}
