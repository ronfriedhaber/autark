use pyo3::prelude::*;
use pyo3::types::PyList;
use std::env;

use autark_error::Error;

const DEFAULT_TINYGRAD_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../tinygrad");

pub fn with_tinygrad<F, R>(f: F) -> Result<R, Error>
where
    F: FnOnce(Python<'_>) -> Result<R, Error>,
{
    Python::attach(|py| {
        ensure_tinygrad_on_path(py)?;
        f(py)
    })
    .map_err(Into::into)
}

fn ensure_tinygrad_on_path(py: Python<'_>) -> PyResult<()> {
    let sys = py.import("sys")?;
    let path_binding = sys.getattr("path")?;
    let path = path_binding.downcast::<PyList>()?;
    let tinygrad_path = tinygrad_path();
    let already_present = path
        .iter()
        .filter_map(|entry| entry.extract::<String>().ok())
        .any(|entry| entry == tinygrad_path);

    if !already_present {
        path.append(tinygrad_path)?;
    }

    Ok(())
}

fn tinygrad_path() -> String {
    env::var("TINYGRAD_PATH").unwrap_or_else(|_| DEFAULT_TINYGRAD_PATH.to_string())
}
