use pyo3::prelude::*;
use pyo3::types::PyList;
use std::env;

use crate::error::Error;

const DEFAULT_TINYGRAD_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../tinygrad");

pub(crate) fn with_tinygrad<F, R>(f: F) -> Result<R, Error>
where
    F: FnOnce(Python<'_>) -> Result<R, Error>,
{
    Ok(Python::attach(|py| {
        ensure_tinygrad_on_path(py)?;
        f(py)
    })
    .unwrap())
}

fn ensure_tinygrad_on_path(py: Python<'_>) -> Result<(), Error> {
    let sys = py.import("sys")?;
    let path_binding = sys.getattr("path")?;
    let path = path_binding.downcast::<PyList>().unwrap(); // TODO
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
