use std::{path::PathBuf, str::FromStr};

use pyo3::{exceptions::PyFileNotFoundError, prelude::*};

use autark::lazyreader::{Csv, LazyReader};

#[pyclass(name = "DataReader")]
#[derive(Clone)]
pub enum PyLazyReader {
    Csv(String),
}

impl LazyReader for PyLazyReader {
    type Error = pyo3::PyErr;

    fn realize(self) -> Result<autark::DataFrame, Self::Error> {
        let lazyreader = match self {
            Self::Csv(path) => Csv(PathBuf::from_str(&path).unwrap()),
        };
        match lazyreader.realize() {
            Ok(x) => Ok(x),
            Err(_) => Err(PyFileNotFoundError::new_err(())),
        }
    }
}
