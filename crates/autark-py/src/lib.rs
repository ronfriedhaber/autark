use pyo3::prelude::*;

use crate::{pylazyframe::PyLazyFrame, pylazyreader::PyLazyReader, pyvalue::PyValue};

pub mod pylazyframe;
pub mod pylazyreader;
pub mod pyvalue;

#[pyfunction(name = "col")]
pub fn pycol(name: &str) -> PyResult<PyValue> {
    Ok(PyValue::from_inner(autark::col(name)))
}

#[pymodule(name = "autarkpy")]
fn autarkpy(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyLazyFrame>()?;
    m.add_class::<PyLazyReader>()?;
    m.add_function(wrap_pyfunction!(pycol, m)?)?;

    Ok(())
}
