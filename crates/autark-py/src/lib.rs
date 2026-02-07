use pyo3::prelude::*;

mod error;
mod kinds;
mod onceframe;
mod program;
mod reader;

use onceframe::PyOnceFrame;
use program::PyProgram;
use reader::{PyCsvReader, PyJsonReader};

#[pymodule(name = "autarkpy")]
fn autarkpy(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyCsvReader>()?;
    m.add_class::<PyJsonReader>()?;
    m.add_class::<PyProgram>()?;
    m.add_class::<PyOnceFrame>()?;
    Ok(())
}
