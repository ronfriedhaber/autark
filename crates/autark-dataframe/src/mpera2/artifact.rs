use pyo3::{prelude::*, with_embedded_python_interpreter};

use crate::{op::OpPool, pyfn::PyFn, with_tinygrad::with_tinygrad};

#[derive(Debug)]
pub struct Artifact {
    // opool: OpPool,
    pub(crate) source: String,
    pub(crate) object: PyFn,
}

impl Artifact {
    pub fn new(source: &str) -> Artifact {
        Artifact {
            source: source.to_string(),
            object: with_tinygrad(|py| PyFn::new(py, source)).unwrap(),
        }
    }
}
