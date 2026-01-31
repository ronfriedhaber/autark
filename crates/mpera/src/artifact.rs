use std::collections::HashMap;

use crate::{pyfn::PyFn, with_tinygrad::with_tinygrad};

#[derive(Debug)]
pub struct Artifact {
    // opool: OpPool,
    pub(crate) source: String,
    pub(crate) object: PyFn,
    // pub(crate) schema_map: HashMap<String, Option<usize>>,
}

impl Artifact {
    pub fn new(source: &str) -> Artifact {
        Artifact {
            source: source.to_string(),
            object: with_tinygrad(|py| PyFn::new(py, source)).unwrap(),
        }
    }
}
