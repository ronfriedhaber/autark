// pub(crate) mod arithmetic;
// pub(crate) mod equality;
// pub(crate) mod filter;
pub(crate) mod arrow_interop;
pub(crate) mod arrow_to;
pub(crate) mod general;

use pyo3::prelude::*;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Tensor {
    pub(super) inner: Arc<Py<PyAny>>,
}
