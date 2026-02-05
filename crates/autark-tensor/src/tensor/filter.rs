use pyo3::types::PyAnyMethods;

use crate::{Result, tensor::Tensor};
use with_tinygrad::with_tinygrad;

impl Tensor {
    pub fn filter(&self, mask: &Tensor) -> Result<Tensor> {
        let new = with_tinygrad(|py| {
            let idx = mask
                .inner
                .bind(py)
                .getattr("nonzero")
                .unwrap()
                .call0()
                .unwrap()
                .getattr("reshape")
                .unwrap()
                .call1((-1,))
                .unwrap();

            let new = self
                .inner
                .bind(py)
                .getattr("gather")
                .unwrap()
                .call1((0, idx))
                .unwrap()
                .unbind();

            Ok(new)
        })?;

        Ok(Tensor::new(new))
    }
}
