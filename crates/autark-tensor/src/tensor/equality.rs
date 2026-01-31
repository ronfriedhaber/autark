use crate::tensor::PyTensor;
use crate::with_tinygrad::with_tinygrad;
use std::sync::Arc;

use pyo3::prelude::*;

macro_rules! impl_pytensor_binaryop_comp {
    ($self: ident, $fn_name:ident, $py_fn_name:literal) => {
        impl PyTensor {
            pub fn $fn_name(&$self, rhs: &PyTensor) -> PyTensor {
                let obj = with_tinygrad(|py| {
                        let obj = $self
                            .inner
                            .bind(py)
                            .getattr($py_fn_name)
                            .unwrap()
                            .call1((&*rhs.inner,))
                            .unwrap();
                        Ok(obj.unbind())
                    })
                    .unwrap();

                PyTensor { inner: Arc::new(obj) }
            }
        }
    }
}

impl_pytensor_binaryop_comp!(self, lt, "__lt__");
impl_pytensor_binaryop_comp!(self, gt, "__gt__");
