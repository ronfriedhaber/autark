use std::sync::Arc;

use pyo3::prelude::*;

use crate::tensor::Tensor;
use with_tinygrad::with_tinygrad;

macro_rules! impl_pytensor_binaryop_tensor {
    ($trait_name:ident, $fn_name:ident, $py_fn_name:literal, $rhs:ty) => {
        impl std::ops::$trait_name<$rhs> for &Tensor {
            type Output = Tensor;

            fn $fn_name(self, rhs: $rhs) -> Tensor {
                let inner = with_tinygrad(|py| {
                    let inner = self
                        .inner
                        .bind(py)
                        .getattr($py_fn_name)
                        .unwrap()
                        .call1((&*rhs.inner,))
                        .unwrap();
                    Ok(inner.unbind())
                })
                .unwrap();

                Tensor {
                    inner: Arc::new(inner),
                }
            }
        }
    };
}

macro_rules! impl_pytensor_binaryop_scalar {
    ($trait_name:ident, $fn_name:ident, $py_fn_name:literal, $rhs:ty) => {
        impl std::ops::$trait_name<$rhs> for &Tensor {
            type Output = Tensor;

            fn $fn_name(self, rhs: $rhs) -> Tensor {
                let inner = with_tinygrad(|py| {
                    let inner = self
                        .inner
                        .bind(py)
                        .getattr($py_fn_name)
                        .unwrap()
                        .call1((rhs,))
                        .unwrap();
                    Ok(inner.unbind())
                })
                .unwrap();

                Tensor {
                    inner: Arc::new(inner),
                }
            }
        }
    };
}

impl_pytensor_binaryop_tensor!(Add, add, "__add__", &Tensor);
impl_pytensor_binaryop_scalar!(Add, add, "__add__", f64);
