use crate::tensor::Tensor;
use crate::with_tinygrad::with_tinygrad;
use std::sync::Arc;

use pyo3::prelude::*;

macro_rules! impl_pytensor_binaryop_arithmetic {
    ($self: ident,  $trait_name:ident, $fn_name:ident, $py_fn_name:literal) => {
        impl std::ops::$trait_name<&Tensor> for &Tensor {
            type Output = Tensor;

            fn $fn_name($self, rhs: &Tensor) -> Tensor {
                 let inner = with_tinygrad(|py| {
                        let inner = $self
                            .inner
                            .bind(py)
                            .getattr($py_fn_name)
                            .unwrap()
                            .call1((&*rhs.inner,))
                            .unwrap();

                        Ok(inner.unbind())
                    })
                    .unwrap();

                Tensor{ inner: Arc::new(inner) }
            }
        }


        impl std::ops::$trait_name<f64> for &Tensor {
            type Output = Tensor;

            fn $fn_name($self, rhs: f64) -> Tensor {
                 let inner = with_tinygrad(|py| {
                        let inner = $self
                            .inner
                            .bind(py)
                            .getattr($py_fn_name)
                            .unwrap()
                            .call1((rhs,))
                            .unwrap();
                        Ok(inner.unbind())
                    })
                    .unwrap();

                Tensor { inner: Arc::new(inner) }
            }
        }
    };
}

impl_pytensor_binaryop_arithmetic!(self, Add, add, "__add__");
impl_pytensor_binaryop_arithmetic!(self, Sub, sub, "__sub__");
impl_pytensor_binaryop_arithmetic!(self, Mul, mul, "__mul__");
impl_pytensor_binaryop_arithmetic!(self, Div, div, "__div__");
