use autark::Tensor;
use pyo3::{exceptions::PyTypeError, prelude::*};

#[pyclass(name = "Tensor")]
#[derive(Clone, Debug)]
pub struct PyTensor(pub(crate) Tensor);

macro_rules! impl_pytensor_op_overload {
    ($fn_name:ident, $op:tt) => {
        fn $fn_name(&self, rhs: &Bound<'_, PyAny>) -> PyResult<Self> {
            if let Ok(x) = rhs.extract::<PyTensor>() {
                return Ok(PyTensor(&self.0 $op &x.0));
            }

            if let Ok(x) = rhs.extract::<f64>() {
                return Ok(PyTensor(&self.0 $op x));
            }

            return Err(PyTypeError::new_err(("",)));
        }
    };
}

macro_rules! impl_pytensor_op_overload_2 {
    ($fn_name:ident, $inner_fn_name:ident) => {
        fn $fn_name(&self, rhs: &Bound<'_, PyAny>) -> PyResult<Self> {
            if let Ok(x) = rhs.extract::<PyTensor>() {
                return Ok(PyTensor(self.0.$inner_fn_name(&x.0)));
            }

            if let Ok(x) = rhs.extract::<f64>() {
                return Ok(PyTensor(
                    self.0.$inner_fn_name(&Tensor::from_slice(&[x]).unwrap()),
                ));
            }

            if let Ok(x) = rhs.extract::<i64>() {
                return Ok(PyTensor(
                    self.0.$inner_fn_name(&Tensor::from_slice(&[x]).unwrap()),
                ));
            }

            return Err(PyTypeError::new_err(("",)));
        }
    };
}

impl PyTensor {
    impl_pytensor_op_overload!(py_add, +);
    impl_pytensor_op_overload!(py_sub, -);
    impl_pytensor_op_overload!(py_mul, *);
    impl_pytensor_op_overload!(py_div, /);

    impl_pytensor_op_overload_2!(py_lt, lt);
    impl_pytensor_op_overload_2!(py_gt, gt);
    // impl_pytensor_op_overload_2!(py_le, le);
    // impl_pytensor_op_overload_2!(py_ge, ge);
}

// TODO: macro too
#[pymethods]
impl PyTensor {
    fn __add__(&self, rhs: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.py_add(rhs)
    }

    fn __sub__(&self, rhs: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.py_sub(rhs)
    }

    fn __mul__(&self, rhs: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.py_mul(rhs)
    }

    fn __div__(&self, rhs: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.py_div(rhs)
    }

    fn __lt__(&self, rhs: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.py_lt(rhs)
    }

    fn __gt__(&self, rhs: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.py_gt(rhs)
    }
}
