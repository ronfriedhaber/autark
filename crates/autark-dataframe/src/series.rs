use std::collections::HashMap;

use autark_tensor::Tensor;

use crate::Result;

pub struct Series {
    variant_map: Option<Vec<String>>,
    tensor: Tensor,
}

impl Series {
    pub fn new(tensor: Tensor, variant_map: Option<Vec<String>>) -> Series {
        Series {
            variant_map,
            tensor,
        }
    }

    pub fn from_arrow(array: &arrow::array::Array) -> Result<Series> {
        let tensor = Tensor::try_from_arrow_1d(array, "");
        use arrow::datatypes::DataType::*;

        let variant_map = match array.data_type() {
            Utf8 => Some({ vec![] }),
            _ => None,
        };

        Series::new(tensor, variant_map)
    }
}
