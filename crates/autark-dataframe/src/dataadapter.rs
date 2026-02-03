use autark_tensor::Tensor;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct DataFramePayload {
    pub data: Tensor,
    pub data_aux: Tensor,

    pub name2index: HashMap<String, usize>,
    pub variant_map: Vec<Vec<String>>,
}

impl DataFramePayload {
    pub fn new(
        data: Tensor,
        data_aux: Tensor,
        name2index: HashMap<String, usize>,
        variant_map: Vec<Vec<String>>,
    ) -> DataFramePayload {
        DataFramePayload {
            data,
            data_aux,
            name2index,
            variant_map,
        }
    }
}
