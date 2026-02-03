use autark_tensor::Tensor;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct DataFramePayload {
    pub(crate) data: Tensor,     // shape = 2D (k,n)
    pub(crate) data_aux: Tensor, // shape = 1D (j,)

    pub(crate) name2index: HashMap<String, usize>,
    pub(crate) variant_map: Vec<Vec<String>>,
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

// pub trait DataAdapter {
//     type Error: Error; // TODO: Erro requires trait

//     fn valorize(&self) -> Result<DataFramePayload, Self::Error>;
// }
