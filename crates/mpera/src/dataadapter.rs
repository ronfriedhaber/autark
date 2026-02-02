use autark_tensor::Tensor;
use std::collections::HashMap;

pub struct DataFramePayload {
    pub(crate) data: Tensor,     // shape = 2D (k,n)
    pub(crate) data_aux: Tensor, // shape = 1D (j,)

    pub(crate) name2index: HashMap<String, usize>,
    // pub(crate) variant_index2repr: HashMap<String, usize>,
}

impl DataFramePayload {
    pub fn new(
        data: Tensor,
        data_aux: Tensor,
        name2index: HashMap<String, usize>,
    ) -> DataFramePayload {
        DataFramePayload {
            data,
            data_aux,
            name2index,
        }
    }
}

// pub trait DataAdapter {
//     type Error: Error; // TODO: Erro requires trait

//     fn valorize(&self) -> Result<DataFramePayload, Self::Error>;
// }
