use autark_tensor::Tensor;
use std::collections::HashMap;

pub struct DataFramePayload {
    pub(crate) data: Tensor,     // shape = 2D (k,n)
    pub(crate) data_aux: Tensor, // shape = 1D (j,)
    pub(crate) string_cols: Vec<usize>,

    pub(crate) name2index: HashMap<String, usize>,
}

impl DataFramePayload {
    pub fn new(
        data: Tensor,
        data_aux: Tensor,
        string_cols: Vec<usize>,
        name2index: HashMap<String, usize>,
    ) -> DataFramePayload {
        DataFramePayload {
            data,
            data_aux,
            string_cols,
            name2index,
        }
    }
}

// pub trait DataAdapter {
//     type Error: Error; // TODO: Erro requires trait

//     fn valorize(&self) -> Result<DataFramePayload, Self::Error>;
// }
