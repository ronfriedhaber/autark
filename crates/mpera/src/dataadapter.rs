use autark_tensor::Tensor;
use std::collections::HashMap;

pub struct DataFramePayloadMetadata {
    pub(crate) string_cols: Vec<usize>,
    pub(crate) nulls: Option<Tensor>,
}

impl DataFramePayloadMetadata {
    pub fn new(string_cols: Vec<usize>, nulls: Option<Tensor>) -> Self {
        Self { string_cols, nulls }
    }
}

pub struct DataFramePayload {
    pub(crate) data: Tensor,     // shape = 2D (k,n)
    pub(crate) data_aux: Tensor, // shape = 1D (j,)
    pub(crate) metadata: DataFramePayloadMetadata,

    pub(crate) name2index: HashMap<String, usize>,
}

impl DataFramePayload {
    pub fn new(
        data: Tensor,
        data_aux: Tensor,
        metadata: DataFramePayloadMetadata,
        name2index: HashMap<String, usize>,
    ) -> DataFramePayload {
        DataFramePayload {
            data,
            data_aux,
            metadata,
            name2index,
        }
    }
}

// pub trait DataAdapter {
//     type Error: Error; // TODO: Erro requires trait

//     fn valorize(&self) -> Result<DataFramePayload, Self::Error>;
// }
