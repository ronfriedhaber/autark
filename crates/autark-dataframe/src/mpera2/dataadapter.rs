use crate::tensor::Tensor;
use std::collections::HashMap;
use std::error::Error;

pub struct DataFramePayload {
    pub(crate) data: Tensor, // Shape = 2D (k,n)

    pub(crate) name2index: HashMap<String, usize>,
}

impl DataFramePayload {
    pub fn new(data: Tensor, name2index: HashMap<String, usize>) -> DataFramePayload {
        DataFramePayload { data, name2index }
    }
}

pub trait DataAdapter {
    type Error: Error; // TODO: Erro requires trait

    fn valorize(&self) -> Result<DataFramePayload, Self::Error>;
}
