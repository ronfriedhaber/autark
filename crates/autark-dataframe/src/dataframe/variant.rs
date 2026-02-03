use std::collections::HashMap;

use arrow::array::{Array, Int32Array, StringArray};
use arrow::datatypes::DataType;

use crate::Result;
use autark_tensor::Tensor;

pub(crate) fn encode_column(arr: &std::sync::Arc<dyn Array>, name: &str) -> Result<(Tensor, Vec<String>)> {
    if matches!(arr.data_type(), DataType::Utf8) {
        let arr = arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                arrow::error::ArrowError::InvalidArgumentError(
                    "Utf8 column type mismatch".to_string(),
                )
            })?;
        let (indices, map) = dictionary_encode(arr);
        let idx_arr: arrow::array::ArrayRef = std::sync::Arc::new(Int32Array::from(indices));
        let tensor = Tensor::try_from_arrow_1d(&idx_arr, name)?;
        return Ok((tensor, map));
    }

    let tensor = Tensor::try_from_arrow_1d(arr, name)?;
    Ok((tensor, Vec::new()))
}

fn dictionary_encode(arr: &StringArray) -> (Vec<i32>, Vec<String>) {
    let mut lookup: HashMap<String, i32> = HashMap::new();
    let mut map: Vec<String> = Vec::new();
    let mut indices: Vec<i32> = Vec::with_capacity(arr.len());

    for i in 0..arr.len() {
        let value = arr.value(i);
        let idx = match lookup.get(value) {
            Some(&ix) => ix,
            None => {
                let ix = map.len() as i32;
                let owned = value.to_string();
                map.push(owned.clone());
                lookup.insert(owned, ix);
                ix
            }
        };
        indices.push(idx);
    }

    (indices, map)
}
