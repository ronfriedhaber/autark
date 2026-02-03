use std::collections::HashMap;

use arrow::array::{Array, Int32Array, StringArray};
use arrow::datatypes::DataType;

use crate::Result;
use autark_tensor::Tensor;

pub(crate) fn encode_column(
    arr: &std::sync::Arc<dyn Array>,
    name: &str,
) -> Result<(Tensor, Vec<String>)> {
    if matches!(arr.data_type(), DataType::Utf8) {
        let arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            arrow::error::ArrowError::InvalidArgumentError("Utf8 column type mismatch".to_string())
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
    let mut map: Vec<String> = arr
        .iter()
        .map(|value| value.unwrap_or("N/A").to_string())
        .collect();
    map.sort();
    map.dedup();

    let lookup: HashMap<String, i32> = map
        .iter()
        .enumerate()
        .map(|(ix, value)| (value.clone(), ix as i32))
        .collect();

    let indices: Vec<i32> = arr
        .iter()
        .map(|value| lookup[value.unwrap_or("N/A")])
        .collect();

    (indices, map)
}
