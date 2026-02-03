use std::collections::HashMap;

use arrow::array::{Array, Int32Array, StringArray};
use arrow::datatypes::DataType;

use crate::Result;
use autark_tensor::Tensor;

use std::collections::hash_map::Entry;

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
    let (_, map, indices) = arr.iter().map(|value| value.unwrap()).fold(
        (HashMap::new(), Vec::new(), Vec::with_capacity(arr.len())),
        |(mut lookup, mut map, mut indices), value| {
            let owned = value.to_string();
            let idx = match lookup.entry(owned) {
                Entry::Occupied(entry) => *entry.get(),
                Entry::Vacant(entry) => {
                    let ix = map.len() as i32;
                    map.push(entry.key().clone());
                    entry.insert(ix);
                    ix
                }
            };
            indices.push(idx);
            (lookup, map, indices)
        },
    );

    (indices, map)
}
