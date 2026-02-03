use std::sync::Arc;

use arrow::array::{
    Array, Float32Array, Float64Array, Int32Array, Int64Array, StringArray, UInt8Array,
};

use crate::Result;

pub(crate) fn apply_variant_map(
    arrays: Vec<Arc<dyn Array>>,
    variant_map: &[Vec<String>],
) -> Result<Vec<Arc<dyn Array>>> {
    arrays
        .into_iter()
        .enumerate()
        .map(|(ix, arr)| {
            let map = match variant_map.get(ix) {
                Some(map) if !map.is_empty() => map,
                _ => return Ok(arr),
            };

            let indices = indices_from_array(arr.as_ref())?;
            let values = indices
                .into_iter()
                .map(|idx| map_index(map, idx))
                .collect::<Result<Vec<String>>>()?;
            Ok(Arc::new(StringArray::from(values)) as Arc<dyn Array>)
        })
        .collect()
}

fn indices_from_array(arr: &dyn Array) -> Result<Vec<i64>> {
    use arrow::datatypes::DataType::*;
    macro_rules! impl_arm {
        ($ty:ty, $cast:expr) => {{
            let a = arr.as_any().downcast_ref::<$ty>().unwrap();
            Ok((0..a.len()).map(|i| $cast(a.value(i))).collect())
        }};
    }
    macro_rules! impl_arm_try {
        ($ty:ty, $cast:expr) => {{
            let a = arr.as_any().downcast_ref::<$ty>().unwrap();
            let mut out = Vec::with_capacity(a.len());
            for i in 0..a.len() {
                out.push($cast(a.value(i), i)?);
            }
            Ok(out)
        }};
    }
    match arr.data_type() {
        Int32 => impl_arm!(Int32Array, |v: i32| v as i64),
        Int64 => impl_arm!(Int64Array, |v: i64| v),
        UInt8 => impl_arm!(UInt8Array, |v: u8| v as i64),
        Float32 => impl_arm_try!(Float32Array, |v: f32, i| -> Result<i64> {
            float_to_index(v as f64, i)
        }),
        Float64 => impl_arm_try!(Float64Array, |v: f64, i| -> Result<i64> {
            float_to_index(v, i)
        }),
        _ => Err(arrow::error::ArrowError::InvalidArgumentError(
            "variant map requires integer/float arrays".to_string(),
        )
        .into()),
    }
}

fn float_to_index(v: f64, i: usize) -> Result<i64> {
    if !v.is_finite() || v.fract() != 0.0 {
        return Err(crate::error::Error::ArrowError(
            arrow::error::ArrowError::InvalidArgumentError(format!(
                "variant map requires integer indices; got non-integer float at {i}: {v}"
            )),
        ));
    }
    Ok(v as i64)
}

fn map_index(map: &[String], idx: i64) -> Result<String> {
    if idx < 0 {
        return Err(arrow::error::ArrowError::InvalidArgumentError(
            "variant index must be non-negative".to_string(),
        )
        .into());
    }
    map.get(idx as usize).cloned().ok_or_else(|| {
        arrow::error::ArrowError::InvalidArgumentError(format!(
            "variant index out of bounds: {idx}"
        ))
        .into()
    })
}
