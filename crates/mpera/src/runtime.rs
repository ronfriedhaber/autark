use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Instant,
};

use arrow::{
    array::{
        Array, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
        UInt8Array,
    },
    datatypes::{Field, Fields, Schema},
};
use pyo3::prelude::*;

use crate::{
    Result, artifact::Artifact, dataadapter::DataFramePayload, output::ProgramOutput,
    with_tinygrad::with_tinygrad,
};
use autark_tensor::Tensor;

pub struct Runtime {
    artifact: Artifact,
}

impl Runtime {
    // artifact needs metadata
    pub fn new(artifact: Artifact) -> Runtime {
        Runtime { artifact }
    }

    #[inline]
    fn prep_input(input: &[DataFramePayload]) -> Result<(Vec<Arc<Py<PyAny>>>, Vec<Py<PyAny>>)> {
        let payloads = input;
        let data: Vec<Tensor> = payloads.iter().map(|x| x.data.clone()).collect();
        let _data_aux: Vec<Tensor> = payloads.iter().map(|x| x.data_aux.clone()).collect();
        let name2index: Vec<HashMap<String, usize>> =
            payloads.iter().map(|x| x.name2index.clone()).collect();

        let args_data: Vec<Arc<Py<PyAny>>> = data
            .iter()
            .enumerate()
            .map(|(ix, x)| x.inner_cloned())
            .collect();
        let args_name2index: Vec<Py<PyAny>> = with_tinygrad(|py| {
            name2index
                .into_iter()
                .map(|x| Ok(x.into_pyobject(py)?.into_any().unbind()))
                .collect::<Result<Vec<Py<PyAny>>>>()
        })?;

        Ok((args_data, args_name2index))
    }

    fn extract_output(
        out: Vec<(String, Py<PyAny>)>,
        variant_map: &[Vec<String>],
    ) -> Result<Vec<(String, Vec<Arc<dyn Array>>)>> {
        out.into_iter()
            .map(|(k, v)| {
                let t = Tensor::new(v);
                let arrays = t.try_into_arrow_1d_or_2d_2()?;
                let arrays = apply_variant_map(arrays, variant_map)?;
                Ok((k, arrays))
            })
            .collect::<Result<Vec<(String, Vec<Arc<dyn Array>>)>>>()
    }

    pub fn run(&self, input: Vec<DataFramePayload>) -> Result<ProgramOutput> {
        let t0 = Instant::now();
        let t1 = Instant::now();

        let (args_data, args_name2index) = Runtime::prep_input(&input)?;
        println!("[MPERA] ARG PREP LAYER0 TOOK: {:?}", t1.elapsed());

        use pyo3::types::PyList;

        let out = with_tinygrad(|py| {
            let t1 = Instant::now();
            let py_args_data = PyList::new(py, args_data.iter().map(|data| &**data))?;

            println!("[MPERA] ARG PREP LAYER§ TOOK: {:?}", t1.elapsed());
            // dbg!(py_args_data.to_string());
            //
            let t1 = Instant::now();
            let out: Vec<(String, Py<PyAny>)> = self
                .artifact
                .object
                .call1(py, (&py_args_data, args_name2index))?
                .extract()?;

            println!("[MPERA] CALL1 RAW0 TOOK: {:?}", t1.elapsed());

            Ok(out)
        })?;

        let t1 = Instant::now();

        log::info!("[MPERA] OUT PARSE LAYER0 TOOK: {:?}", t1.elapsed());
        println!("{:?}", out);
        let variant_map = input
            .first()
            .map(|payload| payload.variant_map.clone())
            .unwrap_or_default();
        let out = Self::extract_output(out, &variant_map)?;

        let schemas: Vec<Arc<Schema>> = out
            .iter()
            .map(|(k, out)| {
                Arc::new(arrow::datatypes::Schema {
                    fields: Fields::from_iter(out.iter().enumerate().map(|(ix, x)| {
                        Field::new(format!("column{ix}"), x.data_type().clone(), true)
                    })),
                    metadata: HashMap::with_capacity(0),
                })
            })
            .collect();

        let elapsed = t0.elapsed();
        let rbs: Result<HashMap<String, RecordBatch>> = out
            .iter()
            .zip(schemas)
            .map(
                |((key, data), schema)| match RecordBatch::try_new(schema, data.clone()) {
                    Ok(x) => Ok((key.clone(), x)),
                    Err(e) => Err(e.into()),
                },
            )
            .collect();

        println!("[MPERA] RUNTIME ELAPSED={elapsed:?}");

        Ok(ProgramOutput(rbs?)) // new type win
    }
}

fn apply_variant_map(
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
    match arr.data_type() {
        Int32 => {
            let a = arr.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok((0..a.len()).map(|i| a.value(i) as i64).collect())
        }
        Int64 => {
            let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok((0..a.len()).map(|i| a.value(i)).collect())
        }
        UInt8 => {
            let a = arr.as_any().downcast_ref::<UInt8Array>().unwrap();
            Ok((0..a.len()).map(|i| a.value(i) as i64).collect())
        }
        Float32 => {
            let a = arr.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok((0..a.len()).map(|i| a.value(i) as i64).collect())
        }
        Float64 => {
            let a = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok((0..a.len()).map(|i| a.value(i) as i64).collect())
        }
        _ => Err(arrow::error::ArrowError::InvalidArgumentError(
            "variant map requires integer/float arrays".to_string(),
        )
        .into()),
    }
}

fn map_index(map: &[String], idx: i64) -> Result<String> {
    if idx < 0 {
        return Err(arrow::error::ArrowError::InvalidArgumentError(
            "variant index must be non-negative".to_string(),
        )
        .into());
    }
    map.get(idx as usize)
        .cloned()
        .ok_or_else(|| {
            arrow::error::ArrowError::InvalidArgumentError(format!(
                "variant index out of bounds: {idx}"
            ))
            .into()
        })
}
