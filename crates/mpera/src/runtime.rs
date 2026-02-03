use std::{collections::HashMap, sync::Arc, time::Instant};

use arrow::{
    array::{Array, RecordBatch},
    datatypes::{Field, Fields, Schema},
};
use pyo3::prelude::*;

use crate::{
    Result, artifact::Artifact, output::ProgramOutput, postprocessing::apply_variant_map,
    programpayload::ProgramPayload, with_tinygrad::with_tinygrad,
};
use autark_dataframe::DataFramePayload;
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

    pub fn run(&self, input: ProgramPayload) -> Result<ProgramOutput> {
        let ProgramPayload {
            dataframes: input,
            variant_map,
        } = input;
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
