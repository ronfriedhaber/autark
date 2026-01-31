use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Instant,
};

use arrow::{
    array::RecordBatch,
    datatypes::{Field, Fields, Schema},
};
use pyo3::prelude::*;

use crate::{
    artifact::Artifact, dataadapter::DataFramePayload, output::ProgramOutput,
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
    fn prep_input(input: &[DataFramePayload]) -> (Vec<Arc<Py<PyAny>>>, Vec<Py<PyAny>>) {
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
            Ok(name2index
                .into_iter()
                .map(|x| x.into_pyobject(py).unwrap().into_any().unbind())
                .collect())
        })
        .unwrap();

        (args_data, args_name2index)
    }

    pub fn run(&self, input: Vec<DataFramePayload>) -> ProgramOutput {
        let t0 = Instant::now();
        let t1 = Instant::now();

        let (args_data, args_name2index) = Runtime::prep_input(&input);
        println!("[MPERA] ARG PREP LAYER0 TOOK: {:?}", t1.elapsed());

        use pyo3::types::PyList;

        let out = with_tinygrad(|py| {
            let t1 = Instant::now();
            let py_args_data = PyList::new(py, args_data.iter().map(|data| &**data)).unwrap();

            println!("[MPERA] ARG PREP LAYER§ TOOK: {:?}", t1.elapsed());
            // dbg!(py_args_data.to_string());
            //
            let t1 = Instant::now();
            let out: Vec<(String, Py<PyAny>)> = self
                .artifact
                .object
                .call1(py, (&py_args_data, args_name2index))
                .unwrap()
                .extract()
                .unwrap();

            println!("[MPERA] CALL1 RAW0 TOOK: {:?}", t1.elapsed());

            Ok(out)
        })
        .unwrap();

        let t1 = Instant::now();
        let out: Vec<(String, Vec<Arc<dyn arrow::array::Array>>)> = out
            .into_iter()
            .map(|(k, v)| {
                let t = Tensor::new(v);
                // dbg!(&t.shape());

                match t.try_into_arrow_1d_or_2d_2() {
                    Ok(x) => (k.clone(), x),
                    Err(e) => panic!(),
                }
            })
            .collect();

        log::info!("[MPERA] OUT PARSE LAYER0 TOOK: {:?}", t1.elapsed());
        println!("{:?}", out);

        let schemas: Vec<Arc<Schema>> = out
            .iter()
            .map(|(_, out)| {
                Arc::new(arrow::datatypes::Schema {
                    fields: Fields::from_iter(out.iter().enumerate().map(|(ix, x)| {
                        Field::new(format!("column{ix}"), x.data_type().clone(), true)
                    })),
                    metadata: HashMap::with_capacity(0),
                })
            })
            .collect();

        let elapsed = t0.elapsed();
        let rbs: HashMap<String, RecordBatch> = out
            .iter()
            .zip(schemas)
            .map(|((key, data), schema)| {
                (
                    key.clone(),
                    RecordBatch::try_new(schema, data.clone()).unwrap(),
                )
            })
            .collect();

        println!("[MPERA] RUNTIME ELAPSED={elapsed:?}");

        ProgramOutput(rbs) // new type win
    }
}
