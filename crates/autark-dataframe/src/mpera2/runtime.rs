use std::{collections::HashMap, sync::Arc};

use pyo3::prelude::*;

use crate::{
    artifact::Artifact,
    dataadapter::{DataAdapter, DataFramePayload},
    flag::{DebugLevel, debug_level},
    tensor::Tensor,
    with_tinygrad::with_tinygrad,
};

pub struct Runtime {
    artifact: Artifact,
}

impl Runtime {
    // artifact needs metadata
    pub fn new(artifact: Artifact) -> Runtime {
        Runtime { artifact }
    }

    pub fn run(&self, data: Tensor, data_aux: Tensor) -> serde_json::Value {
        let payloads: Vec<DataFramePayload> =
            input.into_iter().map(|x| x.valorize().unwrap()).collect();

        // Contemplate on this clone
        let data: Vec<Tensor> = payloads.iter().map(|x| x.data.clone()).collect();
        let name2index: Vec<HashMap<String, usize>> =
            payloads.iter().map(|x| x.name2index.clone()).collect();

        let args_data: Vec<Arc<Py<PyAny>>> = data.iter().map(|x| (x.inner.clone())).collect();
        let args_name2index: Vec<Py<PyAny>> = with_tinygrad(|py| {
            Ok(name2index
                .into_iter()
                .map(|x| x.into_pyobject(py).unwrap().into_any().unbind())
                .collect())
        })
        .unwrap();

        if debug_level() == Some(DebugLevel::II) {
            for (ix, _) in args_data.iter().enumerate() {
                println!("DATA INDEX={} SHAPE={:?}", ix, data[ix].shape());
            }
        }
        // let args_data: &pyo3::Py<pyo3::PyAny> = &*args_data;
        use pyo3::types::PyList;

        let out = with_tinygrad(|py| {
            let py_args_data = PyList::new(py, args_data.iter().map(|a| &**a)).unwrap();
            let out = self
                .artifact
                .object
                .call1(py, (&py_args_data, args_name2index))
                .unwrap();

            Ok(out.to_string())
        })
        .unwrap();

        let out = out
            .replace("'", "\"")
            .replace("False", "false")
            .replace("True", "true");

        // dbg!(&out);
        let out: serde_json::Value = serde_json::from_str(&out).unwrap();
        // dbg!(&out);

        out
    }
}
