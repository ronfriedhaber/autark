use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Instant,
};

use arrow::{
    array::{ArrayData, RecordBatch, make_array},
    buffer::Buffer,
    datatypes::{Field, Fields, Schema},
    util::bit_util,
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
    fn prep_input(
        input: &[DataFramePayload],
    ) -> Result<(Vec<Arc<Py<PyAny>>>, Vec<Arc<Py<PyAny>>>, Vec<Py<PyAny>>)> {
        let payloads = input;
        let data: Vec<Tensor> = payloads.iter().map(|x| x.data.clone()).collect();
        let data_aux: Vec<Tensor> = payloads.iter().map(|x| x.data_aux.clone()).collect();
        let name2index: Vec<HashMap<String, usize>> =
            payloads.iter().map(|x| x.name2index.clone()).collect();

        let args_data: Vec<Arc<Py<PyAny>>> = data.iter().map(|x| x.inner_cloned()).collect();
        let args_data_aux: Vec<Arc<Py<PyAny>>> =
            data_aux.iter().map(|x| x.inner_cloned()).collect();
        let args_name2index: Vec<Py<PyAny>> = with_tinygrad(|py| {
            name2index
                .into_iter()
                .map(|x| Ok(x.into_pyobject(py)?.into_any().unbind()))
                .collect::<Result<Vec<Py<PyAny>>>>()
        })?;

        Ok((args_data, args_data_aux, args_name2index))
    }

    pub fn run(&self, input: Vec<DataFramePayload>) -> Result<ProgramOutput> {
        let t0 = Instant::now();
        let t1 = Instant::now();

        let (args_data, args_data_aux, args_name2index) = Runtime::prep_input(&input)?;
        let out_data_aux = input.get(0).map(|x| x.data_aux.clone());
        let out_string_cols = input
            .get(0)
            .map(|x| x.metadata.string_cols.clone())
            .unwrap_or_default();
        let out_nulls = input.get(0).and_then(|x| x.metadata.nulls.clone());
        println!("[MPERA] ARG PREP LAYER0 TOOK: {:?}", t1.elapsed());

        use pyo3::types::PyList;

        let out = with_tinygrad(|py| {
            let t1 = Instant::now();
            let py_args_data = PyList::new(py, args_data.iter().map(|data| &**data))?;
            let py_args_data_aux = PyList::new(py, args_data_aux.iter().map(|data| &**data))?;

            println!("[MPERA] ARG PREP LAYER§ TOOK: {:?}", t1.elapsed());
            // dbg!(py_args_data.to_string());
            //
            let t1 = Instant::now();
            let out: Vec<(String, Py<PyAny>)> = self
                .artifact
                .object
                .call1(py, (&py_args_data, &py_args_data_aux, args_name2index))?
                .extract()?;

            println!("[MPERA] CALL1 RAW0 TOOK: {:?}", t1.elapsed());

            Ok(out)
        })?;

        let t1 = Instant::now();
        let nulls_raw: Option<Vec<u8>> = out_nulls.as_ref().and_then(|n| n.to_u8_vec().ok());

        let out: Vec<(String, Vec<Arc<dyn arrow::array::Array>>)> = out
            .into_iter()
            .map(|(k, v)| {
                let t = Tensor::new(v);
                let t_shape = t.shape();
                let rows = match t_shape.as_slice() {
                    [n] => *n,
                    [n, _] => *n,
                    _ => 0,
                };
                let cols = match t_shape.as_slice() {
                    [n] => 1,
                    [_, m] => *m,
                    _ => 0,
                };
                let string_cols = if out_string_cols.iter().all(|&ix| ix < rows) {
                    out_string_cols.as_slice()
                } else {
                    &[]
                };
                dbg!(&string_cols);
                // dbg!(&t.shape());

                let res = match (out_data_aux.as_ref(), string_cols.is_empty()) {
                    (Some(aux), false) => t
                        .try_into_arrow_1d_or_2d_with_aux(aux, string_cols)
                        .or_else(|_| t.try_into_arrow_1d_or_2d_2()),
                    _ => t.try_into_arrow_1d_or_2d_2(),
                };
                match res {
                    Ok(x) => {
                        let x = if let Some(n) = nulls_raw.as_ref() {
                            if rows != 0 && cols != 0 && n.len() == rows * cols && x.len() == rows {
                                x.into_iter()
                                    .enumerate()
                                    .map(|(ix, arr)| {
                                        let start = ix * cols;
                                        let end = start + cols;
                                        let mask = &n[start..end];
                                        if mask.iter().all(|&v| v != 0) {
                                            return arr;
                                        }
                                        let data = arr.to_data();
                                        if data.len() != cols {
                                            return arr;
                                        }
                                        let offset = data.offset();
                                        let mut bits = vec![0u8; (offset + cols + 7) / 8];
                                        for (i, &v) in mask.iter().enumerate() {
                                            if v != 0 {
                                                bit_util::set_bit(&mut bits, offset + i);
                                            }
                                        }
                                        let mut b = ArrayData::builder(data.data_type().clone())
                                            .len(data.len())
                                            .offset(offset);
                                        for buf in data.buffers() {
                                            b = b.add_buffer(buf.clone());
                                        }
                                        for child in data.child_data() {
                                            b = b.add_child_data(child.clone());
                                        }
                                        let data = b
                                            .null_bit_buffer(Some(Buffer::from(bits)))
                                            .build()
                                            .unwrap();
                                        make_array(data)
                                    })
                                    .collect()
                            } else {
                                x
                            }
                        } else {
                            x
                        };
                        (k.clone(), x)
                    }
                    Err(e) => panic!(),
                }
            })
            .collect();

        log::info!("[MPERA] OUT PARSE LAYER0 TOOK: {:?}", t1.elapsed());
        println!("{:?}", out);

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
