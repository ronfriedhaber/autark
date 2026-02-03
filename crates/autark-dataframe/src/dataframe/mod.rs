pub mod dtype;
pub(crate) mod variant;
// pub mod metadata;
// pub mod schema;

use std::collections::HashMap;

use arrow::{
    array::{RecordBatch, UInt32Array},
    compute::take,
    util::pretty::{pretty_format_batches, print_batches},
};
use autark_tensor::Tensor;
use mpera::dataadapter::DataFramePayload;

use crate::Result;
use crate::dataframe::variant::encode_column;

#[derive(Debug)]
pub struct DataFrame {
    // arrow_table: RecordBatch
    // metadata: DataFrameMetaData,
    record_batch: RecordBatch,

    data: Tensor,
    variant_map: Vec<Vec<String>>,
}

impl DataFrame {
    pub(crate) fn from_internal(record_batch: RecordBatch) -> Result<DataFrame> {
        let record_batch = drop_null_rows(&record_batch)?;
        // let data = Tensor::stack(&tensors).unwrap();
        // let data = tensors;
        let mut data: Vec<Tensor> = Vec::with_capacity(record_batch.num_columns());
        let mut variant_map: Vec<Vec<String>> = Vec::with_capacity(record_batch.num_columns());

        let schema = record_batch.schema();
        for (ix, x) in record_batch.columns().iter().enumerate() {
            let name = schema.fields()[ix].name();
            let (tensor, map) = encode_column(x, name)?;
            data.push(tensor);
            variant_map.push(map);
        }

        let data = Tensor::stack(&data).unwrap();
        // let data_aux = aux;

        Ok(DataFrame {
            record_batch,
            // metadata,
            data,
            variant_map,
            // data_aux,
        })
    }
}


fn drop_null_rows(record_batch: &RecordBatch) -> Result<RecordBatch> {
    if record_batch
        .columns()
        .iter()
        .all(|col| col.null_count() == 0)
    {
        return Ok(record_batch.clone());
    }

    let mut indices: Vec<u32> = Vec::with_capacity(record_batch.num_rows());
    'row: for row in 0..record_batch.num_rows() {
        for col in record_batch.columns().iter() {
            if col.is_null(row) {
                continue 'row;
            }
        }
        indices.push(row as u32);
    }

    let idx = UInt32Array::from(indices);
    let columns = record_batch
        .columns()
        .iter()
        .map(|col| Ok(take(col.as_ref(), &idx, None)?))
        .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()?;

    Ok(RecordBatch::try_new(record_batch.schema(), columns)?)
}

impl Into<DataFramePayload> for DataFrame {
    fn into(self) -> DataFramePayload {
        DataFramePayload::new(
            self.data,
            Tensor::from_slice(&[0.0]).unwrap(),
            self.record_batch
                .schema()
                .fields
                .iter()
                .enumerate()
                .map(|(ix, field)| (field.name().clone(), ix))
                .collect::<HashMap<String, usize>>(),
            self.variant_map,
            // TODO: Move this to pure fnuction, perchance as part of arrow-interop mod
        )
    }
}

impl std::fmt::Display for DataFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            pretty_format_batches(&[self.record_batch.clone()])
                .expect("Error pretty formatting dataframe.")
        )?;
        Ok(())
    }
}
