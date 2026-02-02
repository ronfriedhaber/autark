pub mod dtype;
// pub mod metadata;
// pub mod schema;

use std::collections::HashMap;

use arrow::{
    array::RecordBatch,
    util::pretty::{pretty_format_batches, print_batches},
};
use autark_tensor::Tensor;
use mpera::dataadapter::{DataFramePayload, DataFramePayloadMetadata};

use crate::Result;

#[derive(Debug)]
pub struct DataFrame {
    // arrow_table: RecordBatch
    // metadata: DataFrameMetaData,
    record_batch: RecordBatch,

    data: Tensor,
    data_aux: Tensor,
}

impl DataFrame {
    pub(crate) fn from_internal(record_batch: RecordBatch) -> Result<DataFrame> {
        let mut data_aux_buf: Vec<u8> = Vec::new();
        let data = record_batch
            .columns()
            .iter()
            .enumerate()
            .map(|(ix, x)| {
                Tensor::try_from_arrow_1d(
                    x,
                    record_batch.schema().fields()[ix].name(),
                    &mut data_aux_buf,
                )
                .unwrap()
            })
            .collect::<Vec<Tensor>>();

        let data = Tensor::stack(&data).unwrap();

        let data_aux = Tensor::from_slice(data_aux_buf.as_slice())?;

        Ok(DataFrame {
            record_batch,
            // metadata,
            data,
            data_aux,
            // data_aux,
        })
    }
}

impl Into<DataFramePayload> for DataFrame {
    fn into(self) -> DataFramePayload {
        let rows = self.record_batch.num_rows();
        let cols = self.record_batch.num_columns();
        let mut nulls_buf: Vec<u8> = Vec::with_capacity(rows * cols);
        let mut any_null = false;
        for c in self.record_batch.columns().iter() {
            for i in 0..rows {
                let v = if c.is_valid(i) { 1u8 } else { 0u8 };
                if v == 0 {
                    any_null = true;
                }
                nulls_buf.push(v);
            }
        }
        DataFramePayload::new(
            self.data,
            self.data_aux,
            DataFramePayloadMetadata::new(
                self.record_batch
                    .schema()
                    .fields()
                    .iter()
                    .enumerate()
                    .filter_map(|(ix, f)| match f.data_type() {
                        arrow::datatypes::DataType::Utf8
                        | arrow::datatypes::DataType::LargeUtf8 => Some(ix),
                        _ => None,
                    })
                    .collect(),
                if any_null {
                    Some(Tensor::from_slice(nulls_buf.as_slice()).unwrap())
                } else {
                    None
                },
            ),
            self.record_batch
                .schema()
                .fields
                .iter()
                .enumerate()
                .map(|(ix, field)| (field.name().clone(), ix))
                .collect::<HashMap<String, usize>>(),
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
