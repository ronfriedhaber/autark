pub mod dtype;
// pub mod metadata;
// pub mod schema;

use std::collections::HashMap;

use arrow::{
    array::RecordBatch,
    util::pretty::{pretty_format_batches, print_batches},
};
use autark_tensor::Tensor;
use mpera::dataadapter::DataFramePayload;

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
        // Shall return auxillary buffer
        let data = record_batch
            .columns()
            .iter()
            .enumerate()
            .map(|(ix, x)| {
                Tensor::try_from_arrow_1d(x, record_batch.schema().fields()[ix].name()).unwrap()
            })
            .collect::<Vec<Tensor>>();

        let data = Tensor::stack(&data).unwrap();

        // TEMP
        let data_aux = Tensor::from_slice(&[1.0, 2.0, 3.0])?;

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
        DataFramePayload::new(
            self.data,
            self.data_aux,
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
