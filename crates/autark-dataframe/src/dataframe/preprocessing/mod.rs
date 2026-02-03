use arrow::{
    array::{RecordBatch, UInt32Array},
    compute::take,
};

use crate::Result;

pub(crate) fn drop_null_rows(record_batch: &RecordBatch) -> Result<RecordBatch> {
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
