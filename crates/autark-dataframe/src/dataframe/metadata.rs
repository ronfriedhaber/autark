use crate::dataframe::schema::Schema;

#[derive(Debug)]
pub(crate) struct DataFrameMetaData {
    pub(crate) schema: Schema,
    length: usize,
}

impl DataFrameMetaData {
    pub fn new(schema: Schema, length: usize) -> DataFrameMetaData {
        DataFrameMetaData { schema, length }
    }
}
