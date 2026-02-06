use crate::Result;
use crate::readers::OnceReader;
use autark_dataframe::DataFrame;
use autark_enhanced_reader::autoread_to_bytes;
use std::io::{BufReader, Cursor};
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow_json::{ReaderBuilder, reader::infer_json_schema_from_seekable};

pub struct JsonReader {
    reader: arrow_json::Reader<std::io::BufReader<Cursor<Vec<u8>>>>,
    schema: Arc<Schema>,
}

impl JsonReader {
    pub fn new(uri: &str) -> Result<JsonReader> {
        let bytes = autoread_to_bytes(uri)?;
        let mut reader = BufReader::new(Cursor::new(bytes));

        let (schema, _inferred_rows) =
            infer_json_schema_from_seekable(&mut reader, Some(1e6 as usize)).unwrap();

        dbg!(&schema);

        let schema = Arc::new(schema);
        let reader = ReaderBuilder::new(schema.clone()).build(reader).unwrap();

        Ok(JsonReader { reader, schema })
    }
}

impl OnceReader for JsonReader {
    fn read(&mut self) -> Result<DataFrame> {
        match self.reader.next() {
            Some(batch) => {
                let df = DataFrame::try_from(batch?)?;
                Ok(df)
            }
            None => Err(crate::Error::EmptyReader),
        }
    }

    fn schema(&self) -> Result<Arc<Schema>> {
        Ok(self.schema.clone())
    }
}
