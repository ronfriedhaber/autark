use crate::Result;
use crate::readers::OnceReader;
use autark_dataframe::DataFrame;
use std::path::PathBuf;

use std::{fs::File, io::BufReader, sync::Arc};

use arrow_json::{ReaderBuilder, reader::infer_json_schema_from_seekable};
use arrow::datatypes::Schema;

pub struct JsonReader {
    reader: arrow_json::Reader<std::io::BufReader<std::fs::File>>,
    schema: Arc<Schema>,
}

impl JsonReader {
    pub fn new(path: PathBuf) -> Result<JsonReader> {
        let file = File::open(path).unwrap();
        let mut reader = BufReader::new(file);

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
