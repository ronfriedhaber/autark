use crate::readers::Reader;
use crate::{DataFrame, Result};
use std::path::PathBuf;

use std::{fs::File, io::BufReader, sync::Arc};

use arrow_json::{ReaderBuilder, reader::infer_json_schema_from_seekable};

pub struct JsonReader {
    reader: arrow_json::Reader<std::io::BufReader<std::fs::File>>,
}

impl JsonReader {
    pub fn new(path: PathBuf) -> Result<JsonReader> {
        let file = File::open(path).unwrap();
        let mut reader = BufReader::new(file);

        let (schema, _inferred_rows) =
            infer_json_schema_from_seekable(&mut reader, Some(1e6 as usize)).unwrap();

        dbg!(&schema);

        let reader = ReaderBuilder::new(Arc::new(schema)).build(reader).unwrap();

        Ok(JsonReader { reader })
    }
}

impl Reader for JsonReader {
    type Error = crate::Error;

    fn next(&mut self) -> crate::Result<Option<crate::DataFrame>> {
        if let Some(Ok(x)) = self.reader.next() {
            let df = DataFrame::try_from(x)?;
            return Ok(Some(df));
        }

        Ok(None)
    }
}
