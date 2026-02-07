use crate::Result;
use crate::readers::OnceReader;
use autark_dataframe::DataFrame;
use autark_enhanced_reader::autoread_to_bytes;
use std::io::{Cursor, Seek, SeekFrom};
use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::datatypes::Schema;
use arrow_csv::{ReaderBuilder, reader::Format};

pub struct CsvReader {
    reader: arrow_csv::reader::BufReader<std::io::BufReader<Cursor<Vec<u8>>>>,
    schema: Arc<Schema>,
}

impl CsvReader {
    pub fn new(uri: &str) -> Result<CsvReader> {
        let bytes = autoread_to_bytes(uri)?;
        let mut cursor = Cursor::new(bytes);

        let (schema, _inferred_rows) = Format::default()
            .with_header(true)
            .infer_schema(&mut cursor, Some(1e6 as usize))
            .unwrap();

        dbg!(&schema);

        cursor.seek(SeekFrom::Start(0)).unwrap();

        let schema = Arc::new(schema);
        let reader = ReaderBuilder::new(schema.clone())
            .with_header(true)
            .build(cursor)
            .unwrap();

        Ok(CsvReader { reader, schema })
    }
}

impl OnceReader for CsvReader {
    fn read(&mut self) -> Result<DataFrame> {
        let batches: Vec<_> = self
            .reader
            .by_ref()
            .map(|batch| batch.map_err(Into::into))
            .collect::<Result<Vec<_>>>()?;

        if batches.is_empty() {
            return Err(crate::Error::EmptyReader);
        }

        let batch = concat_batches(&self.schema, &batches)?;
        Ok(DataFrame::try_from(batch)?)
    }

    fn schema(&self) -> Result<Arc<Schema>> {
        Ok(self.schema.clone())
    }
}
