use crate::readers::Reader;
use crate::{DataFrame, Result};
use std::path::PathBuf;

use std::{
    fs::File,
    io::{BufReader, Seek, SeekFrom},
    sync::Arc,
};

use arrow_csv::{ReaderBuilder, reader::Format};

pub struct CsvReader {
    reader: arrow_csv::reader::BufReader<std::io::BufReader<std::io::BufReader<std::fs::File>>>,
}

impl CsvReader {
    pub fn new(path: PathBuf) -> Result<CsvReader> {
        let mut file = File::open(path).unwrap();

        let (schema, _inferred_rows) = Format::default()
            .with_header(true)
            .infer_schema(&mut file, Some(1e6 as usize))
            .unwrap();

        dbg!(&schema);

        file.seek(SeekFrom::Start(0)).unwrap();

        let reader = ReaderBuilder::new(Arc::new(schema))
            .with_header(true)
            .build(BufReader::new(file))
            .unwrap();

        Ok(CsvReader { reader })
    }
}

impl Reader for CsvReader {
    type Error = crate::Error;

    fn next(&mut self) -> crate::Result<Option<crate::DataFrame>> {
        if let Some(Ok(x)) = self.reader.next() {
            let df = DataFrame::try_from(x)?; // dbg!(&x);
            // println!("{df}");
            return Ok(Some(df));
        }

        Ok(None)
    }
}
