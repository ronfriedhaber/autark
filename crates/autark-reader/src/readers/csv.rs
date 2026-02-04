use crate::Result;
use crate::readers::OnceReader;
use autark_dataframe::DataFrame;
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

impl OnceReader for CsvReader {
    type Error = crate::Error;

    fn read(mut self) -> Result<DataFrame> {
        match self.reader.next() {
            Some(batch) => {
                let df = DataFrame::try_from(batch?)?;
                Ok(df)
            }
            None => Err(crate::Error::EmptyReader),
        }
    }
}
