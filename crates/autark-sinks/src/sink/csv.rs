use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use arrow::csv::{Writer, WriterBuilder};
use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use mpera::output::ProgramOutput;

use crate::sink::Sink;

pub struct CsvSink {
    base: PathBuf,
    inner: Mutex<Inner>,
}

struct Inner {
    writers: HashMap<String, (Writer<BufWriter<File>>, Arc<Schema>)>,
}

impl CsvSink {
    pub fn new(base: PathBuf) -> Result<Self, ArrowError> {
        Ok(Self {
            base,
            inner: Mutex::new(Inner {
                writers: HashMap::new(),
            }),
        })
    }

    fn write_named_batch(&self, name: &str, batch: &RecordBatch) -> Result<(), ArrowError> {
        let mut inner = self.inner.lock().unwrap();

        if let Some((w, _schema)) = inner.writers.get_mut(name) {
            w.write(batch)?;
            return Ok(());
        }

        let path = self.base.join(format!("{name}.csv"));
        let file = BufWriter::new(File::create(&path)?);

        let mut writer = WriterBuilder::new().with_header(true).build(file);

        writer.write(batch)?;
        inner
            .writers
            .insert(name.to_string(), (writer, batch.schema()));

        Ok(())
    }
}

impl Sink for CsvSink {
    type Error = ArrowError;

    fn sink(&self, output: ProgramOutput) -> Result<(), Self::Error> {
        for (name, batch) in output.0 {
            self.write_named_batch(&name, &batch)?;
        }
        Ok(())
    }

    fn finish(self) -> Result<(), Self::Error> {
        Ok(())
    }
}
