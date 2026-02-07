use mpera::{
    output::{ProgramOutput, fuse},
    pipeline::Pipeline,
    program::Program,
    programpayload::ProgramPayload,
    runtime::Runtime,
};

use crate::{Error, Result};
use arrow::datatypes::Schema;
use autark_reader::OnceReader;
use autark_sinks::Sink;

pub struct OnceFrame<S: Sink> {
    readers: Vec<Box<dyn OnceReader>>,
    sinks: Vec<S>,
    // realized: Option<ProgramOutput>, // Perchance can be generic.
    pub p: Program,
}

// TODO: Sep file
#[derive(Debug, Hash)]
pub struct RealizedOnceFrame {
    program_output: ProgramOutput,
}

impl<S: Sink> OnceFrame<S> {
    pub fn new<R: OnceReader + 'static>(reader: R, sink: S) -> OnceFrame<S> {
        OnceFrame {
            readers: vec![Box::new(reader)],
            sinks: vec![sink],
            p: Program::new(),
        }
    }

    pub fn schema(&self, index: Option<usize>) -> Result<Schema> {
        let idx = index.unwrap_or(0);
        let reader = self.readers.get(idx).ok_or(Error::EmptyReader)?;
        Ok(reader.schema()?.as_ref().clone())
    }

    pub fn schema_of_columns(&self, index: Option<usize>, columns: &[&str]) -> Result<Schema> {
        let idx = index.unwrap_or(0);
        let reader = self.readers.get(idx).ok_or(Error::EmptyReader)?;
        let schema = reader.schema()?;
        let fields: Result<Vec<_>> = columns
            .iter()
            .map(|name| Ok(schema.field_with_name(name)?.clone()))
            .collect();
        let fields = fields?;
        Ok(Schema::new(fields))
    }

    pub fn with_reader<R: OnceReader + 'static>(mut self, reader: R) -> OnceFrame<S> {
        self.readers.push(Box::new(reader));
        self
    }

    pub fn with_sink(mut self, sink: S) -> OnceFrame<S> {
        self.sinks.push(sink);
        self
    }

    // shall take S: Sink
    // thinking: what happens if needers more than one frame, perchance acept sequence of reader
    pub fn realize(self) -> Result<RealizedOnceFrame> {
        let pipeline = Pipeline::new(self.p);
        let artifact = pipeline.run()?;
        let runtime = Runtime::new(artifact);

        let dataframes = self
            .readers
            .into_iter()
            .map(|mut reader| Ok(reader.read()?.into()))
            .collect::<Result<Vec<_>>>()?;

        let output = runtime.run(ProgramPayload::new(dataframes)?)?;
        let fused_output = fuse(&[output.clone()]);
        self.sinks.iter().try_for_each(|sink| {
            sink.sink(fused_output.clone())
                .map_err(|err| Error::Sink(err.to_string()))
        })?;

        Ok(RealizedOnceFrame {
            program_output: output,
        })
    }
}
