use mpera::{
    output::{ProgramOutput, fuse},
    pipeline::Pipeline,
    program::Program,
    programpayload::ProgramPayload,
    runtime::Runtime,
};

use crate::{Error, Result, readers::Reader, sink::Sink};

pub struct OnceFrame<R: Reader, S: Sink> {
    reader: R,
    sink: S,
    pub p: Program,
}

impl<R: Reader, S: Sink> OnceFrame<R, S> {
    pub fn new(reader: R, sink: S) -> OnceFrame<R, S> {
        OnceFrame {
            reader,
            sink,
            p: Program::new(),
        }
    }

    // shall take S: Sink
    // thinking: what happens if needers more than one frame, perchance acept sequence of reader
    pub fn realize(mut self) -> Result<()> {
        let pipeline = Pipeline::new(self.p);
        let artifact = pipeline.run()?;
        let runtime = Runtime::new(artifact);

        let mut outputs: Vec<ProgramOutput> = Vec::new();

        while let Some(x) = self.reader.next()? {
            let output = runtime.run(ProgramPayload::new(vec![x.into()])?)?;
            outputs.push(output);
        }

        self.sink
            .sink(fuse(&outputs))
            .map_err(|err| Error::Sink(err.to_string()))?;

        Ok(())
    }
}
