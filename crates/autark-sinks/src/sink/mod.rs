pub mod csv;
pub mod stdout;
pub mod void;

use mpera::output::ProgramOutput;

pub trait Sink: Sized {
    type Error: std::error::Error;

    // fn init() -> std::result::Result<Self, Self::Error>;
    fn sink(&self, output: ProgramOutput) -> std::result::Result<(), Self::Error>;
    fn finish(self) -> std::result::Result<(), Self::Error>;
}
