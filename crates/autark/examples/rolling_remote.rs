// use autark_client::{OnceFrame, Result};
use autark::prelude::*;
use autark_reader::readers::csv::CsvReader;
use autark_sinks::sink::stdout::SinkStdout;

const REMOTE_CSV: &str = "https://raw.githubusercontent.com/microsoft/DataStoriesSamples/refs/heads/master/samples/FraudDetectionOnADL/Data/transactions.csv";

fn main() -> AutarkResult<()> {
    let reader = CsvReader::new(REMOTE_CSV)?;

    let frame = OnceFrame::new(reader, SinkStdout {});
    let schema = frame.schema(None)?;

    frame
        .p
        .dataframe(None)?
        .reduce(ReduceKind::Count)?
        .alias("row_count", None)?;

    frame
        .p
        .dataframe(None)?
        .slice(0, 10)?
        .alias("df", Some(frame.schema(None)?))?;
    let _ = frame.realize()?;

    Ok(())
}
