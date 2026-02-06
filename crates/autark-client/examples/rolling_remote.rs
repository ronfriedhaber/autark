use autark_client::{OnceFrame, Result};
use autark_reader::readers::csv::CsvReader;
use autark_sinks::sink::stdout::SinkStdout;
use mpera::op::ReduceKind;
use std::hash::{DefaultHasher, Hash, Hasher};

const REMOTE_CSV: &str = "https://raw.githubusercontent.com/Snowflake-Labs/demo-datasets/refs/heads/main/avalanche/csv/order-history.csv";

fn main() -> Result<()> {
    let reader = CsvReader::new(REMOTE_CSV)?;
    let frame = OnceFrame::new(reader, SinkStdout {});

    let schema = frame.schema(None)?;
    let first_field = schema
        .fields()
        .first()
        .expect("remote csv should have at least one column");
    let first_column = first_field.name();

    frame
        .p
        .dataframe(None)?
        .col(first_column)?
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
