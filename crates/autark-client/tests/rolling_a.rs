use autark_client::{OnceFrame, Result};
use autark_sinks::sink::{stdout::SinkStdout, void::SinkVoid};
use std::hash::{DefaultHasher, Hash, Hasher};

use arrow::array::record_batch;
use autark_reader::readers::arrow::ArrowReader;
use mpera::op::ReduceKind;

fn hash_of_t<T: Hash>(x: &T) -> Option<u64> {
    let mut hasher = DefaultHasher::new();
    x.hash(&mut hasher);

    Some(hasher.finish())
}

#[test]
fn t1() -> Result<()> {
    let input = record_batch!(
        ("a", Int32, [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]),
        (
            "b",
            Float32,
            [0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4, 12.8, 25.6, 51.2, 102.4]
        ),
        (
            "c",
            Utf8,
            [
                "BOB", "ALICE", "BOB", "BOB", "ALICE", "ALICE", "ALICE", "BOB", "ALICE", "BOB",
                "ALICE"
            ]
        )
    )?;
    let reader = ArrowReader::new(input);

    let frame = OnceFrame::new(reader, SinkStdout {});

    frame
        .p
        .dataframe(None)?
        .col("a")?
        .reduce(ReduceKind::Sum)?
        .alias("a_sum", None)?; // Shall automatically detect result is 0-d scalar and not return a frame.

    frame
        .p
        .dataframe(None)?
        .col("b")?
        .reduce(ReduceKind::Sum)?
        .alias("b_sum", None)?; // Shall automatically detect result is 0-d scalar and not return a frame.

    frame
        .p
        .dataframe(None)?
        .col("a")?
        .rolling(4)?
        .reduce(ReduceKind::Mean)?
        .alias("rolling_mean", None)?;

    frame
        .p
        .dataframe(None)?
        .col("c")?
        .group_by(frame.p.dataframe(None)?.col("c")?, ReduceKind::Count)?
        .alias(
            "group_by_name",
            Some(frame.schema_of_columns(None, &["c", "a"])?),
        )?;

    let realized = frame.realize()?;
    dbg!(hash_of_t(&realized));

    // IMPORTANT: If underlying transformation is mutated, the Hash ought to be recomputed.
    assert_eq!(hash_of_t(&realized), Some(4566204555854787833));

    Ok(())
}
