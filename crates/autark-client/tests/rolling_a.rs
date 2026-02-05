use autark_client::{OnceFrame, Result};
use autark_sinks::sink::void::SinkVoid;
use std::hash::{DefaultHasher, Hash, Hasher};

use arrow::array::record_batch;
use autark_reader::readers::arrow::ArrowReader;
use mpera::op::ReduceOpKind;

fn hash_of_t<T: Hash>(x: &T) -> Option<u64> {
    let mut hasher = DefaultHasher::new();
    x.hash(&mut hasher);

    Some(hasher.finish())
}

#[test]
fn t1() -> Result<()> {
    let input = record_batch!(("a", Int32, [1, 2, 3]))?;
    let reader = ArrowReader::new(input);

    let frame = OnceFrame::new(reader, SinkVoid {});

    frame
        .p
        .dataframe(None)?
        .col("a")?
        .reduce(ReduceOpKind::Sum)?
        .alias("a_sum", None)?; // Shall automatixally detect result is 0-d scalar and not return a frame.

    let realized = frame.realize()?;
    dbg!(hash_of_t(&realized));

    // IMPORTANT: If underlying transformation is mutated, the Hash ought to be recomputed.
    assert_eq!(hash_of_t(&realized), Some(9527474090956641498));

    Ok(())
}
