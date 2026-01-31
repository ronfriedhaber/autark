use arrow::util::pretty::print_batches;
use autark_dataframe::{
    Program,
    onceframe::OnceFrame,
    sink::{Sink, csv::CsvSink, stdout::SinkStdout, void::SinkVoid},
};
use std::{path::PathBuf, str::FromStr};

use autark_dataframe::{
    DataFrame,
    readers::{Reader, csv::CsvReader},
};
use mpera::{
    op::{BinaryOpKind, ReduceOpKind},
    pipeline::Pipeline,
    runtime::Runtime,
};

use crate::common::hash_of_dir;

mod common;

#[test]
fn t0() {
    let csv_reader =
        CsvReader::new(PathBuf::from_str("../../extra/datasets/winequality.csv").unwrap()).unwrap();

    let frame = OnceFrame::new(
        csv_reader,
        CsvSink::new(PathBuf::from_str("./tmp").unwrap()).unwrap(),
    );

    let mu = frame
        .p
        .dataframe(None)
        .col("quality")
        .reduce(ReduceOpKind::Mean);

    let sigma = frame
        .p
        .dataframe(None)
        .col("quality")
        .reduce(ReduceOpKind::Stdev);

    let a = frame
        .p
        .dataframe(None)
        .col("quality")
        .binaryop(mu.clone(), BinaryOpKind::Sub)
        .binaryop(
            sigma.binaryop(frame.p.const_f64(2.0), BinaryOpKind::Mul),
            BinaryOpKind::Div,
        );

    let b = frame
        .p
        .dataframe(None)
        .col("quality")
        // .slice(0, 30)
        .rolling(6)
        .reduce(ReduceOpKind::Mean);

    a.concat(&[b]).alias("frame");

    mu.alias("mean");
    sigma.alias("stdev");

    frame
able        .p
        .dataframe(None)
        .order_by(frame.p.dataframe(None).col("quality"), false)
        .slice(0, 10)
        .alias("df_ordered");

    frame.realize();

    let hash = hash_of_dir("tmp").expect("Error getting hash.");
    dbg!(&hash);
    // assert_eq!(
    //     hash,
    //     "a6563cc841bfdec159c15003531c8019e457f1f9f3d3a384ba0f9b83c15fdec2"
    // );

    std::thread::sleep_ms(2000);
}
