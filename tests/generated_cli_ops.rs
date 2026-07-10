use piano::{BodyRecord, NdjsonLine};

#[test]
fn generated_reader_parses_entire_golden_fixture() {
    let mut lines = include_str!("../piano-runtime/tests/data/golden_run.ndjson").lines();
    let header = lines.next().expect("golden fixture has a header");

    let parsed_header = piano::parse_header(NdjsonLine::new(header.to_string()))
        .expect("generated header parser accepts runtime header");
    assert_eq!(parsed_header.value(), header);

    let mut aggregates = 0usize;
    let mut trailers = 0usize;
    let mut names = Vec::new();
    let mut interrupted = false;

    for line in lines {
        let record = piano::classify_body_line(&NdjsonLine::new(line.to_string()));
        match record {
            BodyRecord::Aggregate { agg } => {
                aggregates += 1;
                names.push(agg.name_id());
                interrupted |= agg.interrupted();
            }
            BodyRecord::Trailer { table } => {
                trailers += 1;
                assert_eq!(table.kind(), "trailer");
            }
            BodyRecord::Measurement { .. } => panic!("golden fixture is aggregate format"),
            BodyRecord::Unrecognized => panic!("golden fixture line was not classified: {line}"),
        }
    }

    assert_eq!(aggregates, 3);
    assert_eq!(trailers, 1);
    assert_eq!(names, vec![0, 1, 1]);
    assert!(interrupted);
}
