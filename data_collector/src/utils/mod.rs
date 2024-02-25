use csv::Writer;
use serde::Serialize;
use std::fs::File;

pub fn write<T>(path: &str, records: Vec<T>)
where
    T: Serialize,
{
    let file = File::create(path).unwrap();
    let mut wtr = Writer::from_writer(file);

    for record in records {
        wtr.serialize(record).unwrap();
    }

    wtr.flush().unwrap();
}
