use samplesheet::reader::read_samplesheet;

use std::env;

fn main() {
    let args: Vec<_> = env::args().collect();

    let ssheet = read_samplesheet(args.get(1).unwrap()).unwrap();

    dbg!(&ssheet.header);

    dbg!(&ssheet.settings);

    dbg!(&ssheet.reads);

    dbg!(&ssheet.data);

    dbg!(&ssheet.other);
}
