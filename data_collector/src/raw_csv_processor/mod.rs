mod tokens;
mod types;

use csv::Reader;
use tokens::Tokens;

use crate::{utils, RawCSVsProcessorArgs};

pub struct RawCSVProcessor {
    args: RawCSVsProcessorArgs,
}

impl RawCSVProcessor {
    pub fn new(args: RawCSVsProcessorArgs) -> Self {
        RawCSVProcessor { args: args }
    }

    pub fn write_tokens_csv(&self) {
        let mut rdr = Reader::from_path(&self.args.swaps_path).expect("can't read swaps csv");

        let mut tokens = Tokens::new(self.args.candlestick_len);
        for result in rdr.deserialize() {
            tokens.handle_swap(result.unwrap());
        }

        println!("[SWAPS HANDLED]");

        tokens.build_candlesticks();

        utils::write(
            &format!("{}/tokens.csv", self.args.output_dir),
            tokens.to_vec(),
        );
    }
}
