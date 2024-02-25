mod tokens;
mod types;

use csv::Reader;
use tokens::Tokens;

use crate::utils;

pub struct RawCSVProcessor {}

impl RawCSVProcessor {
    pub fn new() -> Self {
        RawCSVProcessor {}
    }

    pub fn write_tokens_csv(&self, swaps_path: &str, output_dir: &str) {
        let mut swaps = Vec::new();

        let mut rdr = Reader::from_path(swaps_path).expect("can't read swaps csv");
        for result in rdr.deserialize() {
            swaps.push(result.unwrap());
        }

        let mut tokens = Tokens::new(300, 5);
        for swap in swaps {
            tokens.handle_swap(swap);
        }

        tokens.fill_through_window_blocks_price();
        tokens.fill_window();
        tokens.build_candlesticks();

        utils::write(&format!("{}/tokens.csv", output_dir), tokens.to_vec());
    }
}
