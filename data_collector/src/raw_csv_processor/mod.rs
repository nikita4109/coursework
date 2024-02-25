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
        let mut swaps = Vec::new();

        let mut rdr = Reader::from_path(&self.args.swaps_path).expect("can't read swaps csv");
        for result in rdr.deserialize() {
            swaps.push(result.unwrap());
        }

        let mut tokens = Tokens::new(self.args.blocks_window_len, self.args.candlestick_len);
        for swap in swaps {
            tokens.handle_swap(swap);
        }

        tokens.fill_through_window_blocks_price();
        tokens.fill_window();
        tokens.build_candlesticks();

        utils::write(
            &format!("{}/tokens.csv", self.args.output_dir),
            tokens.to_vec(),
        );
    }
}
