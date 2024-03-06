use serde::Serialize;
use web3::types::Address;

#[derive(Serialize, Clone, Default)]
pub struct TokenTick {
    pub block_number: u64,
    pub token_symbol: String,
    pub token_address: Address,
    pub price: f64,
    pub next_candle_close_price: f64,
    pub volume: f64,
    pub buys_count: u64,
    pub sells_count: u64,
    pub buys_usd: f64,
    pub sells_usd: f64,
}

#[derive(Serialize, Clone, Default)]
pub struct Candlestick {
    pub open_block_number: u64,
    pub close_block_number: u64,
    pub token_symbol: String,
    pub token_address: Address,

    pub open_price: f64,
    pub close_price: f64,
    pub high_price: f64,
    pub low_price: f64,
    pub target_price: f64,
    pub volume: f64,
    pub buys_count: u64,
    pub sells_count: u64,
    pub buys_usd: f64,
    pub sells_usd: f64,

    pub volume_6h: f64,
    pub buys_count_6h: u64,
    pub sells_count_6h: u64,
    pub buys_usd_6h: f64,
    pub sells_usd_6h: f64,
    pub high_price_6h: f64,
    pub low_price_6h: f64,
    pub std_price_change_6h: f64,
    pub avg_price_change_6h: f64,

    pub volume_1d: f64,
    pub buys_count_1d: u64,
    pub sells_count_1d: u64,
    pub buys_usd_1d: f64,
    pub sells_usd_1d: f64,
    pub high_price_1d: f64,
    pub low_price_1d: f64,
    pub std_price_change_1d: f64,
    pub avg_price_change_1d: f64,

    pub volume_3d: f64,
    pub buys_count_3d: u64,
    pub sells_count_3d: u64,
    pub buys_usd_3d: f64,
    pub sells_usd_3d: f64,
    pub high_price_3d: f64,
    pub low_price_3d: f64,
    pub std_price_change_3d: f64,
    pub avg_price_change_3d: f64,

    pub buys_count_week: u64,
    pub sells_count_week: u64,
    pub buys_usd_week: f64,
    pub sells_usd_week: f64,
    pub volume_week: f64,
}
