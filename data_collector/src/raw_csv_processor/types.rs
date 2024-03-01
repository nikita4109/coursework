use serde::Serialize;
use web3::types::Address;

#[derive(Serialize, Clone, Default)]
pub struct TokenTick {
    pub block_number: u64,
    pub token_symbol: String,
    pub token_address: Address,
    pub price: f64,
    pub price_through_window: f64,
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

    pub volume_window: f64,
    pub buys_count_window: u64,
    pub sells_count_window: u64,
    pub buys_usd_window: f64,
    pub sells_usd_window: f64,
    pub high_price_window: f64,
    pub low_price_window: f64,
    pub std_price_change_window: f64,
    pub avg_price_change_window: f64,

    pub buys_count_day: u64,
    pub sells_count_day: u64,
    pub buys_usd_day: f64,
    pub sells_usd_day: f64,
    pub volume_day: f64,
}
