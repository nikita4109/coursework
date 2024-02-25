use super::types::Candlestick;
use super::types::TokenTick;
use crate::logs_processor::types::SwapTick;
use std::collections::BTreeMap;
use std::collections::HashMap;
use ta::{indicators::ExponentialMovingAverage, Next};
use web3::types::Address;

pub struct Tokens {
    blocks_window_len: u64,
    candlestick_len: u64,
    agr_token_ticks: HashMap<Address, BTreeMap<u64, TokenTick>>,
    candlesticks: Vec<Candlestick>,
}

impl Tokens {
    pub fn new(blocks_window_len: u64, candlestick_len: u64) -> Self {
        Self {
            blocks_window_len: blocks_window_len,
            candlestick_len: candlestick_len,
            agr_token_ticks: HashMap::new(),
            candlesticks: Vec::new(),
        }
    }

    pub fn handle_swap(&mut self, swap: SwapTick) {
        let volume =
            swap.token0_usd_price * swap.amount0_in + swap.token1_usd_price * swap.amount1_in;

        self.update(
            swap.block_number,
            &swap.token0_symbol,
            swap.token0_address,
            swap.token0_usd_price,
            swap.amount0_in,
            swap.amount0_out,
            volume,
        );

        self.update(
            swap.block_number,
            &swap.token1_symbol,
            swap.token1_address,
            swap.token1_usd_price,
            swap.amount1_in,
            swap.amount1_out,
            volume,
        );
    }

    fn update(
        &mut self,
        block_number: u64,
        token_symbol: &str,
        token_address: Address,
        price: f64,
        amount_in: f64,
        amount_out: f64,
        volume: f64,
    ) {
        if !self.agr_token_ticks.contains_key(&token_address) {
            self.agr_token_ticks.insert(token_address, BTreeMap::new());
        }

        let agr_blocks = self.agr_token_ticks.get_mut(&token_address).unwrap();
        if !agr_blocks.contains_key(&block_number) {
            agr_blocks.insert(
                block_number,
                TokenTick {
                    block_number: block_number,
                    token_symbol: token_symbol.to_owned(),
                    token_address: token_address,
                    price: price,
                    price_through_window: 0.0,
                    volume: 0.0,
                    buys_count: 0,
                    sells_count: 0,
                    buys_usd: 0.0,
                    sells_usd: 0.0,
                    volume_window: 0.0,
                    buys_count_window: 0,
                    sells_count_window: 0,
                    buys_usd_window: 0.0,
                    sells_usd_window: 0.0,
                    high_price: 0.0,
                    low_price: 0.0,
                    macd: 0.0,
                    signal_line: 0.0,
                },
            );
        }

        let token_tick = agr_blocks.get_mut(&block_number).unwrap();
        token_tick.price = price;
        token_tick.volume += volume;

        if amount_in != 0.0 {
            token_tick.buys_count += 1;
        }

        if amount_out != 0.0 {
            token_tick.sells_count += 1;
        }

        token_tick.buys_usd += amount_in * price;
        token_tick.sells_usd += amount_out * price;
    }

    pub fn fill_through_window_blocks_price(&mut self) {
        for (_, ticks) in self.agr_token_ticks.iter_mut() {
            let mut prices = BTreeMap::new();
            for (block_number, tick) in ticks.iter_mut().rev() {
                prices.insert(block_number, tick.price);
                while let Some((last_block_number, price)) = prices.last_key_value() {
                    if block_number + self.blocks_window_len >= **last_block_number {
                        tick.price_through_window = *price;
                        break;
                    }

                    prices.pop_last();
                }
            }
        }
    }

    pub fn fill_window(&mut self) {
        for (_, ticks) in self.agr_token_ticks.iter_mut() {
            let mut volume_window = 0.0;
            let mut buys_count_window = 0;
            let mut sells_count_window = 0;
            let mut buys_usd_window = 0.0;
            let mut sells_usd_window = 0.0;

            let mut window = BTreeMap::new();

            for (block_number, tick) in ticks.iter_mut() {
                window.insert(block_number, tick.clone());

                volume_window += tick.volume;
                buys_count_window += tick.buys_count;
                sells_count_window += tick.sells_count;
                buys_usd_window += tick.buys_usd;
                sells_usd_window += tick.sells_usd;

                while let Some((first_block_number, first_tick)) = window.first_key_value() {
                    if block_number - **first_block_number <= self.blocks_window_len {
                        break;
                    }

                    volume_window -= first_tick.volume;
                    buys_count_window -= first_tick.buys_count;
                    sells_count_window -= first_tick.sells_count;
                    buys_usd_window -= first_tick.buys_usd;
                    sells_usd_window -= first_tick.sells_usd;

                    window.pop_first();
                }

                tick.volume_window = volume_window;
                tick.buys_count_window = buys_count_window;
                tick.sells_count_window = sells_count_window;
                tick.buys_usd_window = buys_usd_window;
                tick.sells_usd_window = sells_usd_window;

                let mut prices = Vec::new();
                let mut min_price: f64 = i32::MAX as f64;
                let mut max_price: f64 = i32::MIN as f64;
                for (_, tick) in &window {
                    min_price = f64::min(min_price, tick.price);
                    max_price = f64::max(max_price, tick.price);

                    prices.push(tick.price);
                }

                tick.low_price = min_price;
                tick.high_price = max_price;
                (tick.macd, tick.signal_line) = macd(prices);
            }
        }
    }

    pub fn build_candlesticks(&mut self) {
        for (_, ticks) in &self.agr_token_ticks {
            let mut window = Vec::new();

            for (block_number, tick) in ticks {
                window.push(tick.clone());
                if let Some(first_tick) = window.first() {
                    if block_number % self.candlestick_len
                        <= first_tick.block_number % self.candlestick_len
                        || block_number - first_tick.block_number >= self.candlestick_len
                    {
                        let candlestick = self.build_candlestick(window.clone());
                        self.candlesticks.push(candlestick);
                        window.clear();
                        continue;
                    }
                }
            }

            if window.len() > 0 {
                let candlestick = self.build_candlestick(window.clone());
                self.candlesticks.push(candlestick);
            }
        }
    }

    fn build_candlestick(&self, window: Vec<TokenTick>) -> Candlestick {
        let mut candlestick = Candlestick {
            open_block_number: window[0].block_number,
            close_block_number: window.last().unwrap().block_number,
            token_symbol: window[0].token_symbol.clone(),
            token_address: window[0].token_address,
            open_price: window[0].price,
            close_price: window.last().unwrap().price,
            price_through_window: window[0].price_through_window,
            volume_window: window[0].volume_window,
            buys_count_window: window[0].buys_count_window,
            sells_count_window: window[0].sells_count_window,
            buys_usd_window: window[0].buys_usd_window,
            sells_usd_window: window[0].sells_usd_window,
            high_price: window[0].high_price,
            low_price: window[0].low_price,
            macd: window[0].macd,
            signal_line: window[0].signal_line,
            ..Default::default()
        };

        for tick in window {
            candlestick.volume += tick.volume;
            candlestick.buys_count += tick.buys_count;
            candlestick.sells_count += tick.sells_count;
            candlestick.buys_usd += tick.buys_usd;
            candlestick.sells_usd += tick.sells_usd;
        }

        return candlestick;
    }

    pub fn to_vec(&self) -> Vec<Candlestick> {
        let mut candlesticks = self.candlesticks.clone();
        candlesticks.sort_by_key(|x| x.open_block_number);

        return candlesticks;
    }
}

fn macd(prices: Vec<f64>) -> (f64, f64) {
    let mut ema12 = ExponentialMovingAverage::new(12).unwrap();
    let mut ema26 = ExponentialMovingAverage::new(26).unwrap();

    let ema12_values: Vec<f64> = prices.iter().map(|&price| ema12.next(price)).collect();
    let ema26_values: Vec<f64> = prices.iter().map(|&price| ema26.next(price)).collect();

    let macd_values: Vec<f64> = ema12_values
        .iter()
        .zip(ema26_values.iter())
        .map(|(&ema12, &ema26)| ema12 - ema26)
        .collect();

    let mut signal_line_ema = ExponentialMovingAverage::new(9).unwrap();
    let signal_line_values: Vec<f64> = macd_values
        .iter()
        .map(|&macd| signal_line_ema.next(macd))
        .collect();

    let todays_macd = *macd_values.last().unwrap();
    let todays_signal_line = *signal_line_values.last().unwrap();

    return (todays_macd, todays_signal_line);
}
