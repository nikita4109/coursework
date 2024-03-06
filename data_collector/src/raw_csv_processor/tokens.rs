use super::types::Candlestick;
use super::types::TokenTick;
use crate::logs_processor::types::SwapTick;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
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
                    next_candle_close_price: 0.0,
                    volume: 0.0,
                    buys_count: 0,
                    sells_count: 0,
                    buys_usd: 0.0,
                    sells_usd: 0.0,
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

    pub fn build_candlesticks(&mut self) {
        for (_, ticks) in &self.agr_token_ticks {
            let mut bucket: Vec<TokenTick> = Vec::new();
            let start_idx = self.candlesticks.len();

            let blocks_in_hour = 300u64;

            let mut window_6h = Window::new(blocks_in_hour * 6);
            let mut window_1d = Window::new(blocks_in_hour * 24);
            let mut window_3d = Window::new(blocks_in_hour * 24 * 3);
            let mut big_window = BigWindow::new(blocks_in_hour * 24 * 7);

            for (block_number, tick) in ticks {
                if let Some(first_tick) = bucket.first() {
                    if block_number % self.candlestick_len
                        <= first_tick.block_number % self.candlestick_len
                        || block_number - first_tick.block_number >= self.candlestick_len
                    {
                        let mut candlestick = self.build_candlestick(bucket.clone());

                        window_6h.fill_6h(&mut candlestick);
                        window_6h.add(candlestick.clone());

                        window_1d.fill_1d(&mut candlestick);
                        window_1d.add(candlestick.clone());

                        window_3d.fill_3d(&mut candlestick);
                        window_3d.add(candlestick.clone());

                        big_window.fill(&mut candlestick);
                        big_window.add(candlestick.clone());

                        self.candlesticks.push(candlestick);
                        bucket.clear();
                    }
                }

                bucket.push(tick.clone());
            }

            if bucket.len() == 0 {
                let mut candlestick = self.build_candlestick(bucket.clone());
                window_6h.fill_6h(&mut candlestick);
                window_1d.fill_1d(&mut candlestick);
                window_3d.fill_3d(&mut candlestick);
                big_window.fill(&mut candlestick);

                self.candlesticks.push(candlestick);
            }

            for i in start_idx + 1..self.candlesticks.len() {
                self.candlesticks[i - 1].target_price = self.candlesticks[i].close_price;
            }
        }
    }

    fn build_candlestick(&self, bucket: Vec<TokenTick>) -> Candlestick {
        let mut candlestick = Candlestick {
            open_block_number: bucket[0].block_number
                - bucket[0].block_number % self.candlestick_len,
            close_block_number: bucket[0].block_number
                - bucket[0].block_number % self.candlestick_len
                + self.candlestick_len
                - 1,
            token_symbol: bucket[0].token_symbol.clone(),
            token_address: bucket[0].token_address,
            open_price: bucket[0].price,
            close_price: bucket.last().unwrap().price,
            ..Default::default()
        };

        candlestick.high_price = i32::MIN as f64;
        candlestick.low_price = i32::MAX as f64;

        for tick in bucket {
            candlestick.volume += tick.volume;
            candlestick.buys_count += tick.buys_count;
            candlestick.sells_count += tick.sells_count;
            candlestick.buys_usd += tick.buys_usd;
            candlestick.sells_usd += tick.sells_usd;

            candlestick.high_price = f64::max(candlestick.high_price, tick.price);
            candlestick.low_price = f64::min(candlestick.low_price, tick.price);
        }

        return candlestick;
    }

    pub fn to_vec(&self) -> Vec<Candlestick> {
        let mut candlesticks = self.candlesticks.clone();
        candlesticks.sort_by_key(|x| x.open_block_number);

        return candlesticks;
    }
}

struct BigWindow {
    blocks_in_window: u64,
    deque: VecDeque<Candlestick>,

    volume_window: f64,
    buys_count_window: u64,
    sells_count_window: u64,
    buys_usd_window: f64,
    sells_usd_window: f64,
}

impl BigWindow {
    fn new(blocks_in_window: u64) -> Self {
        Self {
            blocks_in_window: blocks_in_window,
            deque: VecDeque::new(),

            volume_window: 0.0,
            buys_count_window: 0,
            sells_count_window: 0,
            buys_usd_window: 0.0,
            sells_usd_window: 0.0,
        }
    }

    fn add(&mut self, candle: Candlestick) {
        while !self.deque.is_empty()
            && candle.open_block_number - self.deque[0].open_block_number > self.blocks_in_window
        {
            self.volume_window -= self.deque[0].volume;
            self.buys_count_window -= self.deque[0].buys_count;
            self.sells_count_window -= self.deque[0].sells_count;
            self.buys_usd_window -= self.deque[0].buys_usd;
            self.sells_usd_window -= self.deque[0].sells_usd;

            self.deque.pop_front();
        }

        self.volume_window += candle.volume;
        self.buys_count_window += candle.buys_count;
        self.sells_count_window += candle.sells_count;
        self.buys_usd_window += candle.buys_usd;
        self.sells_usd_window += candle.sells_usd;

        self.deque.push_back(candle.clone());
    }

    fn fill(&self, candle: &mut Candlestick) {
        candle.volume_week = self.volume_window;
        candle.buys_count_week = self.buys_count_window;
        candle.sells_count_week = self.sells_count_window;
        candle.buys_usd_week = self.buys_usd_window;
        candle.sells_usd_week = self.sells_usd_window;
    }
}

struct Window {
    blocks_in_window: u64,
    deque: VecDeque<Candlestick>,

    volume_window: f64,
    buys_count_window: u64,
    sells_count_window: u64,
    buys_usd_window: f64,
    sells_usd_window: f64,
    high_price_window: f64,
    low_price_window: f64,
}

impl Window {
    fn new(blocks_in_window: u64) -> Self {
        Self {
            blocks_in_window: blocks_in_window,
            deque: VecDeque::new(),

            volume_window: 0.0,
            buys_count_window: 0,
            sells_count_window: 0,
            buys_usd_window: 0.0,
            sells_usd_window: 0.0,
            high_price_window: 0.0,
            low_price_window: 0.0,
        }
    }

    fn add(&mut self, candle: Candlestick) {
        while !self.deque.is_empty()
            && candle.open_block_number - self.deque[0].open_block_number > self.blocks_in_window
        {
            self.volume_window -= self.deque[0].volume;
            self.buys_count_window -= self.deque[0].buys_count;
            self.sells_count_window -= self.deque[0].sells_count;
            self.buys_usd_window -= self.deque[0].buys_usd;
            self.sells_usd_window -= self.deque[0].sells_usd;

            self.deque.pop_front();
        }

        self.volume_window += candle.volume;
        self.buys_count_window += candle.buys_count;
        self.sells_count_window += candle.sells_count;
        self.buys_usd_window += candle.buys_usd;
        self.sells_usd_window += candle.sells_usd;

        self.deque.push_back(candle.clone());

        self.high_price_window = i32::MIN as f64;
        self.low_price_window = i32::MAX as f64;

        for candle in &self.deque {
            self.high_price_window = f64::max(self.high_price_window, candle.high_price);
            self.low_price_window = f64::min(self.low_price_window, candle.low_price);
        }
    }

    fn fill_6h(&self, candle: &mut Candlestick) {
        let mut price_changes = Vec::new();
        let mut avg_price = 0.0;
        for candle in &self.deque {
            price_changes.push(candle.close_price / candle.open_price);
            avg_price += candle.open_price;
        }

        if let Some(last) = self.deque.back() {
            avg_price += last.close_price;
            avg_price = avg_price / (self.deque.len() + 1) as f64;
        }

        candle.std_price_change_6h = match std_deviation(&price_changes) {
            Some(r) => r,
            None => 0.0,
        };
        candle.avg_price_change_6h = avg_price;

        candle.open_price_6h = match self.deque.front() {
            Some(r) => r.open_price,
            None => 0.0,
        };

        candle.volume_6h = self.volume_window;
        candle.buys_count_6h = self.buys_count_window;
        candle.sells_count_6h = self.sells_count_window;
        candle.buys_usd_6h = self.buys_usd_window;
        candle.sells_usd_6h = self.sells_usd_window;
        candle.high_price_6h = self.high_price_window;
        candle.low_price_6h = self.low_price_window;
    }

    fn fill_1d(&self, candle: &mut Candlestick) {
        let mut price_changes = Vec::new();
        let mut avg_price = 0.0;
        for candle in &self.deque {
            price_changes.push(candle.close_price / candle.open_price);
            avg_price += candle.open_price;
        }

        if let Some(last) = self.deque.back() {
            avg_price += last.close_price;
            avg_price = avg_price / (self.deque.len() + 1) as f64;
        }

        candle.std_price_change_1d = match std_deviation(&price_changes) {
            Some(r) => r,
            None => 0.0,
        };
        candle.avg_price_change_1d = avg_price;

        candle.open_price_1d = match self.deque.front() {
            Some(r) => r.open_price,
            None => 0.0,
        };

        candle.volume_1d = self.volume_window;
        candle.buys_count_1d = self.buys_count_window;
        candle.sells_count_1d = self.sells_count_window;
        candle.buys_usd_1d = self.buys_usd_window;
        candle.sells_usd_1d = self.sells_usd_window;
        candle.high_price_1d = self.high_price_window;
        candle.low_price_1d = self.low_price_window;
    }

    fn fill_3d(&self, candle: &mut Candlestick) {
        let mut price_changes = Vec::new();
        let mut avg_price = 0.0;
        for candle in &self.deque {
            price_changes.push(candle.close_price / candle.open_price);
            avg_price += candle.open_price;
        }

        if let Some(last) = self.deque.back() {
            avg_price += last.close_price;
            avg_price = avg_price / (self.deque.len() + 1) as f64;
        }

        candle.std_price_change_3d = match std_deviation(&price_changes) {
            Some(r) => r,
            None => 0.0,
        };
        candle.avg_price_change_3d = avg_price;

        candle.open_price_3d = match self.deque.front() {
            Some(r) => r.open_price,
            None => 0.0,
        };

        candle.volume_3d = self.volume_window;
        candle.buys_count_3d = self.buys_count_window;
        candle.sells_count_3d = self.sells_count_window;
        candle.buys_usd_3d = self.buys_usd_window;
        candle.sells_usd_3d = self.sells_usd_window;
        candle.high_price_3d = self.high_price_window;
        candle.low_price_3d = self.low_price_window;
    }
}

fn mean(data: &[f64]) -> Option<f64> {
    let sum = data.iter().sum::<f64>() as f64;
    let count = data.len();

    match count {
        positive if positive > 0 => Some(sum / count as f64),
        _ => None,
    }
}

fn std_deviation(data: &[f64]) -> Option<f64> {
    match (mean(data), data.len()) {
        (Some(data_mean), count) if count > 0 => {
            let variance = data
                .iter()
                .map(|value| {
                    let diff = data_mean - (*value as f64);

                    diff * diff
                })
                .sum::<f64>()
                / count as f64;

            Some(variance.sqrt())
        }
        _ => None,
    }
}
