mod price_agregator;
mod types;

use self::types::{CEXData, SyncTick, Token};
use self::types::{LiquidityTick, SwapTick};
use crate::logs_processor::types::{CEXRecord, TokenTick};
use crate::pools_collector::PoolInfo;
use crate::LogsProcessorArgs;
use csv::Reader;
use csv::Writer;
use ethabi::token;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{BufReader, Read};
use std::ops::Bound::{Excluded, Unbounded};
use std::path::Path;
use std::{fs::File, io::BufRead};
use ta::{indicators::ExponentialMovingAverage, Next};
use types::{parse_event, Event};
use web3::contract::Contract;
use web3::contract::Options;
use web3::transports::Http;
use web3::types::Address;
use web3::types::U256;
use web3::Web3;

pub struct LogsProcessor {
    rpc: String,
    events: Vec<Event>,
    cex_data: Vec<CEXData>,
    pools: HashMap<Address, PoolInfo>,
}

impl LogsProcessor {
    pub fn new(args: LogsProcessorArgs) -> Self {
        LogsProcessor {
            rpc: args.rpc,
            events: LogsProcessor::read_logs_csv(&args.logs_path),
            cex_data: LogsProcessor::read_cex_data_csv(&args.cex_data_path),
            pools: LogsProcessor::read_pools(&args.pools_path),
        }
    }

    fn read_logs_csv(path: &str) -> Vec<Event> {
        let mut events = Vec::new();

        let file = File::open(Path::new(path)).expect("invalid logs csv path");

        let reader = BufReader::new(file);

        for line in reader.lines() {
            match line {
                Ok(content) => {
                    let args = content.split(',').map(|s| s.to_string()).collect();
                    if let Some(event) = parse_event(args) {
                        events.push(event);
                    }
                }
                Err(e) => {
                    panic!("Error reading line: {}", e);
                }
            }
        }

        return events;
    }

    fn read_cex_data_csv(path: &str) -> Vec<CEXData> {
        let mut data = Vec::new();

        let mut rdr = Reader::from_path(path).expect("can't read CEX csv");
        for result in rdr.deserialize() {
            let record: CEXRecord = result.unwrap();
            if record.platform_slug != "ethereum" {
                continue;
            }

            if let Ok(token_address) = record.token_adress.parse() {
                data.push(CEXData {
                    address: token_address,
                    token_symbol: record.symbol,
                });
            }
        }

        return data;
    }

    fn read_pools(path: &str) -> HashMap<Address, PoolInfo> {
        let mut file = File::open(path).expect("invalid path");
        let mut content = String::new();
        file.read_to_string(&mut content)
            .expect("can't read file with pools");

        let vec: Vec<PoolInfo> = serde_json::from_str(&content).expect("invalid pools json");
        let mut pools = HashMap::new();

        for pool in vec {
            pools.insert(pool.address, pool);
        }

        return pools;
    }

    pub async fn write_csv(&self, dir: &str) {
        let mut token_address_to_token = HashMap::new();
        for cex_record in &self.cex_data {
            if token_address_to_token.contains_key(&cex_record.address) {
                continue;
            }

            if let Some(decimals) = self.get_decimals(cex_record.address).await {
                token_address_to_token.insert(
                    cex_record.address,
                    Token {
                        symbol: cex_record.token_symbol.clone(),
                        address: cex_record.address,
                        decimals: decimals,
                    },
                );
            }
        }

        println!("[CEX csv handled]");

        let mut pool_address_to_tokens = HashMap::new();
        for (address, pool_info) in &self.pools {
            let token0 = match token_address_to_token.get(&pool_info.token0) {
                Some(token) => token,
                None => continue,
            };

            let token1 = match token_address_to_token.get(&pool_info.token1) {
                Some(token) => token,
                None => continue,
            };

            pool_address_to_tokens.insert(address, (token0, token1));
        }

        let usd_token_addresses = vec![
            "0xdac17f958d2ee523a2206206994597c13d831ec7"
                .parse()
                .unwrap(),
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
                .parse()
                .unwrap(),
            "0x6b175474e89094c44da98b954eedeac495271d0f"
                .parse()
                .unwrap(),
        ];

        let decent_tokens = vec![
            "0xdac17f958d2ee523a2206206994597c13d831ec7"
                .parse()
                .unwrap(),
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
                .parse()
                .unwrap(),
            "0x6b175474e89094c44da98b954eedeac495271d0f"
                .parse()
                .unwrap(),
            "0xB8c77482e45F1F44dE1745F52C74426C631bDD52"
                .parse()
                .unwrap(),
            "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
                .parse()
                .unwrap(),
            "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
                .parse()
                .unwrap(),
        ];

        let mut price_agregator =
            price_agregator::PriceAgregator::new(usd_token_addresses, decent_tokens);

        let mut reserves = Vec::new();
        let mut swaps = Vec::new();
        let mut liquidity_providing = Vec::new();
        for event in &self.events {
            match event {
                Event::Sync(event) => {
                    if let Some((token0, token1)) = pool_address_to_tokens.get(&event.address) {
                        price_agregator.handle_sync(token0, token1, event);

                        reserves.push(SyncTick {
                            token0_symbol: token0.symbol.clone(),
                            token1_symbol: token1.symbol.clone(),
                            token0_address: token0.address,
                            token1_address: token1.address,
                            block_number: event.block_number,
                            address: event.address,
                            reserve0: normalize(event.reserve0, token0.decimals),
                            reserve1: normalize(event.reserve1, token1.decimals),
                            token0_usd_price: price_agregator.token_usd_price(token0),
                            token1_usd_price: price_agregator.token_usd_price(token1),
                        });
                    }
                }

                Event::Swap(event) => {
                    if let Some((token0, token1)) = pool_address_to_tokens.get(&event.address) {
                        swaps.push(SwapTick {
                            token0_symbol: token0.symbol.clone(),
                            token1_symbol: token1.symbol.clone(),
                            token0_address: token0.address,
                            token1_address: token1.address,
                            block_number: event.block_number,
                            address: event.address,
                            sender: event.sender,
                            amount0_in: normalize(event.amount0_in, token0.decimals),
                            amount0_out: normalize(event.amount0_out, token0.decimals),
                            amount1_in: normalize(event.amount1_in, token1.decimals),
                            amount1_out: normalize(event.amount1_out, token1.decimals),
                            token0_usd_price: price_agregator.token_usd_price(token0),
                            token1_usd_price: price_agregator.token_usd_price(token1),
                        });
                    }
                }

                Event::Mint(event) => {
                    if let Some((token0, token1)) = pool_address_to_tokens.get(&event.address) {
                        liquidity_providing.push(LiquidityTick {
                            token0_symbol: token0.symbol.clone(),
                            token1_symbol: token1.symbol.clone(),
                            token0_address: token0.address,
                            token1_address: token1.address,
                            block_number: event.block_number,
                            address: event.address,
                            sender: event.sender,
                            amount0: normalize(event.amount0, token0.decimals),
                            amount1: normalize(event.amount1, token1.decimals),
                            token0_usd_price: price_agregator.token_usd_price(token0),
                            token1_usd_price: price_agregator.token_usd_price(token1),
                        });
                    }
                }

                Event::Burn(event) => {
                    if let Some((token0, token1)) = pool_address_to_tokens.get(&event.address) {
                        liquidity_providing.push(LiquidityTick {
                            token0_symbol: token0.symbol.clone(),
                            token1_symbol: token1.symbol.clone(),
                            token0_address: token0.address,
                            token1_address: token1.address,
                            block_number: event.block_number,
                            address: event.address,
                            sender: event.sender,
                            amount0: -normalize(event.amount0, token0.decimals),
                            amount1: -normalize(event.amount1, token1.decimals),
                            token0_usd_price: price_agregator.token_usd_price(token0),
                            token1_usd_price: price_agregator.token_usd_price(token1),
                        });
                    }
                }
            };
        }

        println!("[Events handled]");

        let mut tokens = Tokens::new();
        for swap in swaps {
            tokens.handle_swap(swap);
        }

        tokens.fill_through_window_blocks_price(300);
        tokens.fill_window(300);

        Self::write(&format!("{}/tokens.csv", dir), tokens.to_vec());
    }

    fn write<T>(path: &str, records: Vec<T>)
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

    async fn get_decimals(&self, token_address: Address) -> Option<u64> {
        let http = Http::new(&self.rpc).expect("Can't connect to RPC");
        let web3 = Web3::new(http);

        let abi = include_bytes!("../../abi/erc20.abi");
        let contract = Contract::from_json(web3.eth(), token_address, abi)
            .expect("Failed to create contract from ABI");

        let decimals: U256 = match contract
            .query("decimals", (), None, Options::default(), None)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                println!("can't get decimals for {:?}", token_address);
                return None;
            }
        };

        return Some(decimals.as_u64());
    }
}

pub fn normalize(amount: U256, decimals: u64) -> f64 {
    u256_to_f64(amount) / 10.0_f64.powf(decimals as f64)
}

pub fn u256_to_f64(a: U256) -> f64 {
    a.to_string().parse().unwrap()
}

struct Tokens {
    agr_token_ticks: HashMap<Address, BTreeMap<u64, TokenTick>>,
}

impl Tokens {
    fn new() -> Self {
        Self {
            agr_token_ticks: HashMap::new(),
        }
    }

    fn handle_swap(&mut self, swap: SwapTick) {
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
                    price_through_100_blocks: 0.0,
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

    fn fill_through_window_blocks_price(&mut self, blocks_window_len: u64) {
        for (_, ticks) in self.agr_token_ticks.iter_mut() {
            let mut prices = BTreeMap::new();
            for (block_number, tick) in ticks.iter_mut().rev() {
                prices.insert(block_number, tick.price);
                while let Some((last_block_number, price)) = prices.last_key_value() {
                    if block_number + blocks_window_len >= **last_block_number {
                        tick.price_through_100_blocks = *price;
                        break;
                    }

                    prices.pop_last();
                }
            }
        }
    }

    fn fill_window(&mut self, blocks_window_len: u64) {
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
                    if block_number - **first_block_number <= blocks_window_len {
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

    fn to_vec(&self) -> Vec<TokenTick> {
        let mut vticks = Vec::new();
        for (_, ticks) in &self.agr_token_ticks {
            for (_, tick) in ticks {
                vticks.push(tick.clone());
            }
        }

        vticks.sort_by_key(|x| x.block_number);

        return vticks;
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
