mod price_agregator;
pub mod types;

use self::types::{CEXData, SyncTick, Token};
use self::types::{LiquidityTick, SwapTick};
use crate::logs_processor::types::CEXRecord;
use crate::pools_collector::PoolInfo;
use crate::{utils, LogsProcessorArgs};
use csv::Reader;
use csv::Writer;
use std::collections::HashMap;
use std::io::{BufReader, Read, Write};
use std::path::Path;
use std::{fs::File, io::BufRead};
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

    pub async fn write_raw_csvs(&self, dir: &str) {
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

        let liquidity_providing_file =
            File::create(&format!("{}/liquidity_providing.csv", dir)).unwrap();
        let mut liquidity_providing_wtr = Writer::from_writer(liquidity_providing_file);

        let reserves_file = File::create(&format!("{}/reserves.csv", dir)).unwrap();
        let mut reserves_wtr = Writer::from_writer(reserves_file);

        let swaps_file = File::create(&format!("{}/swaps.csv", dir)).unwrap();
        let mut swaps_wtr = Writer::from_writer(swaps_file);

        for event in &self.events {
            match event {
                Event::Sync(event) => {
                    if let Some((token0, token1)) = pool_address_to_tokens.get(&event.address) {
                        price_agregator.handle_sync(token0, token1, event);

                        reserves_wtr
                            .serialize(SyncTick {
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
                            })
                            .unwrap();
                    }
                }

                Event::Swap(event) => {
                    if let Some((token0, token1)) = pool_address_to_tokens.get(&event.address) {
                        swaps_wtr
                            .serialize(SwapTick {
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
                            })
                            .unwrap();
                    }
                }

                Event::Mint(event) => {
                    if let Some((token0, token1)) = pool_address_to_tokens.get(&event.address) {
                        liquidity_providing_wtr
                            .serialize(LiquidityTick {
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
                            })
                            .unwrap();
                    }
                }

                Event::Burn(event) => {
                    if let Some((token0, token1)) = pool_address_to_tokens.get(&event.address) {
                        liquidity_providing_wtr
                            .serialize(LiquidityTick {
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
                            })
                            .unwrap();
                    }
                }
            };
        }

        println!("[Events handled]");

        liquidity_providing_wtr.flush().unwrap();
        reserves_wtr.flush().unwrap();
        swaps_wtr.flush().unwrap();
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
