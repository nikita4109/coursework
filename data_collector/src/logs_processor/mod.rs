mod price_agregator;

use crate::db::models::{
    parse_event, CEXData, CEXRecord, Event, LiquidityTick, PoolInfo, SwapTick, SyncTick, Token,
};
use crate::{utils, LogsProcessorArgs};
use diesel::prelude::*;
use diesel::PgConnection;
use std::collections::HashMap;
use std::io::{BufReader, Read};
use std::path::Path;
use std::{fs::File, io::BufRead};
use web3::contract::Contract;
use web3::contract::Options;
use web3::transports::Http;
use web3::types::Address;
use web3::types::U256;
use web3::Web3;

pub struct LogsProcessor {
    rpc: String,
    cex_data: Vec<CEXData>,
    pools: HashMap<Address, PoolInfo>,
    logs_path: String,
}

impl LogsProcessor {
    pub fn new(conn: &PgConnection, args: LogsProcessorArgs) -> Self {
        LogsProcessor {
            rpc: args.rpc,
            cex_data: LogsProcessor::read_cex_data_db(conn),
            pools: LogsProcessor::read_pools_db(conn),
            logs_path: args.logs_path,
        }
    }

    fn read_cex_data_db(conn: &PgConnection) -> Vec<CEXData> {
        use crate::db::schema::cex_data::dsl::*;
        cex_data
            .filter(platform_slug.eq("ethereum"))
            .load::<CEXData>(conn)
            .expect("Error loading CEX data from database")
    }

    fn read_pools_db(conn: &PgConnection) -> HashMap<Address, PoolInfo> {
        use crate::db::schema::pools::dsl::*;
        let pool_infos = pools
            .load::<PoolInfo>(conn)
            .expect("Error loading pools from database");

        pool_infos
            .into_iter()
            .map(|pool| {
                let addr: Address = pool.address.parse().expect("Invalid pool address");
                (addr, pool)
            })
            .collect()
    }

    pub async fn save_to_db(&self, conn: &PgConnection) {
        let mut token_address_to_token = HashMap::new();
        for cex_record in &self.cex_data {
            if token_address_to_token.contains_key(&cex_record.token_address) {
                continue;
            }

            if let Some(decimals) = self
                .get_decimals(cex_record.token_address.parse().unwrap())
                .await
            {
                token_address_to_token.insert(
                    cex_record.token_address.clone(),
                    Token {
                        symbol: cex_record.symbol.clone(),
                        address: cex_record.token_address.parse().unwrap(),
                        decimals: decimals,
                    },
                );
            }
        }

        println!("[CEX data handled]");

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

        let file = File::open(Path::new(&self.logs_path)).expect("invalid logs csv path");
        let reader = BufReader::new(file);

        for line in reader.lines() {
            match line {
                Ok(content) => {
                    let args = content.split(',').map(|s| s.to_string()).collect();
                    if let Some(event) = parse_event(args) {
                        match event {
                            Event::Sync(event) => {
                                if let Some((token0, token1)) =
                                    pool_address_to_tokens.get(&event.address)
                                {
                                    price_agregator.handle_sync(token0, token1, &event);

                                    let record = SyncTick {
                                        token0_symbol: token0.symbol.clone(),
                                        token1_symbol: token1.symbol.clone(),
                                        token0_address: format!("{:?}", token0.address),
                                        token1_address: format!("{:?}", token1.address),
                                        block_number: event.block_number as i64,
                                        address: format!("{:?}", event.address),
                                        reserve0: normalize(event.reserve0, token0.decimals),
                                        reserve1: normalize(event.reserve1, token1.decimals),
                                        token0_usd_price: price_agregator.token_usd_price(token0),
                                        token1_usd_price: price_agregator.token_usd_price(token1),
                                    };

                                    diesel::insert_into(crate::db::schema::sync_ticks::table)
                                        .values(&record)
                                        .execute(conn)
                                        .expect("Error saving sync log to database");
                                }
                            }

                            Event::Swap(event) => {
                                if let Some((token0, token1)) =
                                    pool_address_to_tokens.get(&event.address)
                                {
                                    let record = SwapTick {
                                        token0_symbol: token0.symbol.clone(),
                                        token1_symbol: token1.symbol.clone(),
                                        token0_address: format!("{:?}", token0.address),
                                        token1_address: format!("{:?}", token1.address),
                                        block_number: event.block_number as i64,
                                        address: format!("{:?}", event.address),
                                        sender: format!("{:?}", event.sender),
                                        amount0_in: normalize(event.amount0_in, token0.decimals),
                                        amount0_out: normalize(event.amount0_out, token0.decimals),
                                        amount1_in: normalize(event.amount1_in, token1.decimals),
                                        amount1_out: normalize(event.amount1_out, token1.decimals),
                                        token0_usd_price: price_agregator.token_usd_price(token0),
                                        token1_usd_price: price_agregator.token_usd_price(token1),
                                    };

                                    diesel::insert_into(crate::db::schema::swap_ticks::table)
                                        .values(&record)
                                        .execute(conn)
                                        .expect("Error saving swap log to database");
                                }
                            }

                            Event::Mint(event) => {
                                if let Some((token0, token1)) =
                                    pool_address_to_tokens.get(&event.address)
                                {
                                    let record = LiquidityTick {
                                        token0_symbol: token0.symbol.clone(),
                                        token1_symbol: token1.symbol.clone(),
                                        token0_address: format!("{:?}", token0.address),
                                        token1_address: format!("{:?}", token1.address),
                                        block_number: event.block_number as i64,
                                        address: format!("{:?}", event.address),
                                        sender: format!("{:?}", event.sender),
                                        amount0: normalize(event.amount0, token0.decimals),
                                        amount1: normalize(event.amount1, token1.decimals),
                                        token0_usd_price: price_agregator.token_usd_price(token0),
                                        token1_usd_price: price_agregator.token_usd_price(token1),
                                    };

                                    diesel::insert_into(crate::db::schema::liquidity_ticks::table)
                                        .values(&record)
                                        .execute(conn)
                                        .expect("Error saving mint log to database");
                                }
                            }

                            Event::Burn(event) => {
                                if let Some((token0, token1)) =
                                    pool_address_to_tokens.get(&event.address)
                                {
                                    let record = LiquidityTick {
                                        token0_symbol: token0.symbol.clone(),
                                        token1_symbol: token1.symbol.clone(),
                                        token0_address: format!("{:?}", token0.address),
                                        token1_address: format!("{:?}", token1.address),
                                        block_number: event.block_number as i64,
                                        address: format!("{:?}", event.address),
                                        sender: format!("{:?}", event.sender),
                                        amount0: -normalize(event.amount0, token0.decimals),
                                        amount1: -normalize(event.amount1, token1.decimals),
                                        token0_usd_price: price_agregator.token_usd_price(token0),
                                        token1_usd_price: price_agregator.token_usd_price(token1),
                                    };

                                    diesel::insert_into(crate::db::schema::liquidity_ticks::table)
                                        .values(&record)
                                        .execute(conn)
                                        .expect("Error saving burn log to database");
                                }
                            }
                        };
                    }
                }
                Err(e) => {
                    panic!("Error reading line: {}", e);
                }
            }
        }

        println!("[Events handled]");
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
            Err(_) => {
                println!("can't get decimals for {:?}", token_address);
                return None;
            }
        };

        Some(decimals.as_u64())
    }
}

pub fn normalize(amount: U256, decimals: u64) -> f64 {
    u256_to_f64(amount) / 10.0_f64.powf(decimals as f64)
}

pub fn u256_to_f64(a: U256) -> f64 {
    a.to_string().parse().unwrap()
}
