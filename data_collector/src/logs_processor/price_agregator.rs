use super::{normalize, types::SyncEvent};
use crate::logs_processor::types::Token;
use ethabi::token;
use serde::de;
use std::{
    collections::{HashMap, HashSet},
    ops::Add,
};
use web3::types::{Address, U256};

#[derive(Default, Clone)]
struct Pool {
    address: Address,
    token0: Token,
    token1: Token,
    reserve0: U256,
    reserve1: U256,
}

impl Pool {
    fn price0(&self) -> f64 {
        let reserve0 = normalize(self.reserve0, self.token0.decimals);
        let reserve1 = normalize(self.reserve1, self.token1.decimals);

        reserve1 / reserve0
    }

    fn price1(&self) -> f64 {
        let reserve0 = normalize(self.reserve0, self.token0.decimals);
        let reserve1 = normalize(self.reserve1, self.token1.decimals);

        reserve0 / reserve1
    }
}

pub struct PriceAgregator {
    usd_token_addresses: HashSet<Address>,
    pools: HashMap<Address, Pool>,
    token_to_biggest_pool: HashMap<Address, Pool>,
    tokens_prices: HashMap<Address, f64>,
    decent_token_addresses: HashSet<Address>,
}

impl PriceAgregator {
    pub fn new(usd_token_addresses: Vec<Address>, decent_token_addresses: Vec<Address>) -> Self {
        let mut usd_tokens_hashset = HashSet::new();
        for address in usd_token_addresses {
            usd_tokens_hashset.insert(address);
        }

        let mut decent_tokens_hashset = HashSet::new();
        for address in decent_token_addresses {
            decent_tokens_hashset.insert(address);
        }

        PriceAgregator {
            usd_token_addresses: usd_tokens_hashset,
            pools: HashMap::new(),
            token_to_biggest_pool: HashMap::new(),
            tokens_prices: HashMap::new(),
            decent_token_addresses: decent_tokens_hashset,
        }
    }

    pub fn handle_sync(&mut self, token0: &Token, token1: &Token, event: &SyncEvent) {
        let pool = Pool {
            address: event.address,
            token0: token0.clone(),
            token1: token1.clone(),
            reserve0: event.reserve0,
            reserve1: event.reserve1,
        };

        self.pools.insert(event.address, pool.clone());

        self.update_price(token0, Some(pool.clone()));
        self.update_price(token1, Some(pool.clone()));
    }

    fn update_price(&mut self, token: &Token, pool: Option<Pool>) {
        let best_pool = match self.find_best_pool(token, pool) {
            Some(r) => r,
            None => return,
        };

        self.token_to_biggest_pool
            .insert(token.address, best_pool.clone());

        let usd_price = if best_pool.token0.address == token.address {
            best_pool.price0() * self.price(&best_pool.token1)
        } else {
            best_pool.price1() * self.price(&best_pool.token0)
        };

        self.tokens_prices.insert(token.address, usd_price);

        if token.symbol == "WETH" {
            println!(
                "{} {:?} {:?} {:?} {:.5}",
                token.symbol, best_pool.address, best_pool.reserve0, best_pool.reserve1, usd_price
            );
        }
    }

    fn find_best_pool(&mut self, token: &Token, pool: Option<Pool>) -> Option<Pool> {
        match self.token_to_biggest_pool.get(&token.address) {
            Some(biggest_pool) => {
                let pool = match pool {
                    Some(r) => r,
                    None => return Some(biggest_pool.clone()),
                };

                if self.decent_token_addresses.contains(&token.address)
                    && self.decent_token_addresses.contains(&pool.token0.address)
                        != self.decent_token_addresses.contains(&pool.token1.address)
                {
                    return Some(biggest_pool.clone());
                }

                let reserve_biggest = if biggest_pool.token0.address == token.address {
                    biggest_pool.reserve0
                } else {
                    biggest_pool.reserve1
                };

                let reserve_current = if pool.token0.address == token.address {
                    pool.reserve0
                } else {
                    pool.reserve1
                };

                if reserve_biggest < reserve_current {
                    Some(pool.clone())
                } else {
                    Some(biggest_pool.clone())
                }
            }
            None => {
                let pool = match pool {
                    Some(r) => r,
                    None => return None,
                };

                if self.decent_token_addresses.contains(&token.address)
                    && self.decent_token_addresses.contains(&pool.token0.address)
                        != self.decent_token_addresses.contains(&pool.token1.address)
                {
                    return None;
                }

                Some(pool)
            }
        }
    }

    fn price(&mut self, token: &Token) -> f64 {
        if self.usd_token_addresses.contains(&token.address) {
            return 1.0;
        }

        match self.tokens_prices.get(&token.address) {
            Some(r) => *r,
            None => 0.0,
        }
    }

    pub fn token_usd_price(&mut self, token: &Token) -> f64 {
        if self.usd_token_addresses.contains(&token.address) {
            return 1.0;
        }

        self.update_price(token, None);

        match self.tokens_prices.get(&token.address) {
            Some(r) => *r,
            None => 0.0,
        }
    }
}
