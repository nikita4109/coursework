use super::{normalize, types::SyncEvent, u256_to_f64};
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
    fn price1(&self) -> f64 {
        let reserve0 = normalize(self.reserve0, self.token0.decimals);
        let reserve1 = normalize(self.reserve1, self.token1.decimals);

        reserve1 / reserve0
    }

    fn price0(&self) -> f64 {
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

        self.update_price(token0, &pool);
        self.update_price(token1, &pool);
    }

    fn update_price(&mut self, token: &Token, pool: &Pool) {
        if !(self.decent_token_addresses.contains(&pool.token0.address)
            || self.decent_token_addresses.contains(&pool.token1.address))
        {
            return;
        }

        let best_pool = match self.token_to_biggest_pool.get(&token.address) {
            Some(biggest_pool) => {
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
                    pool.clone()
                } else {
                    biggest_pool.clone()
                }
            }
            None => pool.clone(),
        };

        self.token_to_biggest_pool
            .insert(token.address, best_pool.clone());

        let usd_price = if best_pool.token0.address == token.address {
            best_pool.price0() * self.token_usd_price(&best_pool.token1)
        } else {
            best_pool.price1() * self.token_usd_price(&best_pool.token0)
        };

        self.tokens_prices.insert(token.address, usd_price);
    }

    pub fn token_usd_price(&self, token: &Token) -> f64 {
        if self.usd_token_addresses.contains(&token.address) {
            return 1.0;
        }

        match self.tokens_prices.get(&token.address) {
            Some(r) => *r,
            None => 0.0,
        }
    }
}
