use super::{types::SyncEvent, u256_to_f64};
use crate::logs_processor::types::Token;
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
        let reserve0 = u256_to_f64(self.reserve0);
        let reserve1 = u256_to_f64(self.reserve1);

        reserve0 / reserve1
    }

    fn price1(&self) -> f64 {
        let reserve0 = u256_to_f64(self.reserve0);
        let reserve1 = u256_to_f64(self.reserve1);

        reserve1 / reserve0
    }
}

pub struct PriceAgregator {
    usd_token_addresses: HashSet<Address>,
    pools: HashMap<Address, Pool>,
    token_to_biggest_pool: HashMap<Address, Pool>,
    tokens_prices: HashMap<Address, f64>,
}

impl PriceAgregator {
    pub fn new(usd_token_addresses: Vec<Address>) -> Self {
        let mut hashset = HashSet::new();
        for address in usd_token_addresses {
            hashset.insert(address);
        }

        PriceAgregator {
            usd_token_addresses: hashset,
            pools: HashMap::new(),
            token_to_biggest_pool: HashMap::new(),
            tokens_prices: HashMap::new(),
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
        let best_pool = match self.token_to_biggest_pool.get(&token.address) {
            Some(biggest_pool) => {
                if biggest_pool.reserve0 < pool.reserve0 || biggest_pool.reserve1 < pool.reserve1 {
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
