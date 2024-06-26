use super::normalize;
use crate::db::models::{SyncEvent, Token};
use std::collections::{HashMap, HashSet};
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

        if reserve0 < 1e-8 {
            return 0.0;
        }

        reserve1 / reserve0
    }

    fn price1(&self) -> f64 {
        let reserve0 = normalize(self.reserve0, self.token0.decimals);
        let reserve1 = normalize(self.reserve1, self.token1.decimals);

        if reserve1 < 1e-8 {
            return 0.0;
        }

        reserve0 / reserve1
    }
}

pub struct PriceAgregator {
    usd_token_addresses: HashSet<Address>,
    pools: Vec<Pool>,
    token_to_biggest_pool: HashMap<Address, usize>,
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
            pools: Vec::new(),
            token_to_biggest_pool: HashMap::new(),
            tokens_prices: HashMap::new(),
            decent_token_addresses: decent_tokens_hashset,
        }
    }

    pub fn handle_sync(&mut self, token0: &Token, token1: &Token, event: &SyncEvent) {
        self.pools.push(Pool {
            address: event.address,
            token0: token0.clone(),
            token1: token1.clone(),
            reserve0: event.reserve0,
            reserve1: event.reserve1,
        });

        let idx = self.pools.len() - 1;

        self.update_price(token0, idx);
        self.update_price(token1, idx);
    }

    fn update_price(&mut self, token: &Token, pool_idx: usize) {
        let best_pool_idx = match self.find_best_pool(token, pool_idx) {
            Some(r) => r,
            None => return,
        };

        self.token_to_biggest_pool
            .insert(token.address, best_pool_idx);

        let best_pool = &self.pools[best_pool_idx];
        let usd_price = if best_pool.token0.address == token.address {
            best_pool.price0() * self.token_usd_price(&best_pool.token1)
        } else {
            best_pool.price1() * self.token_usd_price(&best_pool.token0)
        };

        if usd_price == 0.0 {
            return;
        }

        self.tokens_prices.insert(token.address, usd_price);
    }

    fn find_best_pool(&mut self, token: &Token, pool_idx: usize) -> Option<usize> {
        let pool = &self.pools[pool_idx];

        match self.token_to_biggest_pool.get(&token.address) {
            Some(biggest_pool_idx) => {
                let biggest_pool_idx = *biggest_pool_idx;
                let biggest_pool = &self.pools[biggest_pool_idx];
                if pool.address == biggest_pool.address {
                    return Some(pool_idx);
                }

                if self.decent_token_addresses.contains(&token.address)
                    && self.decent_token_addresses.contains(&pool.token0.address)
                        != self.decent_token_addresses.contains(&pool.token1.address)
                {
                    return Some(biggest_pool_idx);
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
                    Some(pool_idx)
                } else {
                    Some(biggest_pool_idx)
                }
            }
            None => {
                if self.decent_token_addresses.contains(&token.address)
                    && self.decent_token_addresses.contains(&pool.token0.address)
                        != self.decent_token_addresses.contains(&pool.token1.address)
                {
                    return None;
                }

                Some(pool_idx)
            }
        }
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
