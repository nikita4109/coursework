use serde::{Deserialize, Serialize};
use web3::types::{Address, U256};

#[derive(Debug, Serialize)]
pub struct SyncTick {
    pub token_symbol: String,
    pub event: SyncEvent,
}

#[derive(Debug, Deserialize)]
pub struct CEXRecord {
    id: u32,
    name: String,
    pub symbol: String,
    slug: String,
    cmcRank: u32,
    circulatingSupply: f64,
    totalSupply: f64,
    lastUpdated: String,
    dateAdded: String,
    numMarketPairs: u32,
    price: f64,
    volume24h: f64,
    marketCap: f64,
    platform_id: String,
    pub platform_slug: String,
    platform_symbol: String,
    pub token_adress: String,
}

pub struct CEXData {
    pub address: Address,
    pub token_symbol: String,
}

#[derive(Debug, Serialize, Clone)]
pub enum Event {
    Sync(SyncEvent),
    Swap(SwapEvent),
    Mint(MintEvent),
    Burn(BurnEvent),
}

pub fn parse_event(args: Vec<String>) -> Option<Event> {
    match args[0].as_str() {
        "0" => None,
        "1" => Some(Event::Sync(SyncEvent::new(args))),
        "2" => Some(Event::Swap(SwapEvent::new(args))),
        "3" => Some(Event::Mint(MintEvent::new(args))),
        "4" => Some(Event::Burn(BurnEvent::new(args))),
        _ => panic!("Invalid args"),
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct SyncEvent {
    pub block_number: u64,
    pub address: Address,
    pub reserve0: U256,
    pub reserve1: U256,
}

impl SyncEvent {
    pub fn new(args: Vec<String>) -> Self {
        Self {
            block_number: args[1].parse().expect("block number is invalid"),
            address: args[2].parse().expect("address is invalid"),
            reserve0: args[3].parse().expect("reserve0 is invalid"),
            reserve1: args[4].parse().expect("reserve1 is invalid"),
        }
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct SwapEvent {
    block_number: u64,
    pub address: Address,
    sender: Address,
    amount0_in: U256,
    amount0_out: U256,
    amount1_in: U256,
    amount1_out: U256,
}

impl SwapEvent {
    pub fn new(args: Vec<String>) -> Self {
        Self {
            block_number: args[1].parse().expect("block number is invalid"),
            address: args[2].parse().expect("address is invalid"),
            sender: args[3].parse().expect("sender is invalid"),
            amount0_in: args[4].parse().expect("amount0_in is invalid"),
            amount0_out: args[5].parse().expect("amount0_out is invalid"),
            amount1_in: args[6].parse().expect("amount1_in is invalid"),
            amount1_out: args[7].parse().expect("amount1_out is invalid"),
        }
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct MintEvent {
    block_number: u64,
    pub address: Address,
    sender: Address,
    amount0: U256,
    amount1: U256,
}

impl MintEvent {
    pub fn new(args: Vec<String>) -> Self {
        Self {
            block_number: args[1].parse().expect("block number is invalid"),
            address: args[2].parse().expect("address is invalid"),
            sender: args[3].parse().expect("sender is invalid"),
            amount0: args[4].parse().expect("amount0 is invalid"),
            amount1: args[5].parse().expect("amount1 is invalid"),
        }
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct BurnEvent {
    block_number: u64,
    pub address: Address,
    sender: Address,
    amount0: U256,
    amount1: U256,
}

impl BurnEvent {
    pub fn new(args: Vec<String>) -> Self {
        Self {
            block_number: args[1].parse().expect("block number is invalid"),
            address: args[2].parse().expect("address is invalid"),
            sender: args[3].parse().expect("sender is invalid"),
            amount0: args[4].parse().expect("amount0 is invalid"),
            amount1: args[5].parse().expect("amount1 is invalid"),
        }
    }
}
