use std::ops::Add;

use web3::types::{Address, U256};

pub struct CEXData {
    address: Address,
}

pub enum Event {
    Sync(SyncEvent),
    Swap(SwapEvent),
    Mint(MintEvent),
    Burn(BurnEvent),
}

pub fn parse_event(args: Vec<String>) -> Event {
    match args[0].as_str() {
        "1" => Event::Sync(SyncEvent::new(args)),
        "2" => Event::Swap(SwapEvent::new(args)),
        "3" => Event::Mint(MintEvent::new(args)),
        "4" => Event::Burn(BurnEvent::new(args)),
        _ => panic!("Invalid args"),
    }
}

pub struct SyncEvent {
    block_number: u64,
    address: Address,
    reserve0: U256,
    reserve1: U256,
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

pub struct SwapEvent {
    block_number: u64,
    address: Address,
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

pub struct MintEvent {
    block_number: u64,
    address: Address,
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

pub struct BurnEvent {
    block_number: u64,
    address: Address,
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
