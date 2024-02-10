use web3::{Web3, transports::Http};
use std::fs;
use std::collections::{HashMap, HashSet};
use web3::types::{Address, BlockId, BlockNumber, U64, U256, CallRequest, Bytes};
use csv::Reader;
use std::str::FromStr;
use async_recursion::async_recursion;
use tokio::sync::Semaphore;
use std::sync::Arc;
use std::path::Path;
use lazy_static::lazy_static;
use ethabi::{Token, Contract, Function};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use chrono::Utc;

use crate::price_fetcher;

lazy_static! {
    pub static ref DEAD_ADDRESS: Address = Address::from_str("0x000000000000000000000000000000000000dead").unwrap();
    pub static ref UNICRYPT_ADDRSS: Address = Address::from_str("0x663a5c229c09b049e36dcc11a9b0d4a8eb9db214").unwrap();
    pub static ref OWNER_ADDRESS: Address = Address::from_str("0x65A8F07Bd9A8598E1b5B6C0a88F4779DBC077675").unwrap();
    pub static ref WETH_ADDRESS: Address = Address::from_str("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2").unwrap();
    pub static ref FACTORY_ADDRESS: Address = Address::from_str("0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f").unwrap();
    pub static ref ROUTER_ADDRESS: Address = Address::from_str("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D").unwrap();

    pub static ref FACTORY_ABI: Contract = {
        let abi_content = std::fs::read_to_string("abi/factory.abi")
            .expect("Unable to read abi/factory.abi");
        let contract = ethabi::Contract::load(abi_content.as_bytes())
            .expect("Error parsing abi/factory.abi");
        contract
    };

    pub static ref GET_PAIR_FUNCTION: Function = FACTORY_ABI.function("getPair").unwrap().clone();
}

pub struct Opts {
    pub events_file_path: String,
    pub calls_file_path: String,
    pub timestamps_cache_file_path: String,
    pub output_dir_path: String,
    pub rpc: String,
    pub blocks_after_call: u64,
}

#[async_recursion]
async fn get_blocks_for_timestamps_sorted(semaphore: Arc<Semaphore>, web3: Web3<Http>, timestamps: Vec<u64>, left_block: u64, right_block: u64) -> Vec<u64> {
    if timestamps.is_empty() {
        return Vec::new();
    }

    if left_block + 1 == right_block {
        return vec![left_block; timestamps.len()];
    }

    let mid_block = (left_block + right_block) / 2;
    let permit = semaphore.acquire().await.unwrap();

    let mut mid_timestamp = 0u64;
    for _ in 0..10 {
        if let Ok(x) = web3.eth().block(BlockId::Number(BlockNumber::Number(U64::from(mid_block)))).await {
            mid_timestamp = x.expect("Block by number returned empty block").timestamp.as_u64();
            break;
        }
    }
    drop(permit);
    if mid_timestamp == 0 {
        println!("Can not get block by number");
        std::process::exit(1);
    }

    let lower_bound = timestamps.partition_point(|&x| x < mid_timestamp);
    let upper_bound = timestamps.partition_point(|&x| x <= mid_timestamp);

    let mut res = Vec::new();

    if lower_bound > 0 {
        res.extend(get_blocks_for_timestamps_sorted(semaphore.clone(), web3.clone(), timestamps[0..lower_bound].to_vec(), left_block, mid_block).await);
    }
    if lower_bound != upper_bound {
        for _ in lower_bound..upper_bound {
            res.push(mid_block);
        }
    }
    if upper_bound < timestamps.len() {
        res.extend(get_blocks_for_timestamps_sorted(semaphore.clone(), web3.clone(), timestamps[upper_bound..timestamps.len()].to_vec(), mid_block, right_block).await);
    }

    res
}

async fn get_blocks_for_timestamps(web3: Web3<Http>, timestamps: HashSet<u64>) -> HashMap<u64, u64> {
    let mut timestamps: Vec<u64> = timestamps.into_iter().collect();
    timestamps.sort();

    let mut right_block = 0u64;
    for _ in 0..10 {
        if let Ok(x) = web3.eth().block_number().await {
            right_block = x.as_u64();
            break;
        }
    }
    if right_block == 0 {
        println!("Can not get current block number");
        std::process::exit(1);
    }

    let left_block = right_block - 5000000;

    let semaphore = Arc::new(Semaphore::new(200));

    let blocks = get_blocks_for_timestamps_sorted(semaphore, web3, timestamps.clone(), left_block, right_block).await;

    let mut res: HashMap<u64, u64> = HashMap::new();
    for i in 0..timestamps.len() {
        res.insert(timestamps[i], blocks[i]);
    }

    res
}

async fn collect_blocks_by_timestamps(web3: Web3<Http>, mut all_timestamps: HashSet<u64>, timestamps_cache_file_path: String) -> HashMap<u64, u64> {
    let mut timestamp_to_block: HashMap<u64, u64> = HashMap::new();

    if Path::new(&timestamps_cache_file_path).exists() {
        println!("Found timestamps cache file, reading cache...");
        let timestamps_cache_string = fs::read_to_string(timestamps_cache_file_path.clone()).expect("Can not read timestamps cache");
        let timestamps_cache_lines: Vec<String> = timestamps_cache_string.split('\n').map(|s| s.to_string()).collect();

        for line in timestamps_cache_lines {
            if line.is_empty() {
                continue;
            }
            let values: Vec<String> = line.split(',').map(|s| s.to_string()).collect();
            if values.len() != 2 {
                println!("Wrong amount of columns in timestamps cache file: got {}, expected 2", values.len());
                std::process::exit(1);
            }
            let timestamp = values[0].parse::<u64>().expect("Can not parse timestamp in cache file");
            let block = values[1].parse::<u64>().expect("Can not parse block in cache file");
            timestamp_to_block.insert(timestamp, block);
        }
    } else {
        println!("Timestamps cache file does not exist, continue without it...");
    }

    for (key, _) in &timestamp_to_block {
        all_timestamps.remove(key);
    }

    println!("Need to collect blocks of {} timestamps, starting collection...", all_timestamps.len());

    timestamp_to_block.extend(get_blocks_for_timestamps(web3.clone(), all_timestamps).await);

    let mut timestamps_cache_string = String::new();
    for (&key, &value) in &timestamp_to_block {
        timestamps_cache_string.push_str(&format!("{},{}\n", key, value));
    }

    fs::write(timestamps_cache_file_path.clone(), timestamps_cache_string).expect("Can not write to timestamps cache file");

    println!("Collecting of blocks finished, {} recordings saved to timestamps cache file", timestamp_to_block.len());

    return timestamp_to_block;
}

async fn collect_pools_by_tokens(web3: Web3<Http>, all_tokens: HashSet<Address>) -> HashMap<Address, Address> {
    println!("Starting collecting pools by tokens...");
    let semaphore = Arc::new(Semaphore::new(200));

    let res: Arc<Mutex<HashMap<Address, Address>>> = Arc::new(Mutex::new(HashMap::new()));

    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    for token in &all_tokens {
        let res = res.clone();
        let token = token.clone();
        let semaphore = semaphore.clone();
        let web3 = web3.clone();
        let handle = tokio::spawn(async move {
            let (mut token0, mut token1) = (WETH_ADDRESS.clone(), token.clone());
            if token0 > token1 {
                (token0, token1) = (token1, token0);
            }
            let data = GET_PAIR_FUNCTION.encode_input(&vec![Token::Address(token0), Token::Address(token1)]).unwrap();
            let call_request = CallRequest {
                from: None,
                to: Some(FACTORY_ADDRESS.clone()),
                gas: Some(U256::from(1000000)),
                gas_price: None,
                value: None,
                data: Some(Bytes(data)),
                transaction_type: None,
                access_list: None,
                max_fee_per_gas: None,
                max_priority_fee_per_gas: None,
            };

            let mut pool_address = Address::default();
            let permit = semaphore.acquire().await.unwrap();
            for _ in 0..10 {
                match web3.eth().call(call_request.clone(), None).await {
                    Ok(x) => {
                        let output_tokens = GET_PAIR_FUNCTION.decode_output(&x.0).unwrap();
                        match output_tokens[0].clone() {
                            Token::Address(x) => {
                                pool_address = x;
                                break;
                            },
                            _ => continue
                        }
                    },
                    Err(x) => {
                        sleep(Duration::from_millis(500)).await;
                    }
                }
            }
            drop(permit);

            if pool_address == Address::zero() {
                return;
            }

            let mut guard = res.lock().await;
            guard.insert(token, pool_address);
            drop(guard);
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    let res =  Arc::try_unwrap(res).unwrap().into_inner();

    println!("Pools by tokens collected, were tokens: {}, collected pools: {}", all_tokens.len(), res.len());

    res
}

#[derive(Default, Debug)]
struct BlockInfo {
    block: u64,
    timestamp: u64,
    gas_price: U256,
}

impl std::cmp::PartialOrd for BlockInfo {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.block.partial_cmp(&other.block)
    }
}

impl std::cmp::Ord for BlockInfo {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.block.cmp(&other.block)
    }
}

impl std::cmp::Eq for BlockInfo {
}

impl std::cmp::PartialEq for BlockInfo {
    fn eq(&self, other: &Self) -> bool {
        self.block == other.block
    }
}

async fn collect_block_info(web3: Web3<Http>, mut all_blocks: HashSet<u64>, output_dir_path: String) {
    println!("Starting collection block info by block...");
    
    if !Path::new(&output_dir_path).exists() {
        fs::create_dir(output_dir_path.clone()).expect("Can not create output dir");
    }

    let mut output_block_info_file_path = output_dir_path.clone();
    output_block_info_file_path.push_str("/block_info.csv");

    let mut res: Vec<BlockInfo> = Vec::new();
    if Path::new(&output_block_info_file_path).exists() {
        let block_info_string = fs::read_to_string(&output_block_info_file_path).expect("Can not read block info file");
        let mut rdr = Reader::from_reader(block_info_string.as_bytes());
        for row in rdr.records() {
            let row = row.expect("Can not read row in block info file");
            let elements: Vec<String> = row.iter().map(|x| x.to_string()).collect();
            if elements.len() != 3 {
                println!("Wrong amount of columns in block info file: got {}, expected 2", elements.len());
                std::process::exit(1);
            }
            let block = elements[0].parse::<u64>().expect("Can not parse block in block info file");
            let timestamp = elements[1].parse::<u64>().expect("Can not parse timestamp in block info file");
            let gas_price = U256::from_dec_str(&elements[2]).expect("Can not parse gas_price in block info file");
            res.push(BlockInfo {
                block: block,
                timestamp: timestamp,
                gas_price: gas_price,
            });
            all_blocks.remove(&block);
        }
    }

    println!("Need to collect block info of {} blocks", all_blocks.len());

    let result: Arc<Mutex<Vec<BlockInfo>>> = Arc::new(Mutex::new(res));

    let semaphore = Arc::new(Semaphore::new(200));
    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    for block in all_blocks {
        let web3 = web3.clone();
        let semaphore = semaphore.clone();
        let result = result.clone();
        let handle = tokio::spawn(async move {
            let permit = semaphore.acquire().await.unwrap();
            let mut block_info = BlockInfo::default();
            block_info.block = block;
            
            for _ in 0..10 {
                match web3.eth().block(BlockId::Number(BlockNumber::Number(U64::from(block)))).await {
                    Ok(x) => {
                        let x = x.unwrap();
                        block_info.gas_price = x.base_fee_per_gas.clone().unwrap();
                        block_info.timestamp = x.timestamp.clone().as_u64();
                        break;
                    },
                    _ => {
                        sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                }
            }
            drop(permit);
            if block_info.timestamp == 0 {
                println!("Can not collect gas_price of block {}", block);
                std::process::exit(1);
            }
            
            let mut guard = result.lock().await;
            guard.push(block_info);
            drop(guard);
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    let mut result = Arc::try_unwrap(result).unwrap().into_inner();
    result.sort();

    let mut res_string = String::new();
    res_string.push_str("block,timestamp,gas_price\n");

    for block_info in &result {
        res_string.push_str(&format!("{},{},{:?}\n", block_info.block, block_info.timestamp, block_info.gas_price));
    }

    fs::write(&output_block_info_file_path, res_string).expect("Can not write to block info file");

    println!("Collecting of block info finished, writed {} records in gas_price file", result.len());
}

struct PairCreatedEvent {
    factory: Address,
    token0: Address,
    token1: Address,
    pool: Address,
}

struct SyncEvent {
    pool: Address,
    reserve0: U256,
    reserve1: U256,
}

struct SwapEvent {
    pool: Address,
    sender: Address,
    amount0_in: U256,
    amount0_out: U256,
    amount1_in: U256,
    amount1_out: U256,
}

struct MintEvent {
    pool: Address,
    sender: Address,
    amount0: U256,
    amount1: U256,
}

struct BurnEvent {
    pool: Address,
    sender: Address,
    amount0: U256,
    amount1: U256,
}

enum RawEvent {
    PairCreated(PairCreatedEvent),
    Sync(SyncEvent),
    Swap(SwapEvent),
    Mint(MintEvent),
    Burn(BurnEvent),
}

struct Event {
    block: u64,
    raw_event: RawEvent,
}

impl std::cmp::PartialOrd for Event {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.block.partial_cmp(&other.block)
    }
}

impl std::cmp::Ord for Event {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.block.cmp(&other.block)
    }
}

impl std::cmp::Eq for Event {
}

impl std::cmp::PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.block == other.block
    }
}

impl Event {
    fn parse_address(str: &String) -> Address {
        Address::from_str(str).expect("Can not parse address in event")
    }

    fn parse_u256(str: &String) -> U256 {
        U256::from_dec_str(str).expect("Can not parse U256 in event")
    }

    fn from_string(str: &String) -> Self {
        let parts: Vec<String> = str.split(',').map(|x| x.to_string()).collect();

        let block = parts[1].parse::<u64>().expect("Can not parse block in event");
        if parts[0] == "0" {
            let factory = Self::parse_address(&parts[2]);
            let token0 = Self::parse_address(&parts[3]);
            let token1 = Self::parse_address(&parts[4]);
            let pool = Self::parse_address(&parts[5]);
            return Event {
                block: block,
                raw_event: RawEvent::PairCreated(PairCreatedEvent {
                    factory: factory,
                    token0: token0,
                    token1: token1,
                    pool: pool,     
                }),
            };
        } else if parts[0] == "1" {
            let pool = Self::parse_address(&parts[2]);
            let reserve0 = Self::parse_u256(&parts[3]);
            let reserve1 = Self::parse_u256(&parts[4]);
            return Event {
                block: block,
                raw_event: RawEvent::Sync(SyncEvent {
                    pool: pool,
                    reserve0: reserve0,
                    reserve1: reserve1,
                }),
            };
        } else if parts[0] == "2" {
            let pool = Self::parse_address(&parts[2]);
            let sender = Self::parse_address(&parts[3]);
            let amount0_in = Self::parse_u256(&parts[4]);
            let amount0_out = Self::parse_u256(&parts[5]);
            let amount1_in = Self::parse_u256(&parts[6]);
            let amount1_out = Self::parse_u256(&parts[7]);
            return Event {
                block: block,
                raw_event: RawEvent::Swap(SwapEvent {
                    pool: pool,
                    sender: sender,
                    amount0_in: amount0_in,
                    amount0_out: amount0_out,
                    amount1_in: amount1_in,
                    amount1_out: amount1_out,
                }),
            };
        } else if parts[0] == "3" {
            let pool = Self::parse_address(&parts[2]);
            let sender = Self::parse_address(&parts[3]);
            let amount0 = Self::parse_u256(&parts[4]);
            let amount1 = Self::parse_u256(&parts[5]);
            return Event {
                block: block,
                raw_event: RawEvent::Mint(MintEvent {
                    pool: pool,
                    sender: sender,
                    amount0: amount0,
                    amount1: amount1,
                }),
            };
        } else if parts[0] == "4" {
            let pool = Self::parse_address(&parts[2]);
            let sender = Self::parse_address(&parts[3]);
            let amount0 = Self::parse_u256(&parts[4]);
            let amount1 = Self::parse_u256(&parts[5]);
            return Event {
                block: block,
                raw_event: RawEvent::Burn(BurnEvent {
                    pool: pool,
                    sender: sender,
                    amount0: amount0,
                    amount1: amount1,
                }),
            };
        }
        println!("Wrong event type");
        std::process::exit(1);
    }

    fn to_string(&self) -> String {
        match &self.raw_event {
            RawEvent::PairCreated(e) => format!("0,{:?},{:?},{:?},{:?},{:?}", self.block, e.factory, e.token0, e.token1, e.pool),
            RawEvent::Sync(e) => format!("1,{:?},{:?},{:?},{:?}", self.block, e.pool, e.reserve0, e.reserve1),
            RawEvent::Swap(e) => format!("2,{:?},{:?},{:?},{:?},{:?},{:?},{:?}", self.block, e.pool, e.sender, e.amount0_in, e.amount0_out, e.amount1_in, e.amount1_out),
            RawEvent::Mint(e) => format!("3,{:?},{:?},{:?},{:?},{:?}", self.block, e.pool, e.sender, e.amount0, e.amount1),
            RawEvent::Burn(e) => format!("4,{:?},{:?},{:?},{:?},{:?}", self.block, e.pool, e.sender, e.amount0, e.amount1),
        }
    }

    fn get_pool(&self) -> Address {
        match &self.raw_event {
            RawEvent::PairCreated(e) => {
                e.pool
            },
            RawEvent::Sync(e) => {
                e.pool
            },
            RawEvent::Swap(e) => {
                e.pool
            },
            RawEvent::Mint(e) => {
                e.pool
            },
            RawEvent::Burn(e) => {
                e.pool
            }
        }
    }
}

async fn collect_pool_events(events_file_path: String, output_dir_path: String, token_to_pool: &HashMap<Address, Address>) {
    println!("Starting collection of pool events of all tokens...");
    let events_string = fs::read_to_string(&events_file_path).expect("Can not read events");

    let lines: Vec<String> = events_string.split('\n').map(|s| s.to_string()).collect();
    drop(events_string);

    let mut pool_to_events: HashMap<Address, Vec<Event>> = HashMap::new();
    for (_, &pool) in token_to_pool {
        pool_to_events.insert(pool, Vec::new());
    }

    let total = lines.len();
    let mut prev_procent = 0usize;

    println!("Found {} events in file, starting filtering events...", total);

    for (i, line) in lines.into_iter().enumerate() {
        let curr_procent = i * 100 / total;
        if curr_procent >= prev_procent + 5 {
            println!("{} Completed {}%", Utc::now().format("%H:%M:%S"), curr_procent);
            prev_procent = curr_procent;
        }
        if line.is_empty() {
            continue;
        }
        let event = Event::from_string(&line);
        let pool = event.get_pool();
        if pool_to_events.contains_key(&pool) {
            pool_to_events.get_mut(&pool).unwrap().push(event);
        }
    }

    println!("Filtering finished, starting saving events to files...");

    let total = token_to_pool.len();
    let mut prev_procent = 0usize;

    for (i, (token, pool)) in token_to_pool.iter().enumerate() {
        let curr_procent = i * 100 / total;
        if curr_procent >= prev_procent + 5 {
            println!("{} Completed {}%", Utc::now().format("%H:%M:%S"), curr_procent);
            prev_procent = curr_procent;
        }
        let mut events = pool_to_events.remove(&pool).unwrap();
        events.sort();
        let mut token_dir_path = output_dir_path.clone();
        token_dir_path.push_str(&format!("/{:?}", token));
        if !Path::new(&token_dir_path).exists() {
            fs::create_dir(&token_dir_path).expect("Can not create dir for token");
        }
        let mut token_events_file_path = token_dir_path.clone();
        token_events_file_path.push_str("/events.txt");

        let mut string_to_save = String::new();
        for event in events {
            string_to_save.push_str(&event.to_string());
            string_to_save.push('\n');
        }

        fs::write(&token_events_file_path, string_to_save).expect("Can not save token events to file");
    }

    println!("Saving events to files finished");
}

#[derive(Clone)]
struct CallInfo {
    call_id: u64,
    timestamp: u64,
}

impl std::cmp::PartialOrd for CallInfo {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.timestamp.partial_cmp(&other.timestamp)
    }
}

impl std::cmp::Ord for CallInfo {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}

impl std::cmp::Eq for CallInfo {
}

impl std::cmp::PartialEq for CallInfo {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp
    }
}

fn collect_calls_info(output_dir_path: String, token_to_call_infos: &HashMap<Address, Vec<CallInfo>>, timestamp_to_block: &HashMap<u64, u64>) {
    println!("Starting collection of calls info...");
    for (&token, calls) in token_to_call_infos {
        let mut calls = calls.clone();
        calls.sort();

        let calls_infos_file_path = format!("{}/{:?}/calls_info.csv", output_dir_path, token);
        let mut res_string = String::new();

        res_string.push_str("call_id,timestamp,block\n");
        for call in calls {
            res_string.push_str(&format!("{},{},{}\n", call.call_id, call.timestamp, timestamp_to_block.get(&call.timestamp).unwrap()));
        }

        fs::write(&calls_infos_file_path, res_string).expect("Can not write to calls_info file");
    }
    println!("Collection of calls info finished");
}


#[derive(Default, Clone)]
struct PriceData {
    block: u64,
    buy_amounts: Vec<U256>,
    sell_amounts_x: Vec<U256>,
    sell_amounts_y: Vec<U256>,
    buy_gas_used: u64,
    approve_gas_used: u64,
    sell_gas_used: u64,
    pool_liquidity: Option<U256>,
    liquidity_on_zero_address: Option<U256>,
    liquidity_on_dead_address: Option<U256>,
    liquidity_on_unicrypt_address: Option<U256>,
}

impl PriceData {
    fn get_header(sell_amounts_size: usize) -> String {
        let mut res = String::new();
        res.push_str("block,");
        for i in 0..10 {
            res.push_str(&format!("buy_amount_out_0.{},", i + 1));
        }
        for i in 0..sell_amounts_size {
            res.push_str(&format!("sell_amount_in_{},", i));
            res.push_str(&format!("sell_amount_out_{},", i));
        }
        res.push_str("buy_gas_used,approve_gas_used,sell_gas_used,pool_liquidity,liquidity_on_zero_address,liquidity_on_dead_address,liquidity_on_unicrypt_address");
        res
    }

    fn to_string(&self) -> String {
        let mut res = String::new();
        res.push_str(&format!("{},", self.block));
        for i in 0..10 {
            if self.buy_amounts[i].is_zero() {
                res.push(',');
            } else {
                res.push_str(&format!("{:?},", self.buy_amounts[i]));
            }
        }
        for i in 0..self.sell_amounts_x.len() {
            if self.sell_amounts_x[i].is_zero() {
                res.push(',');
            } else {
                res.push_str(&format!("{:?},", self.sell_amounts_x[i]));
            }
            if self.sell_amounts_y[i].is_zero() {
                res.push(',');
            } else {
                res.push_str(&format!("{:?},", self.sell_amounts_y[i]));
            }
        }
        res.push_str(&format!("{},{},{},", self.buy_gas_used, self.approve_gas_used, self.sell_gas_used));
        if let Some(x) = self.pool_liquidity {
            res.push_str(&format!("{:?},", x));
        } else {
            res.push(',');
        }
        if let Some(x) = self.liquidity_on_zero_address {
            res.push_str(&format!("{:?},", x));
        } else {
            res.push(',');
        }
        if let Some(x) = self.liquidity_on_dead_address {
            res.push_str(&format!("{:?},", x));
        } else {
            res.push(',');
        }
        if let Some(x) = self.liquidity_on_unicrypt_address {
            res.push_str(&format!("{:?}", x));
        }

        res
    }
}

struct StatsAggregator {
    total: usize,
    done: usize,
    prev_procent: usize,

    amount_buys: usize,
    amount_buys_failed: usize,
    amount_sells: usize,
    amount_sells_failed: usize,
}

impl StatsAggregator {
    fn new(total: usize) -> Self {
        StatsAggregator {
            total: total,
            done: 0,
            prev_procent: 0,
            amount_buys: 0,
            amount_buys_failed: 0,
            amount_sells: 0,
            amount_sells_failed: 0,
        }
    }

    fn print_info(&self) {
        println!("{} Completed {}%: failed buys: {}/{}, failed sells: {}/{}", Utc::now().format("%H:%M:%S"), self.prev_procent, self.amount_buys_failed, self.amount_buys, self.amount_sells_failed, self.amount_sells);
    }

    fn done(&mut self) {
        self.done += 1;
        let curr_procent = self.done * 100 / self.total;
        if curr_procent >= self.prev_procent + 5 {
            self.prev_procent = curr_procent;
            self.print_info();
        }
    }
}

async fn collect_prices_for_token(web3: Web3<Http>, output_dir_path: String, token: Address, pool: Address, blocks: Vec<u64>, stats: Arc<Mutex<StatsAggregator>>) {   
    let sell_amounts_size = 20usize;
    
    let pf = price_fetcher::PriceFetcher::new(web3.clone(), OWNER_ADDRESS.clone(), ROUTER_ADDRESS.clone(), WETH_ADDRESS.clone()); 
    
    let mut datas: Vec<PriceData> = vec![PriceData::default(); blocks.len()];
    for (i, &block) in blocks.iter().enumerate() {
        datas[i].block = block;
        datas[i].buy_amounts.resize(10, U256::zero());
        datas[i].sell_amounts_x.resize(sell_amounts_size, U256::zero());
        datas[i].sell_amounts_y.resize(sell_amounts_size, U256::zero());
    }

    let mut amount_buys = 0usize;
    let mut amount_buys_failed = 0usize;
    let mut amount_sells = 0usize;
    let mut amount_sells_failed = 0usize;

    let mut min_amount_out = U256::MAX;
    let mut max_amount_out = U256::zero();
    for (i, &block) in blocks.iter().enumerate() {
        for j in 0..10 {
            amount_buys += 1;

            let amount_in_eth = U256::from(100000000000000000u128) * (j + 1); // 10^17
            match pf.get_buy_price(BlockNumber::Number(U64::from(block)), amount_in_eth, token).await {
                Ok((amount_out, gas_used)) => {
                    if amount_out > max_amount_out {
                        max_amount_out = amount_out;
                    }
                    if amount_out < min_amount_out {
                        min_amount_out = amount_out;
                    }
                    datas[i].buy_amounts[j] = amount_out;
                    datas[i].buy_gas_used = gas_used.as_u64();
                },
                _ => {
                    amount_buys_failed += 1;
                    continue;
                }
            }
        }
        if min_amount_out <= max_amount_out {
            let state = match pf.get_state_with_tokens(token, pool, max_amount_out, OWNER_ADDRESS.clone(), BlockNumber::Number(U64::from(block - 1))).await {
                Ok(x) => Some(x),
                _ => None
            };
            let step = (max_amount_out - min_amount_out) / (sell_amounts_size - 1);
            for j in 0..sell_amounts_size {
                amount_sells += 1;

                let amount_in = min_amount_out + step * j;
                datas[i].sell_amounts_x[j] = amount_in;
                match pf.get_sell_price(BlockNumber::Number(U64::from(block)), amount_in, token, pool, state.clone()).await {
                    Ok((amount_out, gas_used_swap, gas_used_approve)) => {
                        datas[i].sell_gas_used = gas_used_swap.as_u64();
                        datas[i].approve_gas_used = gas_used_approve.as_u64();
                        datas[i].sell_amounts_y[j] = amount_out;
                    },
                    _ => {
                        amount_sells_failed += 1;
                        continue;
                    }
                }
            }
        }
    }

    for (i, &block) in blocks.iter().enumerate() {
        let block_number = BlockNumber::Number(U64::from(block));
        if let Ok(x) = pf.get_total_supply(pool, block_number).await {
            datas[i].pool_liquidity = Some(x);
        }
        if let Ok(x) = pf.get_balance_of_call(pool, Address::zero(), block_number).await {
            datas[i].liquidity_on_zero_address = Some(x);
        }
        if let Ok(x) = pf.get_balance_of_call(pool, DEAD_ADDRESS.clone(), block_number).await {
            datas[i].liquidity_on_dead_address = Some(x);
        }
        if let Ok(x) = pf.get_balance_of_call(pool, UNICRYPT_ADDRSS.clone(), block_number).await {
            datas[i].liquidity_on_unicrypt_address = Some(x);
        }
    }

    let mut res_string = String::new();
    res_string.push_str(&PriceData::get_header(sell_amounts_size));
    res_string.push('\n');

    for data in datas {
        res_string.push_str(&data.to_string());
        res_string.push('\n');
    }

    let price_data_file_path = format!("{}/{:?}/price_data.csv", output_dir_path, token);
    fs::write(&price_data_file_path, res_string).expect("Can not write to price_data file");

    let mut guard = stats.lock().await;
    guard.amount_buys += amount_buys;
    guard.amount_buys_failed += amount_buys_failed;
    guard.amount_sells += amount_sells;
    guard.amount_sells_failed += amount_sells_failed;
    guard.done();
    drop(guard);
}

async fn collect_prices(web3: Web3<Http>, output_dir_path: String, token_to_blocks: HashMap<Address, HashSet<u64>>, token_to_pool: HashMap<Address, Address>) {
    println!("Starting collection prices for tokens...");

    let semaphore = Arc::new(Semaphore::new(200));
    let stats = Arc::new(Mutex::new(StatsAggregator::new(token_to_blocks.len())));

    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    for (token, pool) in &token_to_pool {
        let token = token.clone();
        let pool = pool.clone();
        let blocks = token_to_blocks.get(&token).unwrap().clone();
        let web3 = web3.clone();
        let output_dir_path = output_dir_path.clone();
        let semaphore = semaphore.clone();
        let stats = stats.clone();
        
        let handle = tokio::spawn(async move {
            let mut blocks: Vec<u64> = blocks.into_iter().collect();
            blocks.sort();

            let permit = semaphore.acquire().await.unwrap();
            collect_prices_for_token(web3, output_dir_path, token, pool, blocks, stats).await;
            drop(permit);
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    println!("Collection of prices finished");
}

pub async fn collect(opts: Opts) {
    let http = Http::new(&opts.rpc.clone()).expect("Can't connect to RPC");
    let web3 = Web3::new(http);

    let calls_string = fs::read_to_string(opts.calls_file_path.clone()).expect("Can not read calls");

    let mut rdr = Reader::from_reader(calls_string.as_bytes());

    let mut token_to_timestamps: HashMap<Address, HashSet<u64>> = HashMap::new();
    let mut token_to_call_infos: HashMap<Address, Vec<CallInfo>> = HashMap::new();
    let mut all_timestamps: HashSet<u64> = HashSet::new();

    let mut amounted_skipped = 0u32;
    for row in rdr.records() {
        let row = row.expect("Can not read row in calls file");
        let row: Vec<String> = row.iter().map(|s| s.to_string()).collect();
        
        if row.len() != 25 {
            println!("Wrong amount of columns in calls file: got {}, expected 25", row.len());
            return;
        }

        let token = match Address::from_str(&row[17]) {
            Ok(x) => x,
            _ => {
                amounted_skipped += 1;
                continue;
            }
        };
        let call_id = row[0].parse::<u64>().expect("Can not parse call_id in calls file");
        let timestamp = row[24].parse::<u64>().expect("Can not parse timestamp in calls file");

        token_to_timestamps.entry(token).or_default().insert(timestamp);
        token_to_call_infos.entry(token).or_default().push(CallInfo { call_id: call_id, timestamp: timestamp });
        all_timestamps.insert(timestamp);
    }

    println!("Calls file parsed, records skipped because of bad address: {}, unique tokens: {}", amounted_skipped, token_to_timestamps.len());

    let timestamp_to_block = collect_blocks_by_timestamps(web3.clone(), all_timestamps, opts.timestamps_cache_file_path.clone()).await;

    let mut all_tokens: HashSet<Address> = HashSet::new();
    let mut token_to_blocks: HashMap<Address, HashSet<u64>> = HashMap::new();

    for (token, timestamps) in token_to_timestamps {
        let mut blocks: HashSet<u64> = HashSet::new();
        for timestamp in timestamps {
            let &block = timestamp_to_block.get(&timestamp).unwrap();
            for i in 0..opts.blocks_after_call {
                blocks.insert(block + i);
            }
        }
        token_to_blocks.insert(token, blocks);
        all_tokens.insert(token);
    }

    let token_to_pool = collect_pools_by_tokens(web3.clone(), all_tokens).await;

    let mut tokens_to_remove: Vec<Address> = Vec::new();
    for (token, _) in &token_to_blocks {
        if !token_to_pool.contains_key(token) {
            tokens_to_remove.push(*token);
        }
    }

    println!("Removing {} tokens because of lack of pool", tokens_to_remove.len());

    for token in tokens_to_remove {
        token_to_blocks.remove(&token);
        token_to_call_infos.remove(&token);
    }

    let mut all_blocks: HashSet<u64> = HashSet::new();
    for (_, blocks) in &token_to_blocks {
        for block in blocks {
            all_blocks.insert(*block);   
        }
    }
    
    collect_block_info(web3.clone(), all_blocks, opts.output_dir_path.clone()).await;

    collect_pool_events(opts.events_file_path.clone(), opts.output_dir_path.clone(), &token_to_pool).await;
    collect_calls_info(opts.output_dir_path.clone(), &token_to_call_infos, &timestamp_to_block);
    collect_prices(web3.clone(), opts.output_dir_path.clone(), token_to_blocks, token_to_pool).await;

}