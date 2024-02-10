use std::{fs, io::Write};
use lazy_static::lazy_static;
use web3::{Web3, transports::Http};
use web3::types::{Log, Filter, H256, FilterBuilder, BlockNumber, U64};
use tokio::time::{sleep, Duration};
use async_recursion::async_recursion;
use tokio::sync::Semaphore;
use std::sync::Arc;
use chrono::Utc;
use ethabi::{Contract, Event, RawLog, Token};


lazy_static! {
    static ref POOL_ABI: Contract = {
        let abi_content = std::fs::read_to_string("abi/pool.abi")
            .expect("Unable to read abi/pool.abi");
        let contract = ethabi::Contract::load(abi_content.as_bytes())
            .expect("Error parsing abi/pool.abi");
        contract
    };

    static ref FACTORY_ABI: Contract = {
        let abi_content = std::fs::read_to_string("abi/factory.abi")
            .expect("Unable to read abi/factory.abi");
        let contract = ethabi::Contract::load(abi_content.as_bytes())
            .expect("Error parsing abi/factory.abi");
        contract
    };

    static ref PAIR_CREATED_EVENT: Event = FACTORY_ABI.event("PairCreated").unwrap().clone();
    static ref SYNC_EVENT: Event = POOL_ABI.event("Sync").unwrap().clone();
    static ref SWAP_EVENT: Event = POOL_ABI.event("Swap").unwrap().clone();
    static ref MINT_EVENT: Event = POOL_ABI.event("Mint").unwrap().clone();
    static ref BURN_EVENT: Event = POOL_ABI.event("Burn").unwrap().clone();
}

pub struct Opts {
    pub from_block: u64,
    pub to_block: u64,
    pub path: String,
    pub rpc: String,
}

#[derive(Clone)]
struct LocalFilter {
    topics: Vec<H256>,
}

impl LocalFilter {
    fn get_filter(&self, from_block: u64, to_block: u64) -> Filter {
        FilterBuilder::default()
            .from_block(BlockNumber::Number(U64::from(from_block)))
            .to_block(BlockNumber::Number(U64::from(to_block)))
            .topics(Some(self.topics.clone()), None, None, None)
            .build()
    }
}

fn convert_logs_to_string(logs: Vec<Log>) -> String {
    let mut res = String::new();

    for log in logs {
        let topic = log.topics[0].clone();
        match topic {
            _ if topic == PAIR_CREATED_EVENT.signature() => {
                if let Ok(x) = PAIR_CREATED_EVENT.parse_log(RawLog { topics: log.topics.clone(), data: log.data.0.clone() }) {
                    let token0 = match x.params[0].value.clone() {
                        Token::Address(x) => x,
                        _ => continue
                    };
                    let token1 = match x.params[1].value.clone() {
                        Token::Address(x) => x,
                        _ => continue
                    };
                    let pool = match x.params[2].value.clone() {
                        Token::Address(x) => x,
                        _ => continue
                    };
                    res.push_str(&format!("0,{:?},{:?},{:?},{:?},{:?}\n", log.block_number.unwrap(), log.address, token0, token1, pool));
                }
            },
            _ if topic == SYNC_EVENT.signature() => {
                if let Ok(x) = SYNC_EVENT.parse_log(RawLog { topics: log.topics.clone(), data: log.data.0.clone() }) {
                    let reserve0 = match x.params[0].value.clone() {
                        Token::Uint(x) => x,
                        _ => continue
                    };
                    let reserve1 = match x.params[1].value.clone() {
                        Token::Uint(x) => x,
                        _ => continue
                    };
                    res.push_str(&format!("1,{:?},{:?},{:?},{:?}\n", log.block_number.unwrap(), log.address, reserve0, reserve1));
                }
            },
            _ if topic == SWAP_EVENT.signature() => {
                if let Ok(x) = SWAP_EVENT.parse_log(RawLog { topics: log.topics.clone(), data: log.data.0.clone() }) {
                    let sender = match x.params[0].value.clone() {
                        Token::Address(x) => x,
                        _ => continue
                    };
                    let amount0_in = match x.params[1].value.clone() {
                        Token::Uint(x) => x,
                        _ => continue
                    };
                    let amount1_in = match x.params[2].value.clone() {
                        Token::Uint(x) => x,
                        _ => continue
                    };
                    let amount0_out = match x.params[3].value.clone() {
                        Token::Uint(x) => x,
                        _ => continue
                    };
                    let amount1_out = match x.params[4].value.clone() {
                        Token::Uint(x) => x,
                        _ => continue
                    };
                    res.push_str(&format!("2,{:?},{:?},{:?},{:?},{:?},{:?},{:?}\n", log.block_number.unwrap(), log.address, sender, amount0_in, amount0_out, amount1_in, amount1_out)); 
                }
            },
            _ if topic == MINT_EVENT.signature() => {
                if let Ok(x) = MINT_EVENT.parse_log(RawLog { topics: log.topics.clone(), data: log.data.0.clone() }) {
                    let sender = match x.params[0].value.clone() {
                        Token::Address(x) => x,
                        _ => continue
                    };
                    let amount0 = match x.params[1].value.clone() {
                        Token::Uint(x) => x,
                        _ => continue
                    };
                    let amount1 = match x.params[2].value.clone() {
                        Token::Uint(x) => x,
                        _ => continue
                    };
                    res.push_str(&format!("3,{:?},{:?},{:?},{:?},{:?}\n", log.block_number.unwrap(), log.address, sender, amount0, amount1));
                }
            },
            _ if topic == BURN_EVENT.signature() => {
                if let Ok(x) = BURN_EVENT.parse_log(RawLog { topics: log.topics.clone(), data: log.data.0.clone() }) {
                    let sender = match x.params[0].value.clone() {
                        Token::Address(x) => x,
                        _ => continue
                    };
                    let amount0 = match x.params[1].value.clone() {
                        Token::Uint(x) => x,
                        _ => continue
                    };
                    let amount1 = match x.params[2].value.clone() {
                        Token::Uint(x) => x,
                        _ => continue
                    };
                    res.push_str(&format!("4,{:?},{:?},{:?},{:?},{:?}\n", log.block_number.unwrap(), log.address, sender, amount0, amount1));
                }
            },
            _ => continue
        };
    }

    res
}

#[async_recursion]
async fn get_logs(web3: Web3<Http>, from_block: u64, to_block: u64, filter: LocalFilter, semaphore: Arc<Semaphore>) -> String {
    if to_block - from_block > 1000 {
        let mid = (from_block + to_block) / 2;

        let mut a = get_logs(web3.clone(), from_block, mid, filter.clone(), semaphore.clone()).await;
        a.push_str(&get_logs(web3.clone(), mid + 1, to_block, filter.clone(), semaphore.clone()).await);
        return a;
    }

    loop {
        let permit = semaphore.acquire().await.unwrap();
        let res =  web3.eth().logs(filter.get_filter(from_block, to_block)).await;
        drop(permit);
        
        match res {
            Ok(x) => return convert_logs_to_string(x),
            Err(x) => println!("Error doing from block {} to {}: {:?}", from_block, to_block, x)
        };
        if to_block - from_block < 100 {
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        let mid = (from_block + to_block) / 2;

        let mut a = get_logs(web3.clone(), from_block, mid, filter.clone(), semaphore.clone()).await;
        a.push_str(&get_logs(web3.clone(), mid + 1, to_block, filter.clone(), semaphore.clone()).await);
        return a;
    }
}

pub async fn collect(opts: Opts) {
    if std::path::Path::new(&opts.path.clone()).exists() {
        fs::remove_file(opts.path.clone()).expect("Can not remove existing file");
    }
    let mut file = fs::OpenOptions::new().create(true).write(true).open(opts.path.clone()).expect("Can not open file");

    let http = Http::new(&opts.rpc).expect("Can not create http");
    let web3 = Web3::new(http);


    let local_filter = LocalFilter {
        topics: vec![PAIR_CREATED_EVENT.signature(), SYNC_EVENT.signature(), SWAP_EVENT.signature(), MINT_EVENT.signature(), BURN_EVENT.signature()],
    };

    let amount_block_one_iter = 50000;
    let iters = (opts.to_block - opts.from_block - 1) / amount_block_one_iter + 1;

    println!("{} Starting collection, total iters: {}", Utc::now().format("%H:%M:%S"), iters);
    for i in 0..iters {
        let from_block = opts.from_block + i * amount_block_one_iter;
        let mut to_block = from_block + amount_block_one_iter - 1;
        if to_block > opts.to_block {
            to_block = opts.to_block;
        }
        let semaphore = Arc::new(Semaphore::new(200));

        let data = get_logs(web3.clone(), from_block, to_block, local_filter.clone(), semaphore).await;

        file.write_all(data.as_bytes()).expect("Can not write to file");
        file.flush().expect("Can not flush file");

        println!("{} {}/{}", Utc::now().format("%H:%M:%S"), i + 1, iters);
    }
}