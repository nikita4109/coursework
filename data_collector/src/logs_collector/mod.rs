use async_recursion::async_recursion;
use chrono::Utc;
use ethabi::{Contract, Event, RawLog, Token};
use lazy_static::lazy_static;
use std::collections::HashSet;
use std::sync::Arc;
use std::{fs, io::Write};
use tokio::sync::Semaphore;
use tokio::time::{sleep, Duration};
use web3::types::{Address, BlockNumber, Filter, FilterBuilder, Log, TransactionId, H256, U64};
use web3::{transports::Http, Web3};

lazy_static! {
    static ref POOL_ABI: Contract = {
        let abi_content =
            std::fs::read_to_string("abi/pool.abi").expect("Unable to read abi/pool.abi");
        let contract =
            ethabi::Contract::load(abi_content.as_bytes()).expect("Error parsing abi/pool.abi");
        contract
    };
    static ref FACTORY_ABI: Contract = {
        let abi_content =
            std::fs::read_to_string("abi/factory.abi").expect("Unable to read abi/factory.abi");
        let contract =
            ethabi::Contract::load(abi_content.as_bytes()).expect("Error parsing abi/factory.abi");
        contract
    };
    static ref ERC20_ABI: Contract = {
        let abi_content =
            std::fs::read_to_string("abi/erc20.abi").expect("Unable to read abi/erc20.abi");
        let contract =
            ethabi::Contract::load(abi_content.as_bytes()).expect("Error parsing abi/erc20.abi");
        contract
    };
    static ref PAIR_CREATED_EVENT: Event = FACTORY_ABI.event("PairCreated").unwrap().clone();
    static ref SYNC_EVENT: Event = POOL_ABI.event("Sync").unwrap().clone();
    static ref SWAP_EVENT: Event = POOL_ABI.event("Swap").unwrap().clone();
    static ref MINT_EVENT: Event = POOL_ABI.event("Mint").unwrap().clone();
    static ref BURN_EVENT: Event = POOL_ABI.event("Burn").unwrap().clone();
    static ref TRANSFER_EVENT: Event = ERC20_ABI.event("Transfer").unwrap().clone();
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

async fn convert_logs_to_string(
    web3: Web3<Http>,
    logs: Vec<Log>,
    needed_pools: &HashSet<Address>,
) -> (String, String, String, String, String, String) {
    let mut pair_created_logs = String::new();
    let mut sync_logs = String::new();
    let mut swap_logs = String::new();
    let mut mint_logs = String::new();
    let mut burn_logs = String::new();
    let mut transfers_logs = String::new();

    for log in logs {
        if !needed_pools.contains(&log.address) {
            continue;
        }

        let topic = log.topics[0].clone();
        match topic {
            _ if topic == TRANSFER_EVENT.signature() => {
                if let Ok(x) = TRANSFER_EVENT.parse_log(RawLog {
                    topics: log.topics.clone(),
                    data: log.data.0.clone(),
                }) {
                    let from = match x.params[0].value.clone() {
                        Token::Address(x) => x,
                        _ => continue,
                    };
                    let to = match x.params[1].value.clone() {
                        Token::Address(x) => x,
                        _ => continue,
                    };
                    let amount = match x.params[2].value.clone() {
                        Token::Uint(x) => x,
                        _ => continue,
                    };
                    transfers_logs.push_str(&format!(
                        "{:?},{:?},{:?},{:?},{:?}\n",
                        log.block_number.unwrap(),
                        log.address,
                        from,
                        to,
                        amount,
                    ));
                }
            }
            _ if topic == PAIR_CREATED_EVENT.signature() => {
                if let Ok(x) = PAIR_CREATED_EVENT.parse_log(RawLog {
                    topics: log.topics.clone(),
                    data: log.data.0.clone(),
                }) {
                    let token0 = match x.params[0].value.clone() {
                        Token::Address(x) => x,
                        _ => continue,
                    };
                    let token1 = match x.params[1].value.clone() {
                        Token::Address(x) => x,
                        _ => continue,
                    };
                    let pool = match x.params[2].value.clone() {
                        Token::Address(x) => x,
                        _ => continue,
                    };
                    pair_created_logs.push_str(&format!(
                        "{:?},{:?},{:?},{:?},{:?}\n",
                        log.block_number.unwrap(),
                        log.address,
                        token0,
                        token1,
                        pool
                    ));
                }
            }
            _ if topic == SYNC_EVENT.signature() => {
                if let Ok(x) = SYNC_EVENT.parse_log(RawLog {
                    topics: log.topics.clone(),
                    data: log.data.0.clone(),
                }) {
                    let reserve0 = match x.params[0].value.clone() {
                        Token::Uint(x) => x,
                        _ => continue,
                    };
                    let reserve1 = match x.params[1].value.clone() {
                        Token::Uint(x) => x,
                        _ => continue,
                    };
                    sync_logs.push_str(&format!(
                        "{:?},{:?},{:?},{:?}\n",
                        log.block_number.unwrap(),
                        log.address,
                        reserve0,
                        reserve1
                    ));
                }
            }
            _ if topic == SWAP_EVENT.signature() => {
                if let Ok(x) = SWAP_EVENT.parse_log(RawLog {
                    topics: log.topics.clone(),
                    data: log.data.0.clone(),
                }) {
                    let sender = match x.params[0].value.clone() {
                        Token::Address(x) => x,
                        _ => continue,
                    };
                    let amount0_in = match x.params[1].value.clone() {
                        Token::Uint(x) => x,
                        _ => continue,
                    };
                    let amount1_in = match x.params[2].value.clone() {
                        Token::Uint(x) => x,
                        _ => continue,
                    };
                    let amount0_out = match x.params[3].value.clone() {
                        Token::Uint(x) => x,
                        _ => continue,
                    };
                    let amount1_out = match x.params[4].value.clone() {
                        Token::Uint(x) => x,
                        _ => continue,
                    };

                    let from = match log.transaction_hash {
                        Some(hash) => match web3.eth().transaction(TransactionId::Hash(hash)).await
                        {
                            Ok(r) => match r {
                                Some(r) => r.from,
                                None => None,
                            },
                            Err(_) => None,
                        },
                        None => None,
                    };

                    swap_logs.push_str(&format!(
                        "{:?},{:?},{:?},{:?},{:?},{:?},{:?},{:?}\n",
                        log.block_number.unwrap(),
                        from,
                        log.address,
                        sender,
                        amount0_in,
                        amount0_out,
                        amount1_in,
                        amount1_out
                    ));
                }
            }
            _ if topic == MINT_EVENT.signature() => {
                if let Ok(x) = MINT_EVENT.parse_log(RawLog {
                    topics: log.topics.clone(),
                    data: log.data.0.clone(),
                }) {
                    let sender = match x.params[0].value.clone() {
                        Token::Address(x) => x,
                        _ => continue,
                    };
                    let amount0 = match x.params[1].value.clone() {
                        Token::Uint(x) => x,
                        _ => continue,
                    };
                    let amount1 = match x.params[2].value.clone() {
                        Token::Uint(x) => x,
                        _ => continue,
                    };
                    mint_logs.push_str(&format!(
                        "{:?},{:?},{:?},{:?},{:?}\n",
                        log.block_number.unwrap(),
                        log.address,
                        sender,
                        amount0,
                        amount1
                    ));
                }
            }
            _ if topic == BURN_EVENT.signature() => {
                if let Ok(x) = BURN_EVENT.parse_log(RawLog {
                    topics: log.topics.clone(),
                    data: log.data.0.clone(),
                }) {
                    let sender = match x.params[0].value.clone() {
                        Token::Address(x) => x,
                        _ => continue,
                    };
                    let amount0 = match x.params[1].value.clone() {
                        Token::Uint(x) => x,
                        _ => continue,
                    };
                    let amount1 = match x.params[2].value.clone() {
                        Token::Uint(x) => x,
                        _ => continue,
                    };
                    burn_logs.push_str(&format!(
                        "{:?},{:?},{:?},{:?},{:?}\n",
                        log.block_number.unwrap(),
                        log.address,
                        sender,
                        amount0,
                        amount1
                    ));
                }
            }
            _ => continue,
        };
    }

    (
        pair_created_logs,
        sync_logs,
        swap_logs,
        mint_logs,
        burn_logs,
        transfers_logs,
    )
}

#[async_recursion]
async fn get_logs(
    web3: Web3<Http>,
    from_block: u64,
    to_block: u64,
    filter: LocalFilter,
    semaphore: Arc<Semaphore>,
) -> Vec<Log> {
    if to_block - from_block > 1000 {
        let mid = (from_block + to_block) / 2;

        let mut a = get_logs(
            web3.clone(),
            from_block,
            mid,
            filter.clone(),
            semaphore.clone(),
        )
        .await;
        a.extend(
            get_logs(
                web3.clone(),
                mid + 1,
                to_block,
                filter.clone(),
                semaphore.clone(),
            )
            .await,
        );
        return a;
    }

    loop {
        let permit = semaphore.acquire().await.unwrap();
        let res = web3
            .eth()
            .logs(filter.get_filter(from_block, to_block))
            .await;
        drop(permit);

        match res {
            Ok(x) => return x,
            Err(x) => println!(
                "Error doing from block {} to {}: {:?}",
                from_block, to_block, x
            ),
        };
        if to_block - from_block < 100 {
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        let mid = (from_block + to_block) / 2;

        let mut a = get_logs(
            web3.clone(),
            from_block,
            mid,
            filter.clone(),
            semaphore.clone(),
        )
        .await;
        a.extend(
            get_logs(
                web3.clone(),
                mid + 1,
                to_block,
                filter.clone(),
                semaphore.clone(),
            )
            .await,
        );
        return a;
    }
}

pub async fn collect(opts: Opts) {
    let file = std::fs::File::open("input/selected_pools.csv").unwrap();
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);

    let mut addresses = HashSet::new();
    for result in rdr.deserialize() {
        let record: Address = result.unwrap();
        addresses.insert(record);
    }

    let file = std::fs::File::open("input/selected_pools_2.csv").unwrap();
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);
    for result in rdr.deserialize() {
        let record: Address = result.unwrap();
        addresses.insert(record);
    }

    let file_paths = vec![
        format!("{}/pair_created.csv", opts.path),
        format!("{}/sync.csv", opts.path),
        format!("{}/swap.csv", opts.path),
        format!("{}/mint.csv", opts.path),
        format!("{}/burn.csv", opts.path),
    ];

    let mut files = file_paths
        .iter()
        .map(|path| {
            fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(path)
                .expect("Cannot open file")
        })
        .collect::<Vec<_>>();

    let http = Http::new(&opts.rpc).expect("Cannot create http");
    let web3 = Web3::new(http);

    let local_filter = LocalFilter {
        topics: vec![
            // PAIR_CREATED_EVENT.signature(),
            // SYNC_EVENT.signature(),
            SWAP_EVENT.signature(),
            // MINT_EVENT.signature(),
            // BURN_EVENT.signature(),
        ],
    };

    let amount_block_one_iter = 500;
    let iters = (opts.to_block - opts.from_block - 1) / amount_block_one_iter + 1;

    println!(
        "{} Starting collection, total iters: {}",
        Utc::now().format("%H:%M:%S"),
        iters
    );

    for i in 0..iters {
        let from_block = opts.from_block + i * amount_block_one_iter;
        let mut to_block = from_block + amount_block_one_iter - 1;
        if to_block > opts.to_block {
            to_block = opts.to_block;
        }
        let semaphore = Arc::new(Semaphore::new(200));

        let data = get_logs(
            web3.clone(),
            from_block,
            to_block,
            local_filter.clone(),
            semaphore,
        )
        .await;

        println!("[GET]");

        let (pair_created, sync, swap, mint, burn, _) =
            convert_logs_to_string(web3.clone(), data, &addresses).await;

        files[0]
            .write_all(pair_created.as_bytes())
            .expect("Cannot write to pair_created file");
        files[1]
            .write_all(sync.as_bytes())
            .expect("Cannot write to sync file");
        files[2]
            .write_all(swap.as_bytes())
            .expect("Cannot write to swap file");
        files[3]
            .write_all(mint.as_bytes())
            .expect("Cannot write to mint file");
        files[4]
            .write_all(burn.as_bytes())
            .expect("Cannot write to burn file");

        files
            .iter_mut()
            .for_each(|f| f.flush().expect("Cannot flush file"));

        println!("{} {}/{}", Utc::now().format("%H:%M:%S"), i + 1, iters);
    }
}

pub async fn collect_transfers(opts: Opts) {
    let transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        .parse()
        .unwrap();

    let file_paths = vec![format!("{}/transfers.csv", opts.path)];

    let mut files = file_paths
        .iter()
        .map(|path| {
            fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(path)
                .expect("Cannot open file")
        })
        .collect::<Vec<_>>();

    let http = Http::new(&opts.rpc).expect("Cannot create http");
    let web3 = Web3::new(http);

    let local_filter = LocalFilter {
        topics: vec![transfer_topic],
    };

    let amount_block_one_iter = 500;
    let iters = (opts.to_block - opts.from_block - 1) / amount_block_one_iter + 1;

    println!(
        "{} Starting collection, total iters: {}",
        Utc::now().format("%H:%M:%S"),
        iters
    );

    for i in 0..iters {
        let from_block = opts.from_block + i * amount_block_one_iter;
        let mut to_block = from_block + amount_block_one_iter - 1;
        if to_block > opts.to_block {
            to_block = opts.to_block;
        }
        let semaphore = Arc::new(Semaphore::new(200));

        let data = get_logs(
            web3.clone(),
            from_block,
            to_block,
            local_filter.clone(),
            semaphore,
        )
        .await;

        let (_, _, _, _, _, transfers) = convert_logs_to_string(web3.clone(), data, &HashSet::new()).await;

        files[0]
            .write_all(transfers.as_bytes())
            .expect("Cannot write to pair_created file");

        files
            .iter_mut()
            .for_each(|f| f.flush().expect("Cannot flush file"));

        println!("{} {}/{}", Utc::now().format("%H:%M:%S"), i + 1, iters);
    }
}
