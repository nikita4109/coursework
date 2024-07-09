use csv::Reader;
use futures::future::join_all;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::{self, Duration};
use web3::contract::{Contract, Options};
use web3::transports::Http;
use web3::types::{Address, U256};
use web3::Web3;

use crate::TokenMetaCollectorArgs;

#[derive(Debug, Deserialize)]
struct PairCreated {
    token0: Address,
    token1: Address,
}

#[derive(Debug, Serialize)]
struct TokenMeta {
    token: String,
    decimals: Option<u8>,
    supply: Option<String>,
    symbol: Option<String>,
}

async fn get_token_meta(web3: &Web3<Http>, erc20_abi: &str, token_address: Address) -> TokenMeta {
    let contract = Contract::from_json(web3.eth(), token_address, erc20_abi.as_bytes()).unwrap();
    let mut meta_info = TokenMeta {
        token: format!("{:?}", token_address),
        decimals: None,
        supply: None,
        symbol: None,
    };

    let decimals: Result<U256, _> = contract
        .query("decimals", (), None, Options::default(), None)
        .await;
    let supply: Result<U256, _> = contract
        .query("totalSupply", (), None, Options::default(), None)
        .await;
    let symbol: Result<String, _> = contract
        .query("symbol", (), None, Options::default(), None)
        .await;

    match decimals {
        Ok(d) => meta_info.decimals = Some(d.as_u64() as u8),
        Err(e) => println!("{:?}", e),
    }
    match supply {
        Ok(s) => meta_info.supply = Some(format!("{:?}", s)),
        Err(e) => println!("{:?}", e),
    }
    match symbol {
        Ok(s) => meta_info.symbol = Some(s),
        Err(e) => println!("{:?}", e),
    }

    meta_info
}

pub async fn collect_token_meta(args: TokenMetaCollectorArgs) {
    let transport = Http::new(&args.rpc).unwrap();
    let web3 = Web3::new(transport);

    let file_1 = File::open("../data/pair_created.csv").unwrap();
    let mut rdr_1 = Reader::from_reader(file_1);
    let file_2 = File::open("../data/pair_created_2.csv").unwrap();
    let mut rdr_2 = Reader::from_reader(file_2);

    let tokens_1: HashSet<Address> = rdr_1
        .deserialize()
        .map(|result: Result<PairCreated, _>| {
            let record = result.unwrap();
            vec![record.token0, record.token1]
        })
        .flatten()
        .collect();

    let tokens_2: HashSet<Address> = rdr_2
        .deserialize()
        .map(|result: Result<PairCreated, _>| {
            let record = result.unwrap();
            vec![record.token0, record.token1]
        })
        .flatten()
        .collect();

    let all_tokens: Vec<_> = tokens_1.union(&tokens_2).cloned().collect();

    let erc20_abi = include_str!("../../abi/erc20.abi");

    let pb = ProgressBar::new(all_tokens.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})",
            )
            .unwrap()
            .progress_chars("#>-"),
    );

    let max_requests_per_second = 3;
    let semaphore = Arc::new(Semaphore::new(max_requests_per_second)); // Limit to max_requests_per_second concurrent requests
    let mut interval = time::interval(Duration::from_secs(1));

    let mut token_meta_data = Vec::new();

    for chunk in all_tokens.chunks(max_requests_per_second) {
        interval.tick().await;
        let mut tasks = Vec::new();
        for &token in chunk {
            let web3 = web3.clone();
            let erc20_abi = erc20_abi.clone();
            let pb = pb.clone();
            let semaphore = semaphore.clone();
            tasks.push(tokio::spawn(async move {
                let permit = semaphore.acquire().await.unwrap(); // Acquire semaphore permit
                let result = get_token_meta(&web3, &erc20_abi, token).await;
                pb.inc(1);
                drop(permit); // Release semaphore permit
                result
            }));
        }

        let results = join_all(tasks).await;
        for result in results {
            match result {
                Ok(r) => token_meta_data.push(r),
                Err(e) => println!("{:?}", e),
            }
        }
    }

    pb.finish_with_message("Collection complete.");

    let file = File::create("token_meta_data.csv").unwrap();
    let mut wtr = csv::Writer::from_writer(file);
    for meta in token_meta_data {
        wtr.serialize(meta).unwrap();
    }
    wtr.flush().unwrap();
}
