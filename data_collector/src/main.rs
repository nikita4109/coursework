use serde::Deserialize;
use serde::Serialize;
use std::fs::File;
use std::sync::atomic;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use anyhow::{Result, anyhow};

use web3::transports::Http;

use web3::types::{Address, BlockNumber, U256};
use web3::Web3;
use std::env;

mod block_fetcher;
mod price_fetcher;
mod trace_call;
mod collector;

// #[derive(Serialize)]
// struct DealData {
//     msd_id: u32,
//     address: String,
//     timestamp: u64,
//     buy_price: f64,
//     sell_price: f64,
//     tx_fee_buy: f64,
//     tx_fee_sell: f64,
//     amount: f64,
// }

// #[derive(Debug, Deserialize, Clone)]
// struct SignalData {
//     #[serde(rename = "dateUTC")]
//     date_utc: String,
//     coin: String,
//     number_of_callers: u32,
//     dur: String,
//     msg_id: u32,
//     #[serde(rename = "type")]
//     record_type: String,
//     eth_address: String,
//     unix_time: u64,
//     chain: String,
// }

// fn save_deals(deals: Vec<DealData>) {
//     let mut wtr = csv::Writer::from_writer(File::create("data.csv").unwrap());

//     for deal in deals {
//         wtr.serialize(deal).unwrap();
//     }

//     wtr.flush().unwrap();
// }

// async fn process_signal(
//     amount_eth_in: U256,
//     amount_token_in: Option<U256>,
//     signal: SignalData,
//     block_number: BlockNumber,
//     fetcher: price_fetcher::PriceFetcher,
//     public_web3: Web3<Http>,
// ) -> Result<(DealData, U256)> {
//     let (buy_price, amount_tokens_got, gas_used_buy) = match fetcher
//         .get_buy_price(
//             block_number,
//             amount_eth_in,
//             signal.eth_address.parse().unwrap(),
//         )
//         .await
//     {
//         Ok(x) => x,
//         Err(x) => return Err(x)
//     };

//     let amount_token_in = amount_token_in.unwrap_or(amount_tokens_got);

//     let (sell_price, _, gas_used_sell) = match fetcher
//         .get_sell_price(
//             block_number,
//             amount_token_in,
//             signal.eth_address.parse().unwrap(),
//         )
//         .await
//     {
//         Ok(x) => x,
//         Err(x) => return Err(x)
//     };

//     let (timestamp, tx_fee_buy) = price_fetcher::get_tx_fee(&public_web3, block_number, gas_used_buy).await;
//     let tx_fee_sell = tx_fee_buy * (gas_used_sell.as_u64() as f64) / (gas_used_buy.as_u64() as f64);

//     let amount = (amount_eth_in.as_u128() as f64) / 10_f64.powf(18.0);

//     Ok((DealData {
//         msd_id: signal.msg_id,
//         address: signal.eth_address,
//         buy_price,
//         sell_price,
//         timestamp,
//         tx_fee_buy,
//         tx_fee_sell,
//         amount,
//     }, amount_token_in))
// }

// async fn fetch_token_prices(archive_web3: &Web3<Http>, signals: &[SignalData]) -> Vec<DealData> {
//     let price_fetcher = price_fetcher::PriceFetcher::new(archive_web3);
//     let block_fetcher = Arc::new(Mutex::new(block_fetcher::BlockFetcher::new(archive_web3)));
//     let weth_address: Address = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
//         .parse()
//         .unwrap();

//     let semaphore = Arc::new(Semaphore::new(50)); // Semaphore with 10 permits
//     let counter = Arc::new(atomic::AtomicI32::from(0));
//     let mut handles = Vec::new();

//     for signal in signals {
//         let fetcher = price_fetcher.clone();
//         let block_fetcher = block_fetcher.clone();
//         let signal = signal.clone();
//         let counter = counter.clone();
//         let semaphore = semaphore.clone();
//         let archive_web3 = archive_web3.clone();

//         let handle = tokio::spawn(async move {
//             let permit = semaphore.acquire_owned().await.unwrap();

//             let block_number = block_fetcher
//                 .clone()
//                 .lock()
//                 .await
//                 .block_number_from_timestamp(signal.unix_time)
//                 .await;
//             let amount_in = U256::from(1) * U256::exp10(17);

//             let mut deals = Vec::new();

//             let mut amount_tokens: Vec<U256> = Vec::new();

//             for i in 0..3 {
//                 match process_signal(
//                     amount_in * 2_i32.pow(i),
//                     None,
//                     signal.clone(),
//                     BlockNumber::from(block_number),
//                     fetcher.clone(),
//                     archive_web3.clone(),
//                 )
//                 .await {
//                     Ok((deal, amount_token)) => {
//                         deals.push(deal);
//                         println!("{}", counter.fetch_add(1, atomic::Ordering::Acquire));
//                         amount_tokens.push(amount_token);
//                     },
//                     Err(x) => {
//                         println!("Error: {:?}", x);
//                         return None;
//                     }
//                 };
//             }

//             for shift in 1..500 {
//                 for i in 0..3 {
//                     match process_signal(
//                         amount_in * 2_i32.pow(i),
//                         Some(amount_tokens[i as usize]),
//                         signal.clone(),
//                         BlockNumber::from(block_number + shift),
//                         fetcher.clone(),
//                         archive_web3.clone(),
//                     )
//                     .await {
//                         Ok((deal, _)) => {
//                             deals.push(deal);
//                             println!("{}", counter.fetch_add(1, atomic::Ordering::Acquire));
//                         },
//                         Err(x) => {
//                             println!("Error: {:?}", x);
//                             return None;
//                         }
//                     };
//                 }
//             }

//             drop(permit);

//             Some(deals)
//         });

//         handles.push(handle);
//     }

//     let mut total_ok = 0;
//     let total_amount = handles.len();
//     let mut deals = Vec::new();
//     for handle in handles {
//         if let Ok(Some(ds)) = handle.await {
//             deals.extend(ds);
//             total_ok += 1;
//         }
//     }

//     println!("Total ok: {}/{}", total_ok, total_amount);

//     deals
// }

// fn parse_signals(path: &str) -> Vec<SignalData> {
//     let mut rdr = csv::Reader::from_path(path).unwrap();
//     let mut signals = Vec::new();

//     for result in rdr.deserialize() {
//         let signal: SignalData = result.unwrap();
//         signals.push(signal);
//     }

//     // return signals.iter().take(10).cloned().collect();
//     signals
// }

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 7 {
        println!("Error, specify path to events file, path to calls file, timestamps cache file, output dir, rpc, amount of blocks after call");
        return;
    }

    let events_file_path = args[1].clone();
    let calls_file_path = args[2].clone();
    let timestamps_cache_file_path = args[3].clone();
    let output_dir_path = args[4].clone();
    let rpc = args[5].clone();
    let blocks_after_call = args[6].clone().parse::<u64>().expect("Can not parse amount of blocks after call");

    let opts = collector::Opts {
        events_file_path: events_file_path,
        calls_file_path: calls_file_path,
        output_dir_path: output_dir_path,
        timestamps_cache_file_path: timestamps_cache_file_path,
        rpc: rpc,
        blocks_after_call: blocks_after_call,
    };

    collector::collect(opts).await;


    // let signals = parse_signals("./calls.csv");
    // let deals = fetch_token_prices(&archive_web3, &signals).await;
    // save_deals(deals);
    // use std::str::FromStr;
    // let f = price_fetcher::PriceFetcher::new(&archive_web3);
    // f.get_state_with_tokens_and_approve(Address::from_str("0xc59e18d208fc9de263a01c6983fb32052fe21c47").unwrap(), Address::from_str("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2").unwrap(), U256::from(1000000000u128), Address::from_str("0x65a8f07bd9a8598e1b5b6c0a88f4779dbc077675").unwrap(), Address::from_str("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D").unwrap(), BlockNumber::Latest).await;
}
