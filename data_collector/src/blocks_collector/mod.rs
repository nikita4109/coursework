use crate::BlocksCollectorArgs;
use csv::Writer;
use futures::future::join_all;
use serde::Serialize;
use std::fs::File;
use std::sync::Arc;
use tokio::sync::Semaphore;
use web3::transports::Http;
use web3::types::{Block, BlockId, BlockNumber, H256};
use web3::Web3;

#[derive(Serialize)]
struct BlockRecord {
    block_number: u64,
    timestamp: u64,
    gas_price: f64,
    gas_used: u64,
}

pub async fn collect(args: BlocksCollectorArgs) {
    let http = Http::new(&args.rpc).expect("Cannot create http");
    let web3 = Web3::new(http);
    let semaphore = Arc::new(Semaphore::new(100));

    let futures = (args.start_block..args.end_block).map(|block_number| {
        let web3_clone = web3.clone();
        let semaphore_clone = semaphore.clone();
        async move {
            let permit = semaphore_clone
                .acquire_owned()
                .await
                .expect("Failed to acquire semaphore permit");

            let result = web3_clone
                .eth()
                .block(BlockId::Number(BlockNumber::Number(block_number.into())))
                .await
                .map(|block_opt| {
                    block_opt.map(|block| BlockRecord {
                        block_number: block.number.unwrap().as_u64(),
                        timestamp: block.timestamp.as_u64(),
                        gas_used: block.gas_used.as_u64(),
                        gas_price: block
                            .base_fee_per_gas
                            .map_or(0.0, |gas| (gas.as_u128() as f64) / 10_f64.powi(18)),
                    })
                });

            println!(
                "{} / {}",
                block_number - args.start_block,
                args.end_block - args.start_block
            );

            drop(permit);

            result
        }
    });

    let results = join_all(futures.collect::<Vec<_>>()).await;

    let blocks: Vec<BlockRecord> = results
        .into_iter()
        .filter_map(Result::ok)
        .filter_map(|x| x)
        .collect();

    let file = File::create(args.output_filepath).expect("Unable to create file");
    let mut wtr = Writer::from_writer(file);

    for record in blocks {
        wtr.serialize(record).expect("Unable to serialize record");
    }

    wtr.flush().expect("Unable to flush writer");
}
