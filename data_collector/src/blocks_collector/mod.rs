use csv::Writer;
use serde::Serialize;
use std::fs::File;
use web3::transports::Http;
use web3::types::{Block, BlockId, BlockNumber, H256};
use web3::Web3;

use crate::BlocksCollectorArgs;

#[derive(Serialize)]
struct BlockRecord {
    block_number: u64,
    timestamp: u64,
    gas_price: f64,
    gas_used: u64,
}

pub async fn collect(args: BlocksCollectorArgs) {
    let http = Http::new(&args.rpc).expect("Can not create http");
    let web3 = Web3::new(http);

    let mut blocks = Vec::new();
    for block_number in args.start_block..args.end_block {
        let block = web3
            .eth()
            .block(BlockId::Number(BlockNumber::Number(block_number.into())))
            .await
            .unwrap()
            .unwrap();

        blocks.push(BlockRecord {
            block_number: block.number.unwrap().as_u64(),
            timestamp: block.timestamp.as_u64(),
            gas_used: block.gas_used.as_u64(),
            gas_price: (block.base_fee_per_gas.unwrap().as_u128() as f64) / 10_f64.powf(18_f64),
        });
    }

    println!("[Blocks collected]");

    let file = File::create(args.output_filepath).unwrap();
    let mut wtr = Writer::from_writer(file);

    for record in blocks {
        wtr.serialize(record).unwrap();
    }

    wtr.flush().unwrap();
}
