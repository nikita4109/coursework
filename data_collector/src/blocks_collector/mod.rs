use crate::db::db::{establish_connection, insert_multiple_data, load_data};
use crate::db::models::BlockRecord;
use crate::BlocksCollectorArgs;
use diesel::PgConnection;
use futures::future::join_all;
use std::sync::Arc;
use tokio::sync::Semaphore;
use web3::transports::Http;
use web3::types::{BlockId, BlockNumber};
use web3::Web3;

pub async fn collect(conn: &PgConnection, args: BlocksCollectorArgs) {
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
                        id: 0, // Это значение будет игнорироваться при вставке в базу данных из-за SERIAL
                        block_number: block.number.unwrap().as_u64() as i64,
                        timestamp: block.timestamp.as_u64() as i64,
                        gas_used: block.gas_used.as_u64() as i64,
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

    insert_multiple_data(conn, blocks);
}
