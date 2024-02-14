use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::sync::Arc;
use tokio::sync::Mutex;
use web3::contract::Contract;
use web3::contract::Options;
use web3::transports::Http;
use web3::types::{Address, U256};
use web3::Web3;

use crate::PoolsCollectorArgs;

#[derive(Serialize, Deserialize, Debug)]
pub struct PoolInfo {
    pub address: Address,
    pub token0: Address,
    pub token1: Address,
}

pub struct PoolCollector {
    rpc: String,
    output_filepath: String,
}

impl PoolCollector {
    pub fn new(args: PoolsCollectorArgs) -> Self {
        PoolCollector {
            rpc: args.rpc,
            output_filepath: args.output_filepath,
        }
    }

    pub async fn collect(&self) {
        let http = Http::new(&self.rpc).expect("Can't connect to RPC");
        let web3 = Web3::new(http);

        let address: Address = "0x3a0Fa7884dD93f3cd234bBE2A0958Ef04b05E13b"
            .parse()
            .expect("Invalid address format");
        let abi = include_bytes!("../../abi/factory.abi");
        let contract = Contract::from_json(web3.eth(), address, abi)
            .expect("Failed to create contract from ABI");

        let pools_count: U256 = contract
            .query("allPairsLength", (), None, Options::default(), None)
            .await
            .expect("Invalid query for all pairs length");

        let mut handles = Vec::new();

        let semaphore = Arc::new(tokio::sync::Semaphore::new(10));
        for i in 0..pools_count.as_u64() {
            let contract = contract.clone();
            let rpc = self.rpc.clone();

            let semaphore = semaphore.clone();
            let permit = semaphore
                .acquire_owned()
                .await
                .expect("Failed to acquire semaphore permit");

            let handle = tokio::spawn(async move {
                let http = Http::new(&rpc).expect("Can't connect to RPC");
                let web3 = Web3::new(http);

                let pool: Address = contract
                    .query("allPairs", U256::from(i), None, Options::default(), None)
                    .await
                    .expect("Invalid index of pool");

                let pool_contract =
                    Contract::from_json(web3.eth(), pool, include_bytes!("../../abi/pool.abi"))
                        .expect("Failed to create pool contract from ABI");

                let token0: Address = pool_contract
                    .query("token0", (), None, Options::default(), None)
                    .await
                    .expect("Invalid query for token0 method");
                let token1: Address = pool_contract
                    .query("token1", (), None, Options::default(), None)
                    .await
                    .expect("Invalid query for token1 method");

                println!("{} / {}", i, pools_count);

                drop(permit);

                PoolInfo {
                    address: pool,
                    token0: token0,
                    token1: token1,
                }
            });

            handles.push(handle);
        }

        let pools_info = Arc::new(Mutex::new(Vec::new()));
        for handle in handles {
            match handle.await {
                Ok(pool_info) => {
                    let mut pi = pools_info.lock().await;
                    pi.push(pool_info);
                }
                Err(e) => eprintln!("Task failed with error: {:?}", e),
            }
        }

        let pools_info = Arc::try_unwrap(pools_info)
            .expect("Arc is still in use")
            .into_inner();
        let serialized = serde_json::to_string(&*pools_info).expect("Failed to serialize data");

        let file = File::create(&self.output_filepath).expect("Can't create file");
        let mut writer = BufWriter::new(file);
        writer
            .write_all(serialized.as_bytes())
            .expect("Can't write bytes to file");
    }
}
