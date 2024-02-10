use std::collections::HashMap;

use web3::transports::Http;
use web3::types::{BlockId, BlockNumber};
use web3::Web3;

pub struct BlockFetcher {
    web3: Web3<Http>,
    cache: HashMap<u64, BlockData>,
}

#[derive(Clone)]
struct BlockData {
    block_number: u64,
    timestamp: u64,
}

impl BlockFetcher {
    pub fn new(web3: &Web3<Http>) -> BlockFetcher {
        BlockFetcher {
            web3: web3.clone(),
            cache: HashMap::new(),
        }
    }

    pub async fn block_number_from_timestamp(&mut self, timestamp: u64) -> u64 {
        let mut left = self.get_block(17790607).await;
        let mut right = self.get_block(19039335).await;

        while right.block_number - left.block_number > 1 {
            let mid = (left.block_number + right.block_number) / 2;
            let block_data = self.get_block(mid).await;
            if block_data.timestamp <= timestamp {
                left = block_data;
            } else {
                right = block_data;
            }
        }

        left.block_number
    }

    async fn get_block(&mut self, block_number: u64) -> BlockData {
        if let Some(block_data) = self.cache.get(&block_number) {
            return block_data.clone();
        };

        let block = self
            .web3
            .eth()
            .block(BlockId::Number(BlockNumber::from(block_number)))
            .await
            .unwrap()
            .unwrap();

        let block_data = BlockData {
            block_number: block.number.unwrap().as_u64(),
            timestamp: block.timestamp.as_u64(),
        };

        self.cache.insert(block_number, block_data.clone());

        block_data
    }
}
