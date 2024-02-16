mod types;

use self::types::{CEXData, Tick};
use crate::logs_processor::types::CEXRecord;
use crate::pools_collector::PoolInfo;
use crate::LogsProcessorArgs;
use csv::Reader;
use csv::Writer;
use std::collections::HashMap;
use std::io::{BufReader, Read};
use std::path::Path;
use std::{fs::File, io::BufRead};
use types::{parse_event, Event};
use web3::types::Address;

pub struct LogsProcessor {
    events: Vec<Event>,
    cex_data: Vec<CEXData>,
    pools: HashMap<Address, PoolInfo>,
}

impl LogsProcessor {
    pub fn new(args: LogsProcessorArgs) -> Self {
        LogsProcessor {
            events: LogsProcessor::read_logs_csv(&args.logs_path),
            cex_data: LogsProcessor::read_cex_data_csv(&args.cex_data_path),
            pools: LogsProcessor::read_pools(&args.pools_path),
        }
    }

    fn read_logs_csv(path: &str) -> Vec<Event> {
        let mut events = Vec::new();

        let file = File::open(Path::new(path)).expect("invalid logs csv path");

        let reader = BufReader::new(file);

        for line in reader.lines() {
            match line {
                Ok(content) => {
                    let args = content.split(',').map(|s| s.to_string()).collect();
                    let event = parse_event(args);
                    events.push(event)
                }
                Err(e) => {
                    panic!("Error reading line: {}", e);
                }
            }
        }

        return events;
    }

    fn read_cex_data_csv(path: &str) -> Vec<CEXData> {
        let mut data = Vec::new();

        let mut rdr = Reader::from_path(path).expect("can't read CEX csv");
        for result in rdr.deserialize() {
            let record: CEXRecord = result.unwrap();
            if record.platform_slug != "ethereum" {
                continue;
            }

            data.push(CEXData {
                address: record.token_adress.parse().unwrap(),
                token_symbol: record.symbol,
            });
        }

        return data;
    }

    fn read_pools(path: &str) -> HashMap<Address, PoolInfo> {
        let mut file = File::open(path).expect("invalid path");
        let mut content = String::new();
        file.read_to_string(&mut content)
            .expect("can't read file with pools");

        let vec: Vec<PoolInfo> = serde_json::from_str(&content).expect("invalid pools json");
        let mut pools = HashMap::new();

        for pool in vec {
            pools.insert(pool.address, pool);
        }

        return pools;
    }

    pub fn write_csv(&self, path: &str) {
        let mut token_address_to_symbol = HashMap::new();
        for cex_record in &self.cex_data {
            token_address_to_symbol.insert(cex_record.address, cex_record.token_symbol.clone());
        }

        let mut pool_address_to_symbol = HashMap::new();
        for (address, pool_info) in &self.pools {
            if let Some(token_symbol) = token_address_to_symbol.get(&pool_info.token0) {
                pool_address_to_symbol.insert(address, token_symbol.clone());
            }

            if let Some(token_symbol) = token_address_to_symbol.get(&pool_info.token1) {
                pool_address_to_symbol.insert(address, token_symbol.clone());
            }
        }

        let mut tickers = Vec::new();
        for event in &self.events {
            let token_symbol = match event {
                Event::Sync(event) => pool_address_to_symbol.get(&event.address),
                Event::Swap(event) => pool_address_to_symbol.get(&event.address),
                Event::Burn(event) => pool_address_to_symbol.get(&event.address),
                Event::Mint(event) => pool_address_to_symbol.get(&event.address),
            };

            if let Some(token_symbol) = token_symbol {
                tickers.push(Tick {
                    event: event.clone(),
                    token_symbol: token_symbol.clone(),
                });
            }
        }

        let file = File::create(path).unwrap();
        let mut wtr = Writer::from_writer(file);

        for record in tickers {
            wtr.serialize(record).unwrap();
        }

        wtr.flush().unwrap();
    }
}
