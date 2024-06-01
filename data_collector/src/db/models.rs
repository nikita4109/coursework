use super::schema::{blocks, cex_data, logs, pools};
use serde::{Deserialize, Serialize};

#[derive(Queryable, Insertable, Serialize, Deserialize)]
#[table_name = "blocks"]
pub struct BlockRecord {
    pub id: i32,
    pub block_number: i64,
    pub timestamp: i64,
    pub gas_price: f64,
    pub gas_used: i64,
}

#[derive(Queryable, Insertable, Serialize, Deserialize)]
#[table_name = "logs"]
pub struct LogRecord {
    pub id: i32,
    pub log_type: i32,
    pub block_number: i64,
    pub address: String,
    pub data1: Option<String>,
    pub data2: Option<String>,
    pub data3: Option<String>,
    pub data4: Option<String>,
    pub data5: Option<String>,
}

#[derive(Queryable, Insertable, Serialize, Deserialize)]
#[table_name = "cex_data"]
pub struct CEXData {
    pub id: i32,
    pub platform_slug: String,
    pub token_address: String,
    pub symbol: String,
}

#[derive(Queryable, Insertable, Serialize, Deserialize)]
#[table_name = "pools"]
pub struct PoolInfo {
    pub id: i32,
    pub address: String,
    pub token0: String,
    pub token1: String,
}
