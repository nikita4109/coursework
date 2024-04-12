use anyhow::{anyhow, Result};
use csv::Writer;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Clone, Serialize)]
struct Trade {
    id: u64,
    price: String,
    qty: String,
    #[serde(rename = "quoteQty")]
    quote_qty: String,
    time: u64,
    #[serde(rename = "isBuyerMaker")]
    is_buyer_maker: bool,
    #[serde(rename = "isBestMatch")]
    is_best_match: bool,
}

async fn get_historical_trades(
    symbol: &str,
    limit: u32,
    from_id: Option<u64>,
) -> Result<Vec<Trade>> {
    let url = "https://api.binance.com/api/v3/historicalTrades";
    let limit = limit.to_string();
    let mut params = vec![
        ("symbol".to_string(), symbol.to_string()),
        ("limit".to_string(), limit.to_string()),
    ];

    if let Some(id) = from_id {
        params.push(("fromId".to_string(), id.to_string()));
    }

    let client = reqwest::Client::new();
    let response = client.get(url).query(&params).send().await?;

    if response.status().is_success() {
        let trades: Vec<Trade> = response.json().await?;
        Ok(trades)
    } else {
        Err(anyhow!(response.status()))
    }
}

pub async fn fetch_all_trades(output_filepath: &str, symbol: &str) -> Result<()> {
    let mut all_trades = Vec::new();
    let mut from_id = None;
    let limit = 1000;

    loop {
        let trades = get_historical_trades(symbol, limit, from_id).await?;
        all_trades.extend(trades.clone());

        if trades.len() < limit as usize {
            break;
        }

        from_id = Some(trades.first().unwrap().id);
    }

    let mut wtr = Writer::from_path(output_filepath)?;
    for trade in all_trades {
        wtr.serialize(trade)?;
    }
    wtr.flush()?;

    Ok(())
}
