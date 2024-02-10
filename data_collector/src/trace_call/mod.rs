use std::collections::BTreeMap;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use web3::types::H256;
use web3::types::{Address, BlockNumber, Bytes, CallRequest, Log, H160, U256, U64};
use web3::Transport;
use web3::Web3;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AccountDiff {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub balance: Option<U256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonce: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage: Option<BTreeMap<H256, U256>>,
}

pub type StateDiff = BTreeMap<H160, AccountDiff>;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct _AccoundDiff {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub balance: Option<U256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonce: Option<U64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<Bytes>,
    #[serde(rename = "stateDiff", skip_serializing_if = "Option::is_none")]
    pub state_dff: Option<BTreeMap<H256, U256>>,
}

type _StateDiff = BTreeMap<H160, _AccoundDiff>;

fn convert_state_diff(state_diff: StateDiff) -> _StateDiff {
    state_diff
        .into_iter()
        .map(|(key, b)| {
            let nonce = b.nonce.map(U64::from);
            (
                key,
                _AccoundDiff {
                    balance: b.balance,
                    code: b.code,
                    state_dff: b.storage,
                    nonce,
                },
            )
        })
        .collect()
}

#[derive(Debug, Clone)]
pub struct TraceCallParams {
    pub from: Option<Address>,
    pub to: Address,
    pub gas: U256,
    pub gas_price: U256,
    pub value: U256,
    pub data: Bytes,
    pub block: BlockNumber,
    pub state_overrides: Option<StateDiff>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct TraceCallResultWithTracer {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<Address>,
    pub to: Address,
    pub gas: U256,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logs: Option<Vec<Log>>,
    #[serde(rename = "gasUsed")]
    pub gas_used: U256,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<U256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub calls: Option<Vec<TraceCallResultWithTracer>>,
    #[serde(rename = "revertReason", skip_serializing_if = "Option::is_none")]
    pub revert_reason: Option<String>,
}

pub fn get_logs(result: &TraceCallResultWithTracer) -> Vec<Log> {
    let mut res: Vec<Log> = Vec::new();
    if let Some(calls) = result.calls.as_ref() {
        for call in calls {
            res.extend(get_logs(call));
        }
    }
    res.extend(result.logs.clone().unwrap_or_default());

    res
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TraceCallResultWithStateOverride {
    #[serde(rename = "post", skip_serializing_if = "Option::is_none")]
    pub post: Option<StateDiff>,
}

pub async fn trace_call_with_tracer<T: Transport>(
    web3: &Web3<T>,
    params: TraceCallParams,
) -> Result<TraceCallResultWithTracer> {
    let call_request = CallRequest {
        from: params.from,
        to: Some(params.to),
        gas: Some(params.gas),
        gas_price: Some(params.gas_price),
        value: Some(params.value),
        data: Some(params.data),
        transaction_type: None,
        access_list: None,
        max_fee_per_gas: None,
        max_priority_fee_per_gas: None,
    };

    let block = match params.block {
        BlockNumber::Number(num) => format!("0x{:x}", num.as_u64()),
        BlockNumber::Latest => String::from("latest"),
        _ => return Err(anyhow::anyhow!("Block type is not supported")),
    };

    let state = params.state_overrides.map(convert_state_diff);

    let request = vec![
        json!(call_request),
        json!(block),
        json!({
            "tracer": "callTracer",
            "tracerConfig": {
                "onlyTopCall": false,
                "withLog": true,
            },
            "stateOverrides": state,
        }),
    ];

    let result: Value = web3.transport().execute("debug_traceCall", request).await?;

    let result: Result<TraceCallResultWithTracer, serde_json::Error> =
        serde_json::from_value(result);

    match result {
        Ok(result) => Ok(result),
        Err(err) => Err(anyhow::anyhow!(err.to_string())),
    }
}

pub async fn trace_call_with_state_override<T: Transport>(
    web3: &Web3<T>,
    params: TraceCallParams,
) -> Result<TraceCallResultWithStateOverride> {
    let call_request = CallRequest {
        from: params.from,
        to: Some(params.to),
        gas: Some(params.gas),
        gas_price: Some(params.gas_price),
        value: Some(params.value),
        data: Some(params.data),
        transaction_type: None,
        access_list: None,
        max_fee_per_gas: None,
        max_priority_fee_per_gas: None,
    };

    let block = match params.block {
        BlockNumber::Number(num) => format!("0x{:x}", num.as_u64()),
        BlockNumber::Latest => String::from("latest"),
        _ => return Err(anyhow::anyhow!("Block type is not supported")),
    };

    let state = params.state_overrides.map(convert_state_diff);

    let request = vec![
        json!(call_request),
        json!(block),
        json!({
            "tracer": "prestateTracer",
            "tracerConfig": {
                "diffMode": true,
            },
            "stateOverrides": state,
        }),
    ];

    let result: Value = web3.transport().execute("debug_traceCall", request).await?;

    let result: Result<TraceCallResultWithStateOverride, serde_json::Error> =
        serde_json::from_value(result);

    match result {
        Ok(result) => Ok(result),
        Err(err) => Err(anyhow::anyhow!(err.to_string())),
    }
}
