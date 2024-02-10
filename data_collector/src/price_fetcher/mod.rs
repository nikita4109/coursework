use ethabi::ethereum_types::{H160, H256};
use ethabi::{Token, Function, Contract};
use tokio::time::sleep;
use tokio::time::Duration;
use web3::contract::tokens::Tokenizable;
use web3::transports::Http;
use web3::types::{Address, BlockId, BlockNumber, Bytes, Log, U256, CallRequest};
use web3::Web3;
use std::str::FromStr;
use std::collections::{BTreeMap, HashMap};
use lazy_static::lazy_static;
use anyhow::{Result, anyhow};

use crate::trace_call::{self, trace_call_with_state_override, AccountDiff, StateDiff, TraceCallResultWithStateOverride};

lazy_static! {
    pub static ref ROUTER_ABI: Contract = {
        let abi_content = std::fs::read_to_string("abi/router.abi")
            .expect("Unable to read abi/contract.abi");
        let contract = ethabi::Contract::load(abi_content.as_bytes())
            .expect("Error parsing abi/contract.abi");
        contract
    };

    pub static ref SWAP_EXACT_ETH_FOR_TOKENS_SUPPORTING_FEE_ON_TRANSFER_TOKENS_FUNCTION: Function =
        ROUTER_ABI.function("swapExactETHForTokensSupportingFeeOnTransferTokens").unwrap().clone();
    pub static ref SWAP_EXACT_TOKENS_FOR_ETH_SUPPORTING_FEE_ON_TRANSFER_TOKENS_FUNCTION: Function =
        ROUTER_ABI.function("swapExactTokensForETHSupportingFeeOnTransferTokens").unwrap().clone();

    pub static ref ERC20_ABI: Contract = {
        let abi_content = std::fs::read_to_string("abi/erc20.abi")
        .expect("Unable to read abi/erc20.abi");
        let contract = ethabi::Contract::load(abi_content.as_bytes())
            .expect("Error parsing abi/erc20.abi");
        contract
    };
    pub static ref BALANCE_OF_FUNCTION: Function =
        ERC20_ABI.function("balanceOf").unwrap().clone();
    pub static ref TOTAL_SUPPLY_FUNCTION: Function = 
        ERC20_ABI.function("totalSupply").unwrap().clone();
    pub static ref APPROVE_FUNCTION: Function = 
        ERC20_ABI.function("approve").unwrap().clone();
    pub static ref TRANSFER_FUNCTION: Function = 
        ERC20_ABI.function("transfer").unwrap().clone();
}

fn merge_states_left_join(a: &StateDiff, b: &StateDiff) -> StateDiff {
    let mut res: StateDiff = StateDiff::new();
    for (key, value) in a {
        if let Some(other) = b.get(key) {
            let mut storage: BTreeMap<H256, U256> = value.storage.clone().unwrap_or_default();
            for (key, value) in &other.storage.clone().unwrap_or_default() {
                if !storage.contains_key(key) {
                    storage.insert(*key, *value);
                }
            }

            let diff = AccountDiff {
                balance: if value.balance.is_some() { value.balance } else { other.balance },
                nonce: if value.nonce.is_some() { value.nonce } else { other.nonce },
                code: if value.code.is_some() { value.code.clone() } else { other.code.clone() },
                storage: if storage.is_empty() { None } else { Some(storage) },
            };
            res.insert(*key, diff);
        } else {
            res.insert(*key, value.clone());
        }
    }
    for (key, value) in b {
        if !a.contains_key(key) {
            res.insert(*key, value.clone());
        }
    }

    res
}

#[derive(Clone)]
pub struct PriceFetcher {
    web3: Web3<Http>,
    owner: Address,
    router: Address,
    weth: Address,
}

impl PriceFetcher {
    pub fn new(web3: Web3<Http>, owner: Address, router: Address, weth: Address) -> PriceFetcher {
        PriceFetcher {
            web3: web3,
            owner: owner,
            router: router,
            weth: weth,
        }
    }

    fn get_infinite_eth_balance(
        &self,
        owner_address: Address,
        amount: U256,
    ) -> trace_call::StateDiff {
        let mut res: BTreeMap<Address, AccountDiff> = BTreeMap::new();
        res.insert(owner_address, AccountDiff {
            balance: Some(amount),
            nonce: None,
            code: None,
            storage: None,
        });

        res
    }

    pub async fn get_total_supply(
        &self,
        pool: Address,
        block_number: BlockNumber,
    ) -> Result<U256> {
        let input = TOTAL_SUPPLY_FUNCTION.encode_input(&vec![]).unwrap();

        let call_request = CallRequest {
            from: None,
            to: Some(pool),
            gas: Some(U256::from(1000000)),
            gas_price: None,
            value: None,
            data: Some(Bytes(input)),
            transaction_type: None,
            access_list: None,
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
        };

        let output = match self.web3.eth().call(call_request, Some(BlockId::Number(block_number))).await {
            Ok(x) => x.0,
            Err(x) => return Err(anyhow!("Can not call totalSupply for pool {:?}, err: {:?}", pool, x))
        };

        let output_tokens = match TOTAL_SUPPLY_FUNCTION.decode_output(&output) {
            Ok(x) => x,
            Err(x) => return Err(anyhow!("Can not decode total supply output: {:?}", x))
        };

        match output_tokens[0].clone() {
            Token::Uint(x) => Ok(x),
            _ => Err(anyhow!("Unreachable"))
        }
    }

    pub async fn get_balance_of_call(
        &self,
        token: Address,
        holder: Address,
        block_number: BlockNumber,
    ) -> Result<U256> {
        let input = BALANCE_OF_FUNCTION.encode_input(&vec![Token::Address(holder)]).unwrap();

        let call_request = CallRequest {
            from: None,
            to: Some(token),
            gas: Some(U256::from(1000000)),
            gas_price: None,
            value: None,
            data: Some(Bytes(input)),
            transaction_type: None,
            access_list: None,
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
        };

        let output = match self.web3.eth().call(call_request, Some(BlockId::Number(block_number))).await {
            Ok(x) => x.0,
            Err(x) => return Err(anyhow!("Can not call balanceOf for token {:?}, err: {:?}", token, x))
        };

        let output_tokens = match BALANCE_OF_FUNCTION.decode_output(&output) {
            Ok(x) => x,
            Err(x) => return  Err(anyhow!("Can not decode balanceOf: {:?}", x))
        };

        match output_tokens[0].clone() {
            Token::Uint(x) => Ok(x),
            _ => Err(anyhow!("Unreachable"))
        }
    }

    // pub async fn get_state_with_tokens(
    //     &self,
    //     token_address: Address,
    //     amount: U256,
    //     owner_address: Address,
    //     block_number: BlockNumber,
    // ) -> Result<StateDiff> {
    //     let retries = 3;

    //     let other_address = Address::from_str("0xca74f404e0c7bfa35b13b511097df966d5a65597").unwrap();
    //     let gas_price = U256::from(1000000000000u128); // 1000 gwei

    //     let data = self.build_swap_data_in(
    //         token_address,
    //         owner_address,
    //     );

    //     let data_other = self.build_swap_data_in(
    //         token_address,
    //         other_address,
    //     );

    //     let mut amount_eth = U256::from(4000000000000000000u128); // 0.4 eth

    //     let amount_eth_needed = amount_eth * (1 << retries) + U256::from(100000000000000000000u128); // max_in + 100 eth
    //     let state = self.get_infinite_eth_balance(owner_address, amount_eth_needed);
    //     let state_other = self.get_infinite_eth_balance(other_address, amount_eth_needed);

    //     for i in 0..retries {
    //         let mut state_res: BTreeMap<H256, U256> = BTreeMap::new();

    //         let params = trace_call::TraceCallParams {
    //             from: Some(owner_address),
    //             to: self.router,
    //             gas: U256::from(2000000),
    //             gas_price: gas_price,
    //             value: U256::from(amount_eth),
    //             data: data.clone(),
    //             block: block_number,
    //             state_overrides: Some(state.clone()),
    //         };

    //         let params_other = trace_call::TraceCallParams {
    //             from: Some(other_address),
    //             to: self.router,
    //             gas: U256::from(2000000),
    //             gas_price: gas_price,
    //             value: U256::from(amount_eth),
    //             data: data_other.clone(),
    //             block: block_number,
    //             state_overrides: Some(state_other.clone()),
    //         };

    //         let result = trace_call::trace_call_with_tracer(&self.web3, params.clone()).await.unwrap();

    //         if result.error.is_some() || result.revert_reason.is_some() {
    //             if i == retries - 1 {
    //                 return Err(anyhow!("Error while trace_call_with_tracer in get_state_with_tokens for token {:?}, error: {:?}, revert_reason: {:?}", token_address, result.error, result.revert_reason));
    //             }
    //             amount_eth *= 2;
    //             continue;
    //         }

    //         let state = trace_call::trace_call_with_state_override(&self.web3, params)
    //             .await
    //             .unwrap()
    //             .post
    //             .unwrap();

    //         let state_other = trace_call::trace_call_with_state_override(&self.web3, params_other)
    //             .await
    //             .unwrap()
    //             .post
    //             .unwrap();

    //         let state_other = state_other.get(&token_address).cloned().unwrap().storage.unwrap();

    //         //println!("{}\n\n\n\n", serde_json::to_string_pretty(&state).unwrap());
    //         //println!(
    //         //    "{}\n\n\n\n",
    //         //    serde_json::to_string_pretty(&state.get(&target_token).cloned().unwrap()).unwrap()
    //         //);
    //         //println!("{:?}", received);

    //         let current_balance = match self
    //             .get_balance_of(
    //                 token_address,
    //                 owner_address,
    //                 Some(state.clone()),
    //                 block_number,
    //             )
    //             .await
    //         {
    //             Ok(x) => x,
    //             Err(x) => return Err(x)
    //         };

    //         if current_balance < amount {
    //             amount_eth *= 2;
    //             continue;
    //         }

    //         //println!("{:?}", target_token);
    //         //println!("{} {:x}", current_balance, current_balance);

    //         for (key, value) in &state.get(&token_address).cloned().unwrap().storage.unwrap() {
    //             if !state_other.contains_key(&key) {
    //                 state_res.insert(*key, *value);
    //             }
    //         }
    //         if state_res.len() == 0 {
    //             return Err(anyhow!("Error in get_state_with_tokens for token {:?}: no difference between two states", token_address));
    //         }

    //         let mut res = trace_call::StateDiff::new();
    //         res.insert(
    //             token_address,
    //             AccountDiff {
    //                 balance: None,
    //                 nonce: None,
    //                 code: None,
    //                 storage: Some(state_res),
    //             },
    //         );
    //         return Ok(res);
    //     }
        
    //     return Err(anyhow!("Error in get_state_with_tokens: max retries are reached"));
    // }

    pub async fn get_state_with_tokens(
        &self,
        token_address: Address,
        pool_address: Address,
        amount: U256,
        owner_address: Address,
        block_number: BlockNumber,
    ) -> Result<StateDiff> {
        let retries = 3;

        let other_address = Address::from_str("0xca74f404e0c7bfa35b13b511097df966d5a65597").unwrap();
        let gas_price = U256::from(1000000000000u128); // 1000 gwei

        let amount_eth_needed = U256::from(100000000000000000000u128); // 100 eth
        let state = self.get_infinite_eth_balance(pool_address, amount_eth_needed);

        let mut amount_now = amount;
        for i in 0..retries {
            amount_now = amount_now * 7 / 5;
            let mut state_res: BTreeMap<H256, U256> = BTreeMap::new();

            let data = TRANSFER_FUNCTION.encode_input(&vec![Token::Address(owner_address), Token::Uint(amount_now)]).unwrap();
            let data_other = TRANSFER_FUNCTION.encode_input(&vec![Token::Address(other_address), Token::Uint(amount_now)]).unwrap();

            let params = trace_call::TraceCallParams {
                from: Some(pool_address),
                to: token_address,
                gas: U256::from(2000000),
                gas_price: gas_price,
                value: U256::from(0),
                data: Bytes(data),
                block: block_number,
                state_overrides: Some(state.clone()),
            };

            let params_other = trace_call::TraceCallParams {
                from: Some(pool_address),
                to: token_address,
                gas: U256::from(2000000),
                gas_price: gas_price,
                value: U256::from(0),
                data: Bytes(data_other),
                block: block_number,
                state_overrides: Some(state.clone()),
            };

            let result = trace_call::trace_call_with_tracer(&self.web3, params.clone()).await.unwrap();

            if result.error.is_some() || result.revert_reason.is_some() {
                if i == retries - 1 {
                    return Err(anyhow!("Error while trace_call_with_tracer in get_state_with_tokens for token {:?}, error: {:?}, revert_reason: {:?}", token_address, result.error, result.revert_reason));
                }
                continue;
            }

            let result_other = trace_call::trace_call_with_tracer(&self.web3, params_other.clone()).await.unwrap();

            if result_other.error.is_some() || result_other.revert_reason.is_some() {
                if i == retries - 1 {
                    return Err(anyhow!("Error while trace_call_with_tracer for other in get_state_with_tokens for token {:?}, error: {:?}, revert_reason: {:?}", token_address, result.error, result.revert_reason));
                }
                continue;
            }

            let state = trace_call::trace_call_with_state_override(&self.web3, params)
                .await
                .unwrap()
                .post
                .unwrap();

            
            let state_other = trace_call::trace_call_with_state_override(&self.web3, params_other)
                .await
                .unwrap()
                .post
                .unwrap();

            let state_other = match state_other.get(&token_address).cloned() {
                Some(x) => x,
                _ => return Err(anyhow!("Err in get_state_with_tokens for {:?}: other state does not contain token address", token_address))
            };
            let state_other = state_other.storage.unwrap_or_default();

            let current_balance = match self.get_balance_of(token_address, owner_address, Some(state.clone()), block_number).await {
                Ok(x) => x,
                Err(x) => return Err(anyhow!("Error while get_balance_of in get_state_with_tokens for token {:?}, error: {:?}", token_address, x))
            };

            if current_balance < amount {
                if i == retries - 1 {
                    return Err(anyhow!("Error while in get_state_with_tokens for token {:?}, insufficient balance of owner wallet: got {:?}, need {:?}", token_address, current_balance, amount));
                }
                continue;
            }

            let state = match state.get(&token_address).cloned() {
                Some(x) => x,
                _ => return Err(anyhow!("Err in get_state_with_tokens for {:?}: state does not contain token address", token_address))
            };
            let state = state.storage.unwrap_or_default();

            for (key, value) in &state {
                if !state_other.contains_key(key) {
                    state_res.insert(*key, *value);
                }
            }
            if state_res.len() == 0 {
                return Err(anyhow!("Error in get_state_with_tokens for token {:?}: no state difference between wallets", token_address));
            }

            let mut res = trace_call::StateDiff::new();
            res.insert(
                token_address,
                AccountDiff {
                    balance: None,
                    nonce: None,
                    code: None,
                    storage: Some(state_res),
                },
            );
            return Ok(res);
        }
        
        return Err(anyhow!("Error in get_state_with_tokens: max retries are reached"));
    }
    

    pub async fn get_buy_price(
        &self,
        block_number: BlockNumber,
        amount_in: U256, // amount of eths
        token_b: Address,
    ) -> Result<(U256, U256)> {
        let (amount_out, gas_used) = match self
            .complete_first_swap(amount_in, token_b, block_number, self.owner)
            .await
        {
            Ok(x) => x,
            Err(x) => return Err(x)
        };

        Ok((amount_out, gas_used))
    }

    pub async fn get_sell_price(
        &self,
        block_number: BlockNumber,
        amount_in: U256, // amount of tokens
        token_a: Address,
        pool: Address,
        state_with_tokens: Option<StateDiff>,
    ) -> Result<(U256, U256, U256)> {
        let (amount_out, gas_used_swap, gas_used_approve) = match self
            .complete_second_swap(amount_in, token_a, pool, block_number, self.owner, state_with_tokens)
            .await
        {
            Ok(x) => x,
            Err(x) => return Err(x)
        };

        Ok((amount_out, gas_used_swap, gas_used_approve))
    }

    async fn complete_first_swap(
        &self,
        amount_in: U256,
        token_b: Address,
        block_number: BlockNumber,
        owner_address: Address,
    ) -> Result<(U256, U256)> {
        let data = self.build_swap_data_in(token_b, owner_address);

        let needed_eth_balance = amount_in + U256::from(100000000000000000000u128); // amount_in + 100ETH
        let state = self.get_infinite_eth_balance(owner_address, needed_eth_balance);

        let params = trace_call::TraceCallParams {
            from: Some(owner_address),
            to: self.router,
            gas: U256::from(2000000),
            gas_price: U256::from(1000000000000u128), // 1000 gwei
            value: amount_in,
            data,
            block: block_number,
            state_overrides: Some(state),
        };

        let result = match trace_call::trace_call_with_tracer(&self.web3, params.clone()).await {
            Ok(x) => x,
            Err(x) => return Err(anyhow!("Error while trace_call_with_tracer in complete_first_swap: {:?}", x))
        };

        if result.error.is_some() || result.revert_reason.is_some() {
            return Err(anyhow!("Transaction failed while trace_call_with_tracer in complete_first_swap: error: {:?}, revert_reason: {:?}", result.error, result.revert_reason));
        }

        let state = match trace_call_with_state_override(&self.web3, params.clone()).await {
            Ok(x) => x.post.unwrap(),
            Err(x) => return Err(anyhow!("Error while trace_call_with_state_override in complete_first_swap: {:?}", x))
        };

        let amount_out = match self.get_balance_of(token_b, owner_address, Some(state), block_number).await {
            Ok(x) => x,
            Err(x) => return Err(anyhow!("Can't get_balance_of in complete_first_swap, token: {:?}, err: {:?}", token_b, x))
        };

        Ok((amount_out, result.gas_used))
    }

    async fn complete_second_swap(
        &self,
        amount_in: U256,
        token_a: Address,
        pool: Address,
        block_number: BlockNumber,
        owner_address: Address,
        state_with_tokens: Option<StateDiff>,
    ) -> Result<(U256, U256, U256)> {
        let data = self.build_swap_data_out(amount_in, token_a, owner_address);

        let gas_price = U256::from(1000000000000u128); // 1000 gwei
        let needed_eth_balance = U256::from(100000000000000000000u128); // 100ETH

        let mut state = self.get_infinite_eth_balance(owner_address, needed_eth_balance);

        state = self.approve(token_a, owner_address, self.router, amount_in, gas_price, block_number, Some(state.clone())).await?;

        let approve_gas_used = (needed_eth_balance - state.get(&owner_address).unwrap().balance.unwrap()) / gas_price;

        let state_with_tokens = match state_with_tokens {
            Some(x) => x,
            _ => self.get_state_with_tokens(token_a, pool, amount_in, owner_address, block_number).await?
        };
        state = merge_states_left_join(&state, &state_with_tokens);


        let params = trace_call::TraceCallParams {
            from: Some(owner_address),
            to: self.router,
            gas: U256::from(2000000),
            gas_price: gas_price,
            value: U256::from(0),
            data,
            block: block_number,
            state_overrides: Some(state),
        };

        let result = match trace_call::trace_call_with_tracer(&self.web3, params.clone()).await {
            Ok(x) => x,
            Err(x) => return Err(anyhow!("Error while trace_call_with_tracer in complete_second_swap: {:?}", x))
        };

        if result.error.is_some() || result.revert_reason.is_some() {
            return Err(anyhow!("Transaction failed while trace_call_with_tracer in complete_second_swap: error: {:?}, revert_reason: {:?}", result.error, result.revert_reason));
        }

        let new_state = trace_call::trace_call_with_state_override(&self.web3, params).await.unwrap().post.unwrap();

        let total_gas_used = approve_gas_used + result.gas_used;

        let new_balance = new_state.get(&owner_address).unwrap().balance.unwrap();

        let amount_out = new_balance + (total_gas_used * gas_price) - needed_eth_balance;

        Ok((amount_out, result.gas_used, approve_gas_used))
    }

    fn build_swap_data_in(
        &self,
        token_b: Address,
        owner_address: Address,
    ) -> Bytes {
        let path: Vec<Address> = vec![self.weth, token_b];
        let deadline: U256 = U256::from(std::u64::MAX);

        Bytes::from(
            SWAP_EXACT_ETH_FOR_TOKENS_SUPPORTING_FEE_ON_TRANSFER_TOKENS_FUNCTION.encode_input(&[
                U256::from(0).into_token(),
                path.into_token(),
                owner_address.into_token(),
                deadline.into_token(),
            ])
            .unwrap(),
        )
    }

    fn build_swap_data_out(
        &self,
        amount_in: U256,
        token_a: Address,
        owner_address: Address,
    ) -> Bytes {
        let path: Vec<Address> = vec![token_a, self.weth];
        let deadline: U256 = U256::from(std::u64::MAX);

        Bytes::from(
            SWAP_EXACT_TOKENS_FOR_ETH_SUPPORTING_FEE_ON_TRANSFER_TOKENS_FUNCTION.encode_input(&[
                amount_in.into_token(),
                U256::from(0).into_token(),
                path.into_token(),
                owner_address.into_token(),
                deadline.into_token(),
            ])
            .unwrap(),
        )
    }

    async fn get_balance_of(
        &self,
        token_address: Address,
        owner_address: Address,
        state_overrides: Option<trace_call::StateDiff>,
        block_number: BlockNumber,
    ) -> Result<U256> {
        let mut state = self.get_infinite_eth_balance(owner_address, U256::from(100000000000000000000u128)); // 100 ETH
        if let Some(x) = state_overrides {
            state = merge_states_left_join(&state, &x);
        }

        let input = BALANCE_OF_FUNCTION
            .encode_input(&[ethabi::Token::Address(owner_address)])
            .unwrap();

        let params = trace_call::TraceCallParams {
            from: Some(owner_address),
            to: token_address,
            gas: U256::from(2000000),
            gas_price: U256::from(1000000000000u128), // 1000 gwei
            value: U256::from(0),
            data: Bytes(input),
            block: block_number,
            state_overrides: Some(state),
        };

        let res = match trace_call::trace_call_with_tracer(&self.web3, params).await {
            Ok(x) => x,
            Err(x) => return Err(anyhow!("Can not trace_call_with_tracer in get_balance_of: {:?}", x))
        };

        if res.error.is_some() || res.revert_reason.is_some() {
            return Err(anyhow!("Can not trace_call_with_tracer in get_balance_of, error: {:?}, revert_reason: {:?}", res.error, res.revert_reason));
        }

        let res = BALANCE_OF_FUNCTION
            .decode_output(&res.output.unwrap().0)
            .unwrap();

        match res[0].clone() {
            ethabi::Token::Uint(x) => Ok(x),
            _ => Err(anyhow!("Unreachable")),
        }
    }

    async fn approve(
        &self,
        token_address: Address,
        owner_address: Address,
        approve_to_address: Address,
        amount: U256,
        gas_price: U256,
        block_number: BlockNumber,
        state: Option<StateDiff>,
    ) -> Result<StateDiff> {
        let input = APPROVE_FUNCTION
            .encode_input(&[
                ethabi::Token::Address(approve_to_address),
                ethabi::Token::Uint(amount),
            ])
            .unwrap();

        let params = trace_call::TraceCallParams {
            from: Some(owner_address),
            to: token_address,
            gas: U256::from(2000000),
            gas_price: gas_price,
            value: U256::from(0),
            data: Bytes(input),
            block: block_number,
            state_overrides: state.clone(),
        };

        let res = match trace_call::trace_call_with_state_override(&self.web3, params).await {
            Ok(x) => x,
            Err(x) => return Err(anyhow!("Error while trace_call_with_state_override in approve: {:?}", x))
        };

        Ok(merge_states_left_join(&res.post.unwrap(), &state.unwrap_or_default()))
    }
}

pub async fn get_gas_price(web3: &Web3<Http>, block_number: BlockId) -> (u64, U256) {
    let mut attempts = 0;
    const MAX_ATTEMPTS: u32 = 5; // Max number of attempts
    let delay: Duration = Duration::from_secs(1); // Delay between attempts

    while attempts < MAX_ATTEMPTS {
        match web3.eth().block(block_number).await {
            Ok(Some(block)) => {
                if block.base_fee_per_gas.is_none() {
                    attempts += 1;
                    sleep(delay * 2_u32.pow(attempts)).await;
                    continue;
                }

                return (
                    block.timestamp.as_u64(),
                    block.base_fee_per_gas.unwrap() * U256::from(15) / U256::from(10),
                );
            }
            Ok(None) => {
                attempts += 1;
                sleep(delay * 2_u32.pow(attempts)).await;
            }
            Err(_e) => {
                attempts += 1;
                sleep(delay * 2_u32.pow(attempts)).await;
            }
        }
    }

    panic!("Failed to get gas price after multiple attempts")
}

pub async fn get_tx_fee(
    web3: &Web3<Http>,
    block_number: BlockNumber,
    gas_used: U256,
) -> (u64, f64) {
    let (timestamp, gas_price) = get_gas_price(web3, BlockId::from(block_number)).await;
    let tx_fee = ((gas_price * gas_used).as_u128() as f64) / 10_f64.powf(18.0);

    (timestamp, tx_fee)
}
