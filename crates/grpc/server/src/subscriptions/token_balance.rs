use std::collections::HashSet;
use std::pin::Pin;

use crypto_bigint::U256;
use futures::{Stream, stream};
use futures_util::StreamExt;
use rand::Rng;
use starknet_crypto::Felt;
use torii_broker::types::TokenBalanceUpdate;
use torii_broker::MemoryBroker;
use torii_proto::TokenBalance;

use torii_proto::proto::world::SubscribeTokenBalancesResponse;

use crate::GrpcConfig;

/// Creates a stream that subscribes to token balance updates from the broker and applies filtering
pub fn subscribe_token_balances_stream(
    contract_addresses: Vec<Felt>,
    account_addresses: Vec<Felt>,
    token_ids: Vec<U256>,
    config: GrpcConfig,
) -> Pin<Box<dyn Stream<Item = Result<SubscribeTokenBalancesResponse, tonic::Status>> + Send>> {
    let subscription_id = rand::thread_rng().gen::<u64>();
    let contract_addresses_set: HashSet<Felt> = contract_addresses.into_iter().collect();
    let account_addresses_set: HashSet<Felt> = account_addresses.into_iter().collect();
    let token_ids_set: HashSet<U256> = token_ids.into_iter().collect();
    
    let broker_stream: Pin<Box<dyn Stream<Item = TokenBalance> + Send>> = if config.optimistic {
        Box::pin(MemoryBroker::<TokenBalanceUpdate>::subscribe_optimistic())
    } else {
        Box::pin(MemoryBroker::<TokenBalanceUpdate>::subscribe())
    };

    let filtered_stream = broker_stream
        .filter_map(move |balance: TokenBalance| {
            let contract_addresses_filter = contract_addresses_set.clone();
            let account_addresses_filter = account_addresses_set.clone();
            let token_ids_filter = token_ids_set.clone();
            async move {
                // Apply contract address filter
                if !contract_addresses_filter.is_empty()
                    && !contract_addresses_filter.contains(&balance.contract_address)
                {
                    return None;
                }

                // Apply account address filter
                if !account_addresses_filter.is_empty()
                    && !account_addresses_filter.contains(&balance.account_address)
                {
                    return None;
                }

                // Apply token ID filter
                if !token_ids_filter.is_empty()
                    && balance.token_id.is_some()
                    && !token_ids_filter.contains(&balance.token_id.unwrap())
                {
                    return None;
                }

                Some(Ok(SubscribeTokenBalancesResponse {
                    subscription_id,
                    balance: Some(balance.clone().into()),
                }))
            }
        })
        .chain(stream::once(async move {
            // Send initial empty response for firefox/safari unlock issue
            Ok(SubscribeTokenBalancesResponse {
                subscription_id,
                balance: None,
            })
        }));

    Box::pin(filtered_stream)
}
