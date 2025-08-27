use std::collections::HashSet;
use std::pin::Pin;

use crypto_bigint::U256;
use futures::{Stream, stream};
use futures_util::StreamExt;
use rand::Rng;
use starknet_crypto::Felt;
use torii_broker::types::TokenUpdate;
use torii_broker::MemoryBroker;

use torii_proto::proto::world::SubscribeTokensResponse;
use torii_proto::Token;

use crate::GrpcConfig;

/// Creates a stream that subscribes to token updates from the broker and applies filtering
pub fn subscribe_tokens_stream(
    contract_addresses: Vec<Felt>,
    token_ids: Vec<U256>,
    config: GrpcConfig,
) -> Pin<Box<dyn Stream<Item = Result<SubscribeTokensResponse, tonic::Status>> + Send>> {
    let subscription_id = rand::thread_rng().gen::<u64>();
    let contract_addresses_set: HashSet<Felt> = contract_addresses.into_iter().collect();
    let token_ids_set: HashSet<U256> = token_ids.into_iter().collect();
    
    let broker_stream: Pin<Box<dyn Stream<Item = Token> + Send>> = if config.optimistic {
        Box::pin(MemoryBroker::<TokenUpdate>::subscribe_optimistic())
    } else {
        Box::pin(MemoryBroker::<TokenUpdate>::subscribe())
    };

    let filtered_stream = broker_stream
        .filter_map(move |token: Token| {
            let contract_addresses_filter = contract_addresses_set.clone();
            let token_ids_filter = token_ids_set.clone();
            async move {
                // Apply contract address filter
                if !contract_addresses_filter.is_empty()
                    && !contract_addresses_filter.contains(&token.contract_address)
                {
                    return None;
                }

                // Apply token ID filter
                if !token_ids_filter.is_empty()
                    && token.token_id.is_some()
                    && !token_ids_filter.contains(&token.token_id.unwrap())
                {
                    return None;
                }

                Some(Ok(SubscribeTokensResponse {
                    subscription_id,
                    token: Some(token.clone().into()),
                }))
            }
        })
        .chain(stream::once(async move {
            // Send initial empty response for firefox/safari unlock issue
            Ok(SubscribeTokensResponse {
                subscription_id,
                token: None,
            })
        }));

    Box::pin(filtered_stream)
}
