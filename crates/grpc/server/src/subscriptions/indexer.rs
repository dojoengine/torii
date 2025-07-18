use std::pin::Pin;
use std::sync::Arc;

use futures::{Stream, stream};
use futures_util::StreamExt;
use starknet::core::types::Felt;
use torii_broker::types::ContractUpdate;
use torii_broker::MemoryBroker;
use torii_proto::ContractCursor;
use torii_storage::Storage;
use torii_storage::StorageError;

use torii_proto::proto::world::SubscribeIndexerResponse;

use crate::GrpcConfig;

/// Creates a stream that subscribes to indexer updates from the broker and applies filtering
pub async fn subscribe_indexer_stream(
    storage: Arc<dyn Storage>,
    contract_address: Felt,
    config: GrpcConfig,
) -> Result<Pin<Box<dyn Stream<Item = Result<SubscribeIndexerResponse, tonic::Status>> + Send>>, StorageError> {
    let broker_stream: Pin<Box<dyn Stream<Item = ContractCursor> + Send>> = if config.optimistic {
        Box::pin(MemoryBroker::<ContractUpdate>::subscribe_optimistic())
    } else {
        Box::pin(MemoryBroker::<ContractUpdate>::subscribe())
    };

    // Get initial contract cursors
    let contracts = storage.cursors().await?;
    let initial_responses: Vec<Result<SubscribeIndexerResponse, tonic::Status>> = contracts
        .into_iter()
        .map(|(contract_addr, contract)| {
            Ok(SubscribeIndexerResponse {
                head: contract.head.unwrap() as i64,
                tps: contract.tps.unwrap() as i64,
                last_block_timestamp: contract.last_block_timestamp.unwrap() as i64,
                contract_address: contract_addr.to_bytes_be().to_vec(),
            })
        })
        .collect();

    let filtered_stream = broker_stream
        .filter_map(move |contract: ContractCursor| {
            let contract_address_filter = contract_address;
            async move {
                // Apply contract address filter
                if contract_address_filter != Felt::ZERO
                    && contract_address_filter != contract.contract_address
                {
                    return None;
                }

                Some(Ok(SubscribeIndexerResponse {
                    head: contract.head.unwrap() as i64,
                    tps: contract.tps.unwrap() as i64,
                    last_block_timestamp: contract.last_block_timestamp.unwrap() as i64,
                    contract_address: contract.contract_address.to_bytes_be().to_vec(),
                }))
            }
        })
        .chain(stream::iter(initial_responses.into_iter()));

    Ok(Box::pin(filtered_stream))
}
