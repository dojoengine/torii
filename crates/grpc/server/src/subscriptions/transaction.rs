use std::pin::Pin;

use futures::{Stream, stream};
use futures_util::StreamExt;
use torii_broker::types::TransactionUpdate;
use torii_broker::MemoryBroker;

use torii_proto::proto::world::SubscribeTransactionsResponse;
use torii_proto::Transaction;
use torii_proto::TransactionFilter;

use crate::GrpcConfig;

/// Creates a stream that subscribes to transaction updates from the broker and applies filtering
pub fn subscribe_transactions_stream(
    filter: Option<TransactionFilter>,
    config: GrpcConfig,
) -> Pin<Box<dyn Stream<Item = Result<SubscribeTransactionsResponse, tonic::Status>> + Send>> {
    let broker_stream: Pin<Box<dyn Stream<Item = Transaction> + Send>> = if config.optimistic {
        Box::pin(MemoryBroker::<TransactionUpdate>::subscribe_optimistic())
    } else {
        Box::pin(MemoryBroker::<TransactionUpdate>::subscribe())
    };

    let filtered_stream = broker_stream
        .filter_map(move |transaction: Transaction| {
            let filter = filter.clone();
            async move {
                // Apply filter if it exists
                if let Some(filter) = &filter {
                    if !filter.transaction_hashes.is_empty()
                        && !filter.transaction_hashes.contains(&transaction.transaction_hash)
                    {
                        return None;
                    }

                    if !filter.caller_addresses.is_empty() {
                        for caller_address in &transaction.calls {
                            if !filter.caller_addresses.contains(&caller_address.caller_address) {
                                return None;
                            }
                        }
                    }

                    if !filter.contract_addresses.is_empty() {
                        for contract_address in &transaction.calls {
                            if !filter.contract_addresses.contains(&contract_address.contract_address) {
                                return None;
                            }
                        }
                    }

                    if !filter.entrypoints.is_empty() {
                        for entrypoint in &transaction.calls {
                            if !filter.entrypoints.contains(&entrypoint.entrypoint) {
                                return None;
                            }
                        }
                    }

                    if !filter.model_selectors.is_empty() {
                        for model_selector in &transaction.unique_models {
                            if !filter.model_selectors.contains(model_selector) {
                                return None;
                            }
                        }
                    }

                    if filter.from_block.is_some()
                        && transaction.block_number < filter.from_block.unwrap()
                    {
                        return None;
                    }

                    if filter.to_block.is_some() && transaction.block_number > filter.to_block.unwrap()
                    {
                        return None;
                    }
                }

                Some(Ok(SubscribeTransactionsResponse {
                    transaction: Some(transaction.clone().into()),
                }))
            }
        })
        .chain(stream::once(async move {
            // Send initial empty response for firefox/safari unlock issue
            Ok(SubscribeTransactionsResponse { transaction: None })
        }));

    Box::pin(filtered_stream)
}
