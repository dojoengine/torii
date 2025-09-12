use std::str::FromStr;

use async_graphql::dynamic::{
    Field, FieldFuture, FieldValue, InputValue, SubscriptionField, SubscriptionFieldFuture, TypeRef,
};
use async_graphql::{Name, Value};
use starknet_crypto::Felt;
use tokio_stream::StreamExt;
use torii_broker::types::TransactionUpdate;
use torii_broker::MemoryBroker;
use std::sync::Arc;
use torii_storage::{proto as storage_proto, ReadOnlyStorage};

use super::{BasicObject, ResolvableObject, TypeMapping, ValueMapping};
use crate::constants::{
    CALL_TYPE_NAME, TOKEN_TRANSFER_TYPE_NAME, TRANSACTION_NAMES,
    TRANSACTION_TYPE_NAME,
};
use crate::mapping::{CALL_MAPPING, TRANSACTION_MAPPING};
use crate::utils;

#[derive(Debug)]
pub struct CallObject;

impl BasicObject for CallObject {
    fn name(&self) -> (&str, &str) {
        ("call", "calls")
    }

    fn type_name(&self) -> &str {
        CALL_TYPE_NAME
    }

    fn type_mapping(&self) -> &TypeMapping {
        &CALL_MAPPING
    }

    fn related_fields(&self) -> Option<Vec<Field>> {
        None
    }
}
#[derive(Debug)]
pub struct TransactionObject;

impl BasicObject for TransactionObject {
    fn name(&self) -> (&str, &str) {
        TRANSACTION_NAMES
    }

    fn type_name(&self) -> &str {
        TRANSACTION_TYPE_NAME
    }

    fn type_mapping(&self) -> &TypeMapping {
        &TRANSACTION_MAPPING
    }

    fn related_fields(&self) -> Option<Vec<Field>> {
        Some(vec![calls_field(), token_transfers_field()])
    }
}

impl ResolvableObject for TransactionObject {
    fn resolvers(&self) -> Vec<Field> {
        // Single transaction by hash
        let get_one = Field::new(self.name().0, TypeRef::named_nn(self.type_name()), |ctx| {
            FieldFuture::new(async move {
                let storage = ctx.data::<Arc<dyn ReadOnlyStorage>>()?.clone();
                let hash: String = utils::extract::<String>(ctx.args.as_index_map(), "hash")?;
                let filter = storage_proto::TransactionFilter {
                    transaction_hashes: vec![Felt::from_str(&hash).map_err(|e| anyhow::anyhow!(e.to_string()))?],
                    caller_addresses: vec![],
                    contract_addresses: vec![],
                    entrypoints: vec![],
                    model_selectors: vec![],
                    from_block: None,
                    to_block: None,
                };
                let query = storage_proto::TransactionQuery {
                    filter: Some(filter),
                    pagination: storage_proto::Pagination { cursor: None, limit: Some(1), direction: storage_proto::PaginationDirection::Forward, order_by: vec![] },
                };
                let page = storage.transactions(&query).await.map_err(|e| anyhow::anyhow!(e.to_string()))?;
                let tx = page.items.into_iter().next().ok_or_else(|| anyhow::anyhow!("Transaction not found"))?;
                Ok(Some(Value::Object(value_from_transaction(tx))))
            })
        })
        .argument(InputValue::new("hash", TypeRef::named_nn(TypeRef::ID)));

        // Many transactions, optional filters omitted for brevity
        let get_many = Field::new(self.name().1, TypeRef::named_list(self.type_name()), |ctx| {
            FieldFuture::new(async move {
                let storage = ctx.data::<Arc<dyn ReadOnlyStorage>>()?.clone();
                let query = storage_proto::TransactionQuery {
                    filter: None,
                    pagination: storage_proto::Pagination { cursor: None, limit: None, direction: storage_proto::PaginationDirection::Forward, order_by: vec![] },
                };
                let page = storage.transactions(&query).await.map_err(|e| anyhow::anyhow!(e.to_string()))?;
                let list = page.items.into_iter().map(|t| Value::Object(value_from_transaction(t))).collect::<Vec<_>>();
                Ok(Some(Value::List(list)))
            })
        });

        vec![get_one, get_many]
    }

    fn subscriptions(&self) -> Option<Vec<SubscriptionField>> {
        Some(vec![SubscriptionField::new(
            "transaction",
            TypeRef::named_nn(self.type_name()),
            |ctx| {
                SubscriptionFieldFuture::new(async move {
                    let hash = match ctx.args.get("hash") {
                        Some(hash) => Some(hash.string()?.to_string()),
                        None => None,
                    };

                    let caller = match ctx.args.get("hasCaller") {
                        Some(caller) => Some(caller.string()?.to_string()),
                        None => None,
                    };

                    // if hash is None, then subscribe to all transactions
                    // if hash is Some, then subscribe to only the transaction with that hash
                    Ok(MemoryBroker::<TransactionUpdate>::subscribe()
                        .then(move |transaction| {
                            let hash = hash.clone();
                            let caller = caller.clone();
                            async move {
                                let transaction_hash =
                                    format!("{:#x}", transaction.transaction_hash);
                                if (hash.is_none() || hash == Some(transaction_hash.clone()))
                                    && (caller.is_none()
                                        || transaction.calls.iter().any(|call| {
                                            call.caller_address
                                                == Felt::from_str(&caller.clone().unwrap()).unwrap()
                                        }))
                                {
                                    Some(Ok(Value::Object(value_from_transaction(transaction))))
                                } else {
                                    None
                                }
                            }
                        })
                        .filter_map(|result| result))
                })
            },
        )
        .argument(InputValue::new("hash", TypeRef::named(TypeRef::ID)))
        .argument(InputValue::new(
            "hasCaller",
            TypeRef::named(TypeRef::STRING),
        ))])
    }
}

impl TransactionObject {}

fn value_from_transaction(transaction: storage_proto::Transaction) -> ValueMapping {
    async_graphql::dynamic::indexmap::IndexMap::from([
        (
            Name::new("transactionHash"),
            Value::from(format!("{:#x}", transaction.transaction_hash)),
        ),
        (
            Name::new("senderAddress"),
            Value::from(format!("{:#x}", transaction.sender_address)),
        ),
        (
            Name::new("calldata"),
            Value::from(
                transaction
                    .calldata
                    .into_iter()
                    .map(|f| format!("{:#x}", f))
                    .collect::<Vec<_>>()
            ),
        ),
        (Name::new("maxFee"), Value::from(format!("{:#x}", transaction.max_fee))),
        (
            Name::new("signature"),
            Value::from(
                transaction
                    .signature
                    .into_iter()
                    .map(|f| format!("{:#x}", f))
                    .collect::<Vec<_>>()
            ),
        ),
        (Name::new("nonce"), Value::from(format!("{:#x}", transaction.nonce))),
        (Name::new("blockNumber"), Value::from(transaction.block_number as i64)),
        (
            Name::new("transactionType"),
            Value::from(transaction.transaction_type),
        ),
    ])
}

fn calls_field() -> Field {
    Field::new("calls", TypeRef::named_list(CALL_TYPE_NAME), move |ctx| {
        FieldFuture::new(async move {
            match ctx.parent_value.try_to_value()? {
                Value::Object(_indexmap) => {
                    // Calls can be exposed later via storage if needed
                    Ok(Some(Value::List(vec![])))
                }
                _ => Err("incorrect value, requires Value::Object".into()),
            }
        })
    })
}

fn token_transfers_field() -> Field {
    Field::new(
        "tokenTransfers",
        TypeRef::named_list(TOKEN_TRANSFER_TYPE_NAME),
        move |ctx| {
            FieldFuture::new(async move {
                match ctx.parent_value.try_to_value()? {
                    Value::Object(_indexmap) => {
                        let empty: Vec<FieldValue<'_>> = Vec::new();
                        Ok(Some(FieldValue::list(empty)))
                    }
                    _ => Err("incorrect value, requires Value::Object".into()),
                }
            })
        },
    )
}
