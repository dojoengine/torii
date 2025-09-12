use async_graphql::dynamic::{Field, FieldFuture, InputValue, TypeRef};
use async_graphql::{Name, Value};
use starknet::core::types::Felt;
use std::str::FromStr;
use std::sync::Arc;
use torii_storage::{proto as storage_proto, ReadOnlyStorage};

use super::{BasicObject, ResolvableObject, TypeMapping};
use crate::constants::{CONTROLLER_NAMES, CONTROLLER_TYPE_NAME};
use crate::mapping::CONTROLLER_MAPPING;
use crate::utils;

#[derive(Debug)]
pub struct ControllerObject;

impl BasicObject for ControllerObject {
    fn name(&self) -> (&str, &str) {
        CONTROLLER_NAMES
    }

    fn type_name(&self) -> &str {
        CONTROLLER_TYPE_NAME
    }

    fn type_mapping(&self) -> &TypeMapping {
        &CONTROLLER_MAPPING
    }
}

impl ResolvableObject for ControllerObject {
    fn resolvers(&self) -> Vec<Field> {
        // Single controller by address
        let get_one = Field::new(self.name().0, TypeRef::named_nn(self.type_name()), |ctx| {
            FieldFuture::new(async move {
                let storage = ctx.data::<Arc<dyn ReadOnlyStorage>>()?.clone();
                let address: String = utils::extract::<String>(ctx.args.as_index_map(), "address")?;
                let addr = Felt::from_str(&address).map_err(|e| anyhow::anyhow!(e.to_string()))?;
                let query = storage_proto::ControllerQuery {
                    contract_addresses: vec![addr],
                    usernames: vec![],
                    pagination: storage_proto::Pagination { cursor: None, limit: Some(1), direction: storage_proto::PaginationDirection::Forward, order_by: vec![] },
                };
                let page = storage.controllers(&query).await.map_err(|e| anyhow::anyhow!(e.to_string()))?;
                let item = page.items.into_iter().next().ok_or_else(|| anyhow::anyhow!("Controller not found"))?;
                Ok(Some(Value::Object(value_from_controller(item))))
            })
        })
        .argument(InputValue::new("address", TypeRef::named_nn(TypeRef::ID)));

        // Many controllers with optional filters
        let get_many = Field::new(self.name().1, TypeRef::named_list(self.type_name()), |ctx| {
            FieldFuture::new(async move {
                let storage = ctx.data::<Arc<dyn ReadOnlyStorage>>()?.clone();
                let usernames = utils::extract::<Vec<String>>(ctx.args.as_index_map(), "usernames").unwrap_or_default();
                let addresses = utils::extract::<Vec<String>>(ctx.args.as_index_map(), "contractAddresses").unwrap_or_default();
                let limit = utils::extract::<u64>(ctx.args.as_index_map(), "limit").ok().map(|v| v as u32);
                let cursor = utils::extract::<String>(ctx.args.as_index_map(), "cursor").ok();

                let addrs: Vec<Felt> = addresses.into_iter().filter_map(|s| Felt::from_str(&s).ok()).collect();
                let query = storage_proto::ControllerQuery {
                    contract_addresses: addrs,
                    usernames,
                    pagination: storage_proto::Pagination { cursor, limit, direction: storage_proto::PaginationDirection::Forward, order_by: vec![] },
                };
                let page = storage.controllers(&query).await.map_err(|e| anyhow::anyhow!(e.to_string()))?;
                let list = page
                    .items
                    .into_iter()
                    .map(value_from_controller)
                    .map(Value::Object)
                    .collect::<Vec<_>>();
                Ok(Some(Value::List(list)))
            })
        })
        .argument(InputValue::new("usernames", TypeRef::named_list(TypeRef::STRING)))
        .argument(InputValue::new("contractAddresses", TypeRef::named_list(TypeRef::ID)))
        .argument(InputValue::new("limit", TypeRef::named(TypeRef::INT)))
        .argument(InputValue::new("cursor", TypeRef::named(TypeRef::STRING)));

        vec![get_one, get_many]
    }
}

fn value_from_controller(c: storage_proto::Controller) -> async_graphql::dynamic::indexmap::IndexMap<Name, Value> {
    async_graphql::dynamic::indexmap::IndexMap::from([
        (Name::new("id"), Value::from(format!("{:#x}", c.address))),
        (Name::new("username"), Value::from(c.username)),
        (Name::new("address"), Value::from(format!("{:#x}", c.address))),
        (
            Name::new("deployedAt"),
            Value::from(c.deployed_at.format(crate::constants::DATETIME_FORMAT).to_string()),
        ),
    ])
}
