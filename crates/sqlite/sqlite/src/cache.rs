use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use dojo_types::schema::Ty;
use dojo_world::contracts::abigen::model::Layout;
use sqlx::{Pool, Sqlite, SqlitePool};
use starknet::core::types::contract::AbiEntry;
use starknet::core::types::{
    BlockId, BlockTag, ContractClass, EntryPointsByType, LegacyContractAbiEntry, StarknetError,
};
use starknet::core::utils::get_selector_from_name;
use starknet::providers::{Provider, ProviderError};
use starknet_crypto::Felt;
use tokio::sync::{Mutex, RwLock};

use crate::constants::TOKENS_TABLE;
use crate::error::{Error, ParseError};
use crate::utils::I256;

#[derive(Debug, Clone)]
pub struct Model {
    /// Namespace of the model
    pub namespace: String,
    /// The name of the model
    pub name: String,
    /// The selector of the model
    pub selector: Felt,
    /// The class hash of the model
    pub class_hash: Felt,
    /// The contract address of the model
    pub contract_address: Felt,
    pub packed_size: u32,
    pub unpacked_size: u32,
    pub layout: Layout,
    pub schema: Ty,
}

#[derive(Debug)]
pub struct ModelCache {
    pool: SqlitePool,
    model_cache: RwLock<HashMap<Felt, Model>>,
}

impl ModelCache {
    pub async fn new(pool: SqlitePool) -> Result<Self, Error> {
        let models_rows: Vec<torii_sqlite_types::Model> = sqlx::query_as(
            "SELECT id, namespace, name, class_hash, contract_address, packed_size, \
             unpacked_size, layout, schema, executed_at, created_at FROM models",
        )
        .fetch_all(&pool)
        .await?;

        let mut model_cache = HashMap::new();
        for model in models_rows {
            let selector = Felt::from_hex(&model.id).map_err(ParseError::FromStr)?;
            let class_hash = Felt::from_hex(&model.class_hash).map_err(ParseError::FromStr)?;
            let contract_address =
                Felt::from_hex(&model.contract_address).map_err(ParseError::FromStr)?;

            let layout = serde_json::from_str(&model.layout).map_err(ParseError::FromJsonStr)?;
            let schema = serde_json::from_str(&model.schema).map_err(ParseError::FromJsonStr)?;

            model_cache.insert(
                selector,
                Model {
                    namespace: model.namespace,
                    name: model.name,
                    selector,
                    class_hash,
                    contract_address,
                    packed_size: model.packed_size,
                    unpacked_size: model.unpacked_size,
                    layout,
                    schema,
                },
            );
        }

        Ok(Self {
            pool,
            model_cache: RwLock::new(model_cache),
        })
    }

    pub async fn models(&self, selectors: &[Felt]) -> Result<Vec<Model>, Error> {
        if selectors.is_empty() {
            return Ok(self.model_cache.read().await.values().cloned().collect());
        }

        let mut schemas = Vec::with_capacity(selectors.len());
        for selector in selectors {
            schemas.push(self.model(selector).await?);
        }

        Ok(schemas)
    }

    pub async fn model(&self, selector: &Felt) -> Result<Model, Error> {
        {
            let cache = self.model_cache.read().await;
            if let Some(model) = cache.get(selector).cloned() {
                return Ok(model);
            }
        }

        self.update_model(selector).await
    }

    async fn update_model(&self, selector: &Felt) -> Result<Model, Error> {
        let (
            namespace,
            name,
            class_hash,
            contract_address,
            packed_size,
            unpacked_size,
            layout,
            schema,
        ): (String, String, String, String, u32, u32, String, String) = sqlx::query_as(
            "SELECT namespace, name, class_hash, contract_address, packed_size, unpacked_size, \
             layout, schema FROM models WHERE id = ?",
        )
        .bind(format!("{:#x}", selector))
        .fetch_one(&self.pool)
        .await?;

        let class_hash = Felt::from_hex(&class_hash).map_err(ParseError::FromStr)?;
        let contract_address = Felt::from_hex(&contract_address).map_err(ParseError::FromStr)?;

        let layout = serde_json::from_str(&layout).map_err(ParseError::FromJsonStr)?;
        let schema = serde_json::from_str(&schema).map_err(ParseError::FromJsonStr)?;

        let mut cache = self.model_cache.write().await;

        let model = Model {
            namespace,
            name,
            selector: *selector,
            class_hash,
            contract_address,
            packed_size,
            unpacked_size,
            layout,
            schema,
        };
        cache.insert(*selector, model.clone());

        Ok(model)
    }

    pub async fn set(&self, selector: Felt, model: Model) {
        let mut cache = self.model_cache.write().await;
        cache.insert(selector, model);
    }

    pub async fn clear(&self) {
        self.model_cache.write().await.clear();
    }
}

#[derive(Debug)]
pub enum TokenState {
    Registered,
    Registering(Arc<Mutex<()>>),
    NotRegistered,
}

#[derive(Debug)]
pub struct LocalCache {
    pub erc_cache: DashMap<String, I256>,
    // the registry is a map of token_id to a mutex that is used to track if the token is registered
    // we need a mutex for the token state to prevent race conditions in case of multiple token regs
    pub token_id_registry: DashMap<String, TokenState>,
}

impl LocalCache {
    pub async fn new(pool: Pool<Sqlite>) -> Self {
        // read existing token_id's from balances table and cache them
        let token_id_registry: Vec<String> =
            sqlx::query_scalar(&format!("SELECT id FROM {TOKENS_TABLE}"))
                .fetch_all(&pool)
                .await
                .expect("Should be able to read token_id's from blances table");

        Self {
            erc_cache: DashMap::new(),
            token_id_registry: token_id_registry
                .iter()
                .map(|token_id| (token_id.clone(), TokenState::Registered))
                .collect(),
        }
    }

    pub async fn get_token_registration_lock(&self, token_id: &str) -> Option<Arc<Mutex<()>>> {
        let entry = self.token_id_registry.entry(token_id.to_string());
        match entry {
            dashmap::Entry::Occupied(mut occupied) => match occupied.get() {
                TokenState::Registering(mutex) => Some(mutex.clone()),
                TokenState::Registered => None,
                TokenState::NotRegistered => {
                    let mutex = Arc::new(Mutex::new(()));
                    occupied.insert(TokenState::Registering(mutex.clone()));
                    Some(mutex)
                }
            },
            dashmap::Entry::Vacant(vacant) => {
                let mutex = Arc::new(Mutex::new(()));
                vacant.insert(TokenState::Registering(mutex.clone()));
                Some(mutex)
            }
        }
    }

    pub async fn mark_token_registered(&self, token_id: &str) {
        self.token_id_registry
            .insert(token_id.to_string(), TokenState::Registered);
    }

    pub async fn is_token_registered(&self, token_id: &str) -> bool {
        self.token_id_registry
            .get(token_id)
            .map(|t| matches!(t.value(), TokenState::Registered))
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone)]
pub enum ClassAbi {
    Sierra((EntryPointsByType, Vec<AbiEntry>)),
    Legacy(Vec<LegacyContractAbiEntry>),
}

#[derive(Debug)]
pub struct ContractClassCache<P: Provider + Sync + std::fmt::Debug> {
    pub classes: RwLock<HashMap<Felt, (Felt, ClassAbi)>>,
    pub provider: Arc<P>,
}

impl<P: Provider + Sync + std::fmt::Debug> ContractClassCache<P> {
    pub fn new(provider: Arc<P>) -> Self {
        Self {
            classes: RwLock::new(HashMap::new()),
            provider,
        }
    }

    pub async fn get(
        &self,
        contract_address: Felt,
        mut block_id: BlockId,
    ) -> Result<ClassAbi, Error> {
        {
            let classes = self.classes.read().await;
            if let Some(class) = classes.get(&contract_address) {
                return Ok(class.1.clone());
            }
        }

        let class_hash = match self
            .provider
            .get_class_hash_at(block_id, contract_address)
            .await
        {
            Ok(class_hash) => class_hash,
            Err(e) => match e {
                // if we got a block not found error, we probably are in a pending block.
                ProviderError::StarknetError(StarknetError::BlockNotFound) => {
                    block_id = BlockId::Tag(BlockTag::Pending);
                    self.provider
                        .get_class_hash_at(block_id, contract_address)
                        .await?
                }
                _ => return Err(Error::ProviderError(e)),
            },
        };
        let class = match self
            .provider
            .get_class_at(block_id, contract_address)
            .await?
        {
            ContractClass::Sierra(sierra) => {
                let abi: Vec<AbiEntry> = serde_json::from_str(&sierra.abi).unwrap();
                let functions: Vec<AbiEntry> = flatten_abi_funcs_recursive(&abi);
                ClassAbi::Sierra((sierra.entry_points_by_type, functions))
            }
            ContractClass::Legacy(legacy) => ClassAbi::Legacy(legacy.abi.unwrap_or_default()),
        };
        self.classes
            .write()
            .await
            .insert(contract_address, (class_hash, class.clone()));
        Ok(class)
    }
}

fn flatten_abi_funcs_recursive(abi: &[AbiEntry]) -> Vec<AbiEntry> {
    abi.iter()
        .flat_map(|entry| match entry {
            AbiEntry::Function(_) | AbiEntry::L1Handler(_) | AbiEntry::Constructor(_) => {
                vec![entry.clone()]
            }
            AbiEntry::Interface(interface) => flatten_abi_funcs_recursive(&interface.items),
            _ => vec![],
        })
        .collect()
}

pub fn get_entrypoint_name_from_class(class: &ClassAbi, selector: Felt) -> Option<String> {
    match class {
        ClassAbi::Sierra((entrypoints, abi)) => {
            let entrypoint_idx = match entrypoints
                .external
                .iter()
                .chain(entrypoints.l1_handler.iter())
                .chain(entrypoints.constructor.iter())
                .find(|entrypoint| entrypoint.selector == selector)
            {
                Some(entrypoint) => entrypoint.function_idx,
                None => return None,
            };

            abi.get(entrypoint_idx as usize)
                .and_then(|function| match function {
                    AbiEntry::Function(function) => Some(function.name.clone()),
                    AbiEntry::L1Handler(l1_handler) => Some(l1_handler.name.clone()),
                    AbiEntry::Constructor(constructor) => Some(constructor.name.clone()),
                    _ => None,
                })
        }
        ClassAbi::Legacy(abi) => abi.iter().find_map(|entry| match entry {
            LegacyContractAbiEntry::Function(function)
                if get_selector_from_name(&function.name).unwrap() == selector =>
            {
                Some(function.name.clone())
            }
            _ => None,
        }),
    }
}
