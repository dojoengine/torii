use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use starknet::core::types::contract::AbiEntry;
use starknet::core::types::{
    BlockId, BlockTag, ContractClass, EntryPointsByType, LegacyContractAbiEntry, StarknetError,
};
use starknet::core::types::{Felt, U256};
use starknet::core::utils::get_selector_from_name;
use starknet::providers::{Provider, ProviderError};
use tokio::sync::{Mutex, RwLock};
use torii_math::I256;
use torii_proto::{BalanceId, Model, TokenId};
use torii_storage::ReadOnlyStorage;

use crate::error::Error;

pub mod error;

pub type CacheError = Box<dyn std::error::Error + Send + Sync>;

#[async_trait]
pub trait ReadOnlyCache: Send + Sync + std::fmt::Debug {
    /// Get models by selectors. If selectors is empty, returns all models for the world.
    async fn models(&self, world_address: Felt, selectors: &[Felt]) -> Result<Vec<Model>, CacheError>;

    /// Get a specific model by selector for the given world.
    async fn model(&self, world_address: Felt, selector: Felt) -> Result<Model, CacheError>;

    /// Check if a token is registered.
    async fn is_token_registered(&self, token_id: &TokenId) -> bool;

    /// Get a token registration lock for coordination.
    async fn get_token_registration_lock(&self, token_id: TokenId) -> Option<Arc<Mutex<()>>>;

    /// Get the balances diff.
    async fn balances_diff(&self) -> HashMap<BalanceId, I256>;

    /// Get the total supply diff.
    async fn total_supply_diff(&self) -> HashMap<TokenId, I256>;
}

#[async_trait]
pub trait Cache: ReadOnlyCache + Send + Sync + std::fmt::Debug {
    /// Register a model in the cache, scoped to a world.
    async fn register_model(&self, world_address: Felt, selector: Felt, model: Model);

    /// Clear all models from the cache.
    async fn clear_models(&self);

    /// Mark a token as registered.
    async fn mark_token_registered(&self, token_id: TokenId);

    /// Clear the balances diff.
    async fn clear_balances_diff(&self);

    /// Update the balances diff.
    async fn update_balance_diff(&self, token_id: TokenId, from: Felt, to: Felt, value: U256);
}

#[derive(Debug)]
pub struct InMemoryCache {
    pub model_cache: ModelCache,
    pub erc_cache: ErcCache,
}

impl InMemoryCache {
    pub async fn new(storage: Arc<dyn ReadOnlyStorage>) -> Result<Self, Error> {
        Ok(Self {
            model_cache: ModelCache::new(storage.clone()).await?,
            erc_cache: ErcCache::new(storage).await?,
        })
    }
}

#[async_trait]
impl ReadOnlyCache for InMemoryCache {
    async fn models(&self, world_address: Felt, selectors: &[Felt]) -> Result<Vec<Model>, CacheError> {
        self.model_cache
            .models(world_address, selectors)
            .await
            .map_err(|e| Box::new(e) as CacheError)
    }

    async fn model(&self, world_address: Felt, selector: Felt) -> Result<Model, CacheError> {
        self.model_cache
            .model(world_address, selector)
            .await
            .map_err(|e| Box::new(e) as CacheError)
    }

    async fn is_token_registered(&self, token_id: &TokenId) -> bool {
        self.erc_cache.is_token_registered(token_id).await
    }

    async fn get_token_registration_lock(&self, token_id: TokenId) -> Option<Arc<Mutex<()>>> {
        self.erc_cache.get_token_registration_lock(token_id).await
    }

    async fn balances_diff(&self) -> HashMap<BalanceId, I256> {
        self.erc_cache
            .balances_diff
            .iter()
            .map(|t| (t.key().clone(), *t.value()))
            .collect()
    }

    async fn total_supply_diff(&self) -> HashMap<TokenId, I256> {
        self.erc_cache
            .total_supply_diff
            .iter()
            .map(|t| (t.key().clone(), *t.value()))
            .collect()
    }
}

#[async_trait]
impl Cache for InMemoryCache {
    async fn register_model(&self, world_address: Felt, selector: Felt, model: Model) {
        self.model_cache.set(world_address, selector, model).await
    }

    async fn clear_models(&self) {
        self.model_cache.clear().await
    }

    async fn mark_token_registered(&self, token_id: TokenId) {
        self.erc_cache.mark_token_registered(token_id).await
    }

    async fn clear_balances_diff(&self) {
        self.erc_cache.balances_diff.clear();
        self.erc_cache.balances_diff.shrink_to_fit();
        self.erc_cache.total_supply_diff.clear();
        self.erc_cache.total_supply_diff.shrink_to_fit();
    }

    async fn update_balance_diff(&self, token_id: TokenId, from: Felt, to: Felt, value: U256) {
        self.erc_cache
            .update_balance_diff(token_id, from, to, value)
            .await
    }
}

#[derive(Debug)]
pub struct ModelCache {
    // Outer key: world_address, Inner key: model_selector
    model_cache: RwLock<HashMap<Felt, HashMap<Felt, Model>>>,
}

impl ModelCache {
    pub async fn new(storage: Arc<dyn ReadOnlyStorage>) -> Result<Self, Error> {
        let models = storage.models(None, &[]).await?;

        let mut model_cache: HashMap<Felt, HashMap<Felt, Model>> = HashMap::new();
        for model in models {
            model_cache
                .entry(model.world_address)
                .or_insert_with(HashMap::new)
                .insert(model.selector, model);
        }

        Ok(Self {
            model_cache: RwLock::new(model_cache),
        })
    }

    pub async fn models(&self, world_address: Felt, selectors: &[Felt]) -> Result<Vec<Model>, Error> {
        let cache = self.model_cache.read().await;
        
        if selectors.is_empty() {
            return Ok(cache
                .get(&world_address)
                .map(|world_models| world_models.values().cloned().collect())
                .unwrap_or_default());
        }

        drop(cache);
        
        let mut schemas = Vec::with_capacity(selectors.len());
        for selector in selectors {
            schemas.push(self.model(world_address, *selector).await?);
        }

        Ok(schemas)
    }

    pub async fn model(&self, world_address: Felt, selector: Felt) -> Result<Model, Error> {
        let cache = self.model_cache.read().await;
        
        cache
            .get(&world_address)
            .and_then(|world_models| world_models.get(&selector))
            .cloned()
            .ok_or_else(|| Error::ModelNotFound(selector))
    }

    pub async fn set(&self, world_address: Felt, selector: Felt, model: Model) {
        let mut cache = self.model_cache.write().await;
        cache
            .entry(world_address)
            .or_insert_with(HashMap::new)
            .insert(selector, model);
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
pub struct ErcCache {
    pub balances_diff: DashMap<BalanceId, I256>,
    // Track total supply changes for ERC20 tokens (contract_address -> supply_diff)
    pub total_supply_diff: DashMap<TokenId, I256>,
    // the registry is a map of token_id to a mutex that is used to track if the token is registered
    // we need a mutex for the token state to prevent race conditions in case of multiple token regs
    pub token_id_registry: DashMap<TokenId, TokenState>,
}

impl ErcCache {
    pub async fn new(storage: Arc<dyn ReadOnlyStorage>) -> Result<Self, Error> {
        // read existing token_id's from balances table and cache them
        let token_id_registry: HashSet<TokenId> = storage.token_ids().await?;

        Ok(Self {
            balances_diff: DashMap::new(),
            total_supply_diff: DashMap::new(),
            token_id_registry: token_id_registry
                .into_iter()
                .map(|token_id| (token_id, TokenState::Registered))
                .collect(),
        })
    }

    pub async fn get_token_registration_lock(&self, token_id: TokenId) -> Option<Arc<Mutex<()>>> {
        let entry = self.token_id_registry.entry(token_id);
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

    pub async fn mark_token_registered(&self, token_id: TokenId) {
        self.token_id_registry
            .insert(token_id, TokenState::Registered);
    }

    pub async fn is_token_registered(&self, token_id: &TokenId) -> bool {
        self.token_id_registry
            .get(token_id)
            .map(|t| matches!(t.value(), TokenState::Registered))
            .unwrap_or(false)
    }

    pub async fn update_balance_diff(&self, token_id: TokenId, from: Felt, to: Felt, value: U256) {
        let value_i256 = I256::from(value);
        let negative_value_i256 = I256 {
            value,
            is_negative: true,
        };

        // Track individual balance changes
        if from != Felt::ZERO {
            let from_balance_id = BalanceId {
                account_address: from,
                token_id: token_id.clone(),
            };
            // Use atomic operation to avoid deadlocks
            self.balances_diff
                .entry(from_balance_id)
                .and_modify(|balance| *balance -= value_i256)
                .or_insert(negative_value_i256);
        }

        if to != Felt::ZERO {
            let to_balance_id = BalanceId {
                account_address: to,
                token_id: token_id.clone(),
            };
            // Use atomic operation to avoid deadlocks
            self.balances_diff
                .entry(to_balance_id)
                .and_modify(|balance| *balance += value_i256)
                .or_insert(value_i256);
        }

        // Track total supply changes based on token type
        // Schema:
        // - ERC-20: total_supply at contract level (sum of all tokens)
        // - ERC-721/ERC-1155: total_supply per token ID only (contract-level handled in registration)

        if token_id.is_nft() {
            // This is an NFT token (ERC721/ERC1155)
            // Only track individual token supply, contract-level is handled in registration
            if from == Felt::ZERO && to != Felt::ZERO {
                // Minting - track supply per token_id
                self.total_supply_diff
                    .entry(token_id.clone())
                    .and_modify(|supply| *supply += value_i256)
                    .or_insert(value_i256);
            } else if from != Felt::ZERO && to == Felt::ZERO {
                // Burning - track supply per token_id
                self.total_supply_diff
                    .entry(token_id.clone())
                    .and_modify(|supply| *supply -= value_i256)
                    .or_insert(negative_value_i256);
            }
        } else {
            // This is an ERC20 token, track supply changes at contract level
            if from == Felt::ZERO && to != Felt::ZERO {
                // Minting - increase total supply by amount
                self.total_supply_diff
                    .entry(token_id.clone())
                    .and_modify(|supply| *supply += value_i256)
                    .or_insert(value_i256);
            } else if from != Felt::ZERO && to == Felt::ZERO {
                // Burning - decrease total supply by amount
                self.total_supply_diff
                    .entry(token_id.clone())
                    .and_modify(|supply| *supply -= value_i256)
                    .or_insert(negative_value_i256);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum ClassAbi {
    Sierra((EntryPointsByType, Vec<AbiEntry>)),
    Legacy(Vec<LegacyContractAbiEntry>),
}

#[derive(Debug)]
pub struct ContractClassCache<P: Provider + Sync + Send + Clone + std::fmt::Debug> {
    pub classes: RwLock<HashMap<Felt, (Felt, ClassAbi)>>,
    pub provider: P,
}

impl<P: Provider + Sync + Send + Clone + std::fmt::Debug> ContractClassCache<P> {
    pub fn new(provider: P) -> Self {
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
                    block_id = BlockId::Tag(BlockTag::PreConfirmed);
                    self.provider
                        .get_class_hash_at(block_id, contract_address)
                        .await?
                }
                _ => return Err(Error::Provider(e)),
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
