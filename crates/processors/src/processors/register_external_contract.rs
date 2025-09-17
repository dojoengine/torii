use std::hash::{DefaultHasher, Hash, Hasher};
use std::str::FromStr;

use async_trait::async_trait;
use dojo_world::contracts::abigen::world::Event as WorldEvent;
use starknet::core::types::Event;
use torii_proto::ContractType;
use tracing::{debug, info};

use crate::error::Error;
use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorContext};
use metrics::counter;

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::register_external_contract";

#[derive(Default, Debug)]
pub struct RegisterExternalContractProcessor;

#[async_trait]
impl<P> EventProcessor<P> for RegisterExternalContractProcessor
where
    P: starknet::providers::Provider + Send + Sync + Clone + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "ExternalContractRegistered".to_string()
    }

    fn validate(&self, _event: &Event) -> bool {
        true
    }

    fn task_identifier(&self, event: &Event) -> TaskId {
        // Use the contract address as the task identifier
        let mut hasher = DefaultHasher::new();
        event.from_address.hash(&mut hasher);
        hasher.finish()
    }

    async fn process(&self, ctx: &EventProcessorContext<P>) -> Result<(), Error> {
        // Torii version is coupled to the world version, so we can expect the event to be well
        // formed.
        let event = match WorldEvent::try_from(&ctx.event).unwrap_or_else(|_| {
            panic!(
                "Expected {} event to be well formed.",
                <RegisterExternalContractProcessor as EventProcessor<P>>::event_key(self)
            )
        }) {
            WorldEvent::ExternalContractRegistered(e) => e,
            _ => {
                unreachable!()
            }
        };

        // Safe to unwrap, since it's coming from the chain.
        let namespace = event.namespace.to_string().unwrap();
        let contract_name = event.contract_name.to_string().unwrap();
        let instance_name = event.instance_name.to_string().unwrap();

        // Parse contract type from contract name
        let contract_type = ContractType::from_str(&contract_name).unwrap_or(ContractType::OTHER);

        info!(
            target: LOG_TARGET,
            namespace = %namespace,
            contract_name = %contract_name,
            instance_name = %instance_name,
            contract_address = %format!("{:#x}", event.contract_address.0),
            "Registered external contract."
        );

        debug!(
            target: LOG_TARGET,
            namespace = %namespace,
            contract_name = %contract_name,
            instance_name = %instance_name,
            contract_selector = %format!("{:#x}", event.contract_selector),
            class_hash = %format!("{:#x}", event.class_hash.0),
            contract_address = %format!("{:#x}", event.contract_address.0),
            block_number = %event.block_number,
            "Registered external contract details."
        );

        // Register the contract in storage
        ctx.storage
            .register_contract(
                event.contract_address.0,
                contract_type,
                event.block_number - 1,
            )
            .await?;

        // Record successful contract registration with context
        counter!(
            "torii_processor_operations_total",
            "operation" => "external_contract_registered",
            "namespace" => namespace,
            "contract_type" => contract_type.to_string()
        )
        .increment(1);

        Ok(())
    }
}
