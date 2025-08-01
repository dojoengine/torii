use std::collections::HashMap;

use controller::ControllerProcessor;
use erc1155_legacy_transfer_batch::Erc1155LegacyTransferBatchProcessor;
use erc1155_legacy_transfer_single::Erc1155LegacyTransferSingleProcessor;
use erc1155_transfer_batch::Erc1155TransferBatchProcessor;
use erc1155_transfer_single::Erc1155TransferSingleProcessor;
use erc20_legacy_transfer::Erc20LegacyTransferProcessor;
use erc20_transfer::Erc20TransferProcessor;
use erc4906_batch_metadata_update::Erc4906BatchMetadataUpdateProcessor;
use erc4906_metadata_update::Erc4906MetadataUpdateProcessor;
use erc721_legacy_transfer::Erc721LegacyTransferProcessor;
use erc721_transfer::Erc721TransferProcessor;
use event_message::EventMessageProcessor;
use metadata_update::MetadataUpdateProcessor;
use raw_event::RawEventProcessor;
use register_event::RegisterEventProcessor;
use register_model::RegisterModelProcessor;
use starknet::core::types::Felt;
use starknet::{core::utils::get_selector_from_name, providers::Provider};
use store_del_record::StoreDelRecordProcessor;
use store_set_record::StoreSetRecordProcessor;
use store_transaction::StoreTransactionProcessor;
use store_update_member::StoreUpdateMemberProcessor;
use store_update_record::StoreUpdateRecordProcessor;
use torii_proto::ContractType;
use upgrade_event::UpgradeEventProcessor;
use upgrade_model::UpgradeModelProcessor;

use crate::processors::erc7572_contract_uri_updated::Erc7572ContractUriUpdatedProcessor;
use crate::{BlockProcessor, EventProcessor, TransactionProcessor};

pub(crate) mod controller;
mod erc1155_legacy_transfer_batch;
mod erc1155_legacy_transfer_single;
mod erc1155_transfer_batch;
mod erc1155_transfer_single;
mod erc20_legacy_transfer;
mod erc20_transfer;
mod erc4906_batch_metadata_update;
mod erc4906_metadata_update;
mod erc721_legacy_transfer;
mod erc721_transfer;
mod erc7572_contract_uri_updated;
mod event_message;
mod metadata_update;
mod raw_event;
mod register_event;
mod register_model;
mod store_del_record;
mod store_set_record;
mod store_transaction;
mod store_update_member;
mod store_update_record;
mod upgrade_event;
mod upgrade_model;

type EventKey = Felt;
type EventProcessorMap<P> = HashMap<EventKey, Vec<Box<dyn EventProcessor<P>>>>;

#[allow(missing_debug_implementations)]
pub struct Processors<P: Provider + Send + Sync + Clone + std::fmt::Debug + 'static> {
    pub block: Vec<Box<dyn BlockProcessor<P>>>,
    pub transaction: Vec<Box<dyn TransactionProcessor<P>>>,
    pub catch_all_event: Box<dyn EventProcessor<P>>,
    pub event_processors: HashMap<ContractType, EventProcessorMap<P>>,
}

impl<P: Provider + Send + Sync + Clone + std::fmt::Debug + 'static> Default for Processors<P> {
    fn default() -> Self {
        Self {
            block: vec![],
            transaction: vec![Box::new(StoreTransactionProcessor)],
            // We shouldn't have a catch all for now since the world doesn't forward raw events
            // anymore.
            catch_all_event: Box::new(RawEventProcessor) as Box<dyn EventProcessor<P>>,
            event_processors: Self::initialize_event_processors(),
        }
    }
}

impl<P: Provider + Send + Sync + Clone + std::fmt::Debug + 'static> Processors<P> {
    pub fn initialize_event_processors() -> HashMap<ContractType, EventProcessorMap<P>> {
        let mut event_processors_map = HashMap::<ContractType, EventProcessorMap<P>>::new();

        let event_processors = vec![
            (
                ContractType::WORLD,
                vec![
                    Box::new(RegisterModelProcessor) as Box<dyn EventProcessor<P>>,
                    Box::new(RegisterEventProcessor) as Box<dyn EventProcessor<P>>,
                    Box::new(UpgradeModelProcessor) as Box<dyn EventProcessor<P>>,
                    Box::new(UpgradeEventProcessor) as Box<dyn EventProcessor<P>>,
                    Box::new(StoreSetRecordProcessor),
                    Box::new(StoreDelRecordProcessor),
                    Box::new(StoreUpdateRecordProcessor),
                    Box::new(StoreUpdateMemberProcessor),
                    Box::new(MetadataUpdateProcessor),
                    Box::new(EventMessageProcessor),
                ],
            ),
            (
                ContractType::ERC20,
                vec![
                    Box::new(Erc20TransferProcessor) as Box<dyn EventProcessor<P>>,
                    Box::new(Erc20LegacyTransferProcessor) as Box<dyn EventProcessor<P>>,
                    Box::new(Erc7572ContractUriUpdatedProcessor) as Box<dyn EventProcessor<P>>,
                ],
            ),
            (
                ContractType::ERC721,
                vec![
                    Box::new(Erc721TransferProcessor) as Box<dyn EventProcessor<P>>,
                    Box::new(Erc721LegacyTransferProcessor) as Box<dyn EventProcessor<P>>,
                    Box::new(Erc4906MetadataUpdateProcessor) as Box<dyn EventProcessor<P>>,
                    Box::new(Erc4906BatchMetadataUpdateProcessor) as Box<dyn EventProcessor<P>>,
                    Box::new(Erc7572ContractUriUpdatedProcessor) as Box<dyn EventProcessor<P>>,
                ],
            ),
            (
                ContractType::ERC1155,
                vec![
                    Box::new(Erc1155TransferBatchProcessor) as Box<dyn EventProcessor<P>>,
                    Box::new(Erc1155TransferSingleProcessor) as Box<dyn EventProcessor<P>>,
                    Box::new(Erc1155LegacyTransferBatchProcessor) as Box<dyn EventProcessor<P>>,
                    Box::new(Erc1155LegacyTransferSingleProcessor) as Box<dyn EventProcessor<P>>,
                    Box::new(Erc4906MetadataUpdateProcessor) as Box<dyn EventProcessor<P>>,
                    Box::new(Erc4906BatchMetadataUpdateProcessor) as Box<dyn EventProcessor<P>>,
                    Box::new(Erc7572ContractUriUpdatedProcessor) as Box<dyn EventProcessor<P>>,
                ],
            ),
            (
                ContractType::UDC,
                vec![Box::new(ControllerProcessor) as Box<dyn EventProcessor<P>>],
            ),
        ];

        for (contract_type, processors) in event_processors {
            for processor in processors {
                let key = get_selector_from_name(processor.event_key().as_str())
                    .expect("Event key is ASCII so this should never fail");
                event_processors_map
                    .entry(contract_type)
                    .or_default()
                    .entry(key)
                    .or_default()
                    .push(processor);
            }
        }

        event_processors_map
    }

    pub fn get_event_processors(
        &self,
        contract_type: ContractType,
    ) -> &HashMap<EventKey, Vec<Box<dyn EventProcessor<P>>>> {
        self.event_processors.get(&contract_type).unwrap()
    }
}
