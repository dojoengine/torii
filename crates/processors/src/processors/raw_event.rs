use async_trait::async_trait;
use starknet::core::types::Event;
use starknet::providers::Provider;

use crate::error::Error;
use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorContext};

#[derive(Default, Debug)]
pub struct RawEventProcessor;

#[async_trait]
impl<P> EventProcessor<P> for RawEventProcessor
where
    P: Provider + Send + Sync + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "".to_string()
    }

    fn validate(&self, _event: &Event) -> bool {
        true
    }

    fn task_identifier(&self, _event: &Event) -> TaskId {
        0
    }

    async fn process(&self, _ctx: &EventProcessorContext<P>) -> Result<(), Error> {
        // We can choose to consider them, or not.

        Ok(())
    }
}
