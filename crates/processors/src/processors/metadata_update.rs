use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use base64::engine::general_purpose;
use base64::Engine as _;
use cainome::cairo_serde::Zeroable;
use dojo_world::config::WorldMetadata;
use dojo_world::contracts::abigen::world::Event as WorldEvent;
use dojo_world::uri::Uri;
use starknet::core::types::{Event, Felt};
use starknet::providers::Provider;
use torii_storage::Storage;
use tracing::{error, info};

use crate::error::{Error, ParseError};
use crate::fetch::fetch_content_from_ipfs;
use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorContext};

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::metadata_update";

#[derive(Default, Debug)]
pub struct MetadataUpdateProcessor;

#[async_trait]
impl<P> EventProcessor<P> for MetadataUpdateProcessor
where
    P: Provider + Send + Sync + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "MetadataUpdate".to_string()
    }

    fn validate(&self, _event: &Event) -> bool {
        true
    }

    fn task_identifier(&self, event: &Event) -> TaskId {
        let mut hasher = DefaultHasher::new();
        event.keys.iter().for_each(|k| k.hash(&mut hasher));
        hasher.finish()
    }

    async fn process(&self, ctx: &EventProcessorContext<P>) -> Result<(), Error> {
        // Torii version is coupled to the world version, so we can expect the event to be well
        // formed.
        let event = match WorldEvent::try_from(&ctx.event).unwrap_or_else(|_| {
            panic!(
                "Expected {} event to be well formed.",
                <MetadataUpdateProcessor as EventProcessor<P>>::event_key(self)
            )
        }) {
            WorldEvent::MetadataUpdate(e) => e,
            _ => {
                unreachable!()
            }
        };

        // We know it's a valid Byte Array since it's coming from the world.
        let uri_str = event.uri.to_string().unwrap();
        info!(
            target: LOG_TARGET,
            resource = %format!("{:#x}", event.resource),
            uri = %uri_str,
            "Resource metadata set."
        );
        ctx.storage
            .set_metadata(&event.resource, &uri_str, ctx.block_timestamp)?;

        // Only retrieve metadata for the World contract.
        let storage = ctx.storage.clone();
        if event.resource.is_zero() {
            tokio::spawn(async move {
                try_retrieve(storage, event.resource, uri_str).await;
            });
        }

        Ok(())
    }
}

async fn try_retrieve(storage: Arc<dyn Storage>, resource: Felt, uri_str: String) {
    match metadata(uri_str.clone()).await {
        Ok((metadata, icon_img, cover_img)) => {
            storage
                .update_metadata(&resource, &uri_str, &metadata, &icon_img, &cover_img)
                .unwrap();
            info!(
                target: LOG_TARGET,
                resource = %format!("{:#x}", resource),
                "Updated resource metadata from ipfs."
            );
        }
        Err(e) => {
            error!(
                target: LOG_TARGET,
                resource = %format!("{:#x}", resource),
                uri = %uri_str,
                error = ?e,
                "Retrieving resource uri."
            );
        }
    }
}

async fn metadata(
    uri_str: String,
) -> Result<(WorldMetadata, Option<String>, Option<String>), Error> {
    let uri = Uri::Ipfs(uri_str);
    let cid = uri.cid().ok_or(Error::UriMalformed)?;

    let bytes = fetch_content_from_ipfs(cid)
        .await
        .map_err(Error::IpfsError)?;
    let metadata: WorldMetadata =
        serde_json::from_str(std::str::from_utf8(&bytes).map_err(ParseError::Utf8Error)?)
            .map_err(ParseError::FromJsonStr)?;

    let icon_img = fetch_image(&metadata.icon_uri).await;
    let cover_img = fetch_image(&metadata.cover_uri).await;

    Ok((metadata, icon_img, cover_img))
}

async fn fetch_image(image_uri: &Option<Uri>) -> Option<String> {
    if let Some(uri) = image_uri {
        let data = fetch_content_from_ipfs(uri.cid()?).await.ok()?;
        let encoded = general_purpose::STANDARD.encode(data);
        return Some(encoded);
    }

    None
}
