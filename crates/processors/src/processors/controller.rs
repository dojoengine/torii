//! Controller processor.
//!
//! This processor is responsible for processing the deployed contracts events from the UDC.
//! It is used to index the controllers and their associated accounts.
//!
//! The challenge here is that controller has different constructor calldata depending on the
//! authentication method used, and no magic value for all the authentication methods.
//!
//! The checks here are purely based on the calldata structure, where the probability
//! of having a random calldata passing those tests is very low.
//!
//! If it is not enough, we query the Cartridge API to check if the address is a known controller
//! (this operation is costly, so we only do it if the calldata structure is not enough).

use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::Duration;

use async_trait::async_trait;
use cainome_cairo_serde::CairoSerde;
use chrono::DateTime;
use reqwest;
use serde::{Deserialize, Serialize};
use starknet::core::types::Event;
use starknet::core::utils::parse_cairo_short_string;
use starknet::macros::felt;
use starknet::providers::Provider;
use starknet_crypto::Felt;
use thiserror::Error;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::error::Error;
use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorContext};

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::controller";
const CARTRIDGE_LOOKUP_URL: &str = "https://api.cartridge.gg/lookup";

#[derive(Default, Debug)]
pub struct ControllerProcessor;

#[derive(Error, Debug)]
pub enum ControllerProcessorError {
    #[error("Calldata length mismatch")]
    CalldataLenMismatch,
    #[error("Cartridge magic length mismatch")]
    CartridgeMagicLengthMismatch,
    #[error("Cartridge magic mismatch")]
    CartridgeMagicMismatch,
    #[error("Invalid Cartridge Encoding")]
    InvalidCartridgeEncoding,
    #[error("Failed to fetch metadata")]
    FetchMetadataError(String),
}

#[derive(cainome_cairo_serde_derive::CairoSerde, Debug)]
struct UdcContractDeployedEvent {
    address: Felt,
    deployer: Felt,
    unique: Felt,
    class_hash: Felt,
    calldata: Vec<Felt>,
    salt: Felt,
}

// https://x.cartridge.gg/
const CARTRIDGE_MAGIC: [Felt; 22] = [
    felt!("0x68"),
    felt!("0x74"),
    felt!("0x74"),
    felt!("0x70"),
    felt!("0x73"),
    felt!("0x3a"),
    felt!("0x2f"),
    felt!("0x2f"),
    felt!("0x78"),
    felt!("0x2e"),
    felt!("0x63"),
    felt!("0x61"),
    felt!("0x72"),
    felt!("0x74"),
    felt!("0x72"),
    felt!("0x69"),
    felt!("0x64"),
    felt!("0x67"),
    felt!("0x65"),
    felt!("0x2e"),
    felt!("0x67"),
    felt!("0x67"),
];

#[derive(Debug, Serialize, Deserialize)]
struct LookupRequest {
    addresses: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LookupResponse {
    results: Vec<LookupResult>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LookupResult {
    username: String,
    addresses: Vec<String>,
}

#[async_trait]
impl<P> EventProcessor<P> for ControllerProcessor
where
    P: Provider + Send + Sync + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "ContractDeployed".to_string()
    }

    fn validate(&self, event: &Event) -> bool {
        // ContractDeployed event has no keys and at least 3 elements in the calldata (which is the smallest calldata in Cartridge encoding).
        event.keys.len() == 1 && event.data.len() >= 3
    }

    fn task_identifier(&self, event: &Event) -> TaskId {
        let mut hasher = DefaultHasher::new();
        // the contract address is the first felt in data
        event.data[0].hash(&mut hasher);
        hasher.finish()
    }

    async fn process(
        &self,
        ctx: &EventProcessorContext<P>,
    ) -> Result<(), Error> {
        let udc_event = UdcContractDeployedEvent::cairo_deserialize(&ctx.event.data, 0)?;

        if !is_cartridge_controller(&udc_event).await? {
            return Ok(());
        }

        // Last felt in data is the salt which is the username encoded as short string.
        let username_felt = udc_event.salt;
        let username = match parse_cairo_short_string(&username_felt) {
            Ok(username) => username,
            Err(_) => {
                // This shouldn't happen since the calldata may vary depending on the controller deploy calldata, but the salt
                // is always the username.
                // The few cases we have are not clearly identified yet, but something is off with those accounts.
                // Hence, they are currently silently discarded.
                // (Only 3 controller accounts have been identified so far with this issue.)
                return Ok(());
            }
        };

        let address = format!("{:#064x}", udc_event.address);

        info!(
            target: LOG_TARGET,
            username = %username,
            address = %address,
            "Controller deployed."
        );

        ctx.storage.add_controller(&username, &address, DateTime::from_timestamp(ctx.block_timestamp as i64, 0).unwrap()).await?;

        Ok(())
    }
}

/// Validates that a username only contains letters, numbers, and hyphens
fn is_valid_username(username: &str) -> bool {
    if username.is_empty() {
        return false;
    }
    username.chars().all(|c| c.is_alphanumeric() || c == '-')
}

/// Fetches controller information from the Cartridge API.
async fn fetch_controller_username(
    address: &str,
) -> Result<Option<String>, ControllerProcessorError> {
    const MAX_RETRIES: u32 = 3;
    const INITIAL_BACKOFF: Duration = Duration::from_secs(2);

    debug!(
        target: LOG_TARGET,
        address = address,
        "Looking up controller username from Cartridge API"
    );

    let client = reqwest::Client::new();
    let request = LookupRequest {
        addresses: vec![address.to_string()],
    };

    for attempt in 0..MAX_RETRIES {
        match client
            .post(CARTRIDGE_LOOKUP_URL)
            .json(&request)
            .send()
            .await
        {
            Ok(response) => match response.json::<LookupResponse>().await {
                Ok(lookup) => {
                    // If we got any results, the address is a controller since we are only
                    // looking up for one address.
                    return Ok(lookup.results.first().map(|r| r.username.clone()));
                }
                Err(e) => {
                    return Err(ControllerProcessorError::FetchMetadataError(format!(
                        "Failed to parse controller lookup response: {}",
                        e
                    )));
                }
            },
            Err(e) => {
                if attempt < MAX_RETRIES {
                    let backoff = INITIAL_BACKOFF * 2u32.pow(attempt);
                    warn!(
                        target: LOG_TARGET,
                        attempt = attempt + 1,
                        backoff_secs = backoff.as_secs(),
                        error = ?e,
                        "Failed to fetch controller lookup, retrying..."
                    );
                    sleep(backoff).await;
                } else {
                    return Err(ControllerProcessorError::FetchMetadataError(
                        "Failed to fetch controller lookup".to_string(),
                    ));
                }
            }
        }
    }

    Err(ControllerProcessorError::FetchMetadataError(
        "Failed to fetch controller lookup after all retries".to_string(),
    ))
}

/// Checks if the controller is a Cartridge controller.
///
/// We first check if the calldata matches any of the known patterns. The probability
/// of having a random calldata passing those tests is very low.
///
/// If it is not enough, we query the Cartridge API to check if the address is a known controller.
async fn is_cartridge_controller(
    event: &UdcContractDeployedEvent,
) -> Result<bool, ControllerProcessorError> {
    let username = parse_cairo_short_string(&event.salt).unwrap_or_default();

    // First check if any of the known calldata patterns match
    if parse_controller_calldata_webauthn(&event.calldata).is_ok() {
        // To discard some invalid controllers, we check the username
        // (it shouldn't be necessary in this case since the webauthn calldata contains a magic number).
        return Ok(is_valid_username(&username));
    }

    if parse_controller_calldata_eip191(&event.calldata).is_ok() {
        return Ok(is_valid_username(&username));
    }

    if parse_controller_calldata_starknet(&event.calldata).is_ok() {
        return Ok(is_valid_username(&username));
    }

    if parse_controller_calldata_external(&event.calldata).is_ok() {
        return Ok(is_valid_username(&username));
    }

    if parse_controller_calldata_siws(&event.calldata).is_ok() {
        return Ok(is_valid_username(&username));
    }

    // We can discard the event if the username is not valid, even if some controllers are in
    // a weird state where the username if actually invalid. Those will be discarded.
    if !is_valid_username(&username) {
        return Ok(false);
    }

    // If we get here, we need to check with the Cartridge API to discard some false positives.
    let address = format!("{:#064x}", event.address);
    fetch_controller_username(&address)
        .await
        .map(|username| username.is_some())
}

/// Parses a calldata expected to be a Cartridge controller deployment calldata for WebAuthn (passkeys).
fn parse_controller_calldata_webauthn(calldata: &[Felt]) -> Result<(), ControllerProcessorError> {
    // The calldata has to be more than 25 felts.
    if calldata.len() < 25 {
        return Err(ControllerProcessorError::CalldataLenMismatch);
    }

    if calldata[0] != felt!("0x0") {
        return Err(ControllerProcessorError::InvalidCartridgeEncoding);
    }

    if calldata[1] != felt!("0x4") {
        return Err(ControllerProcessorError::InvalidCartridgeEncoding);
    }

    // Check for the Cartridge magic sequence.
    let cartridge_magic_len = calldata[2];
    // Length has to be 22.
    if cartridge_magic_len != Felt::from(22) {
        return Err(ControllerProcessorError::CartridgeMagicLengthMismatch);
    }

    // This should never fail if since our len is 22.
    // In future version of controller this may change, using the class hash
    // the processing could differ.
    let cartridge_magic: [Felt; 22] = calldata[3..25].try_into().unwrap();

    // Has to match with https://x.cartridge.gg/.
    if !CARTRIDGE_MAGIC.eq(&cartridge_magic) {
        return Err(ControllerProcessorError::CartridgeMagicMismatch);
    }

    Ok(())
}

/// Parses a calldata expected to be a Cartridge controller deployment calldata for EIP191.
fn parse_controller_calldata_eip191(calldata: &[Felt]) -> Result<(), ControllerProcessorError> {
    if calldata.len() < 4 {
        return Err(ControllerProcessorError::CalldataLenMismatch);
    }

    if calldata[0] != felt!("0x0") {
        return Err(ControllerProcessorError::InvalidCartridgeEncoding);
    }

    if calldata[1] != felt!("0x3") {
        return Err(ControllerProcessorError::InvalidCartridgeEncoding);
    }

    if calldata.len() == 4 && calldata[3] != felt!("0x1") {
        return Err(ControllerProcessorError::InvalidCartridgeEncoding);
    }

    Ok(())
}

/// Parses a calldata expected to be a Cartridge controller deployment calldata for external authentication.
fn parse_controller_calldata_external(calldata: &[Felt]) -> Result<(), ControllerProcessorError> {
    if calldata.len() < 3 {
        return Err(ControllerProcessorError::CalldataLenMismatch);
    }

    if calldata[0] != felt!("0x1") {
        return Err(ControllerProcessorError::InvalidCartridgeEncoding);
    }

    if calldata.len() == 3 && calldata[2] != felt!("0x1") {
        return Err(ControllerProcessorError::InvalidCartridgeEncoding);
    }

    Ok(())
}

/// Parses a calldata expected to be a Cartridge controller deployment calldata for Starknet authentication.
fn parse_controller_calldata_starknet(calldata: &[Felt]) -> Result<(), ControllerProcessorError> {
    if calldata.len() < 4 {
        return Err(ControllerProcessorError::CalldataLenMismatch);
    }

    if calldata[0] != felt!("0x0") {
        return Err(ControllerProcessorError::InvalidCartridgeEncoding);
    }

    if calldata[1] != felt!("0x0") {
        return Err(ControllerProcessorError::InvalidCartridgeEncoding);
    }

    if calldata.len() == 4 && calldata[3] != felt!("0x1") {
        return Err(ControllerProcessorError::InvalidCartridgeEncoding);
    }

    Ok(())
}

/// Parses a calldata expected to be a Cartridge controller deployment calldata for SIWS.
fn parse_controller_calldata_siws(calldata: &[Felt]) -> Result<(), ControllerProcessorError> {
    if calldata.len() < 5 {
        return Err(ControllerProcessorError::CalldataLenMismatch);
    }

    if calldata[0] != felt!("0x0") {
        return Err(ControllerProcessorError::InvalidCartridgeEncoding);
    }

    if calldata[1] != felt!("0x5") {
        return Err(ControllerProcessorError::InvalidCartridgeEncoding);
    }

    if calldata.len() == 5 && calldata[4] != felt!("0x1") {
        return Err(ControllerProcessorError::InvalidCartridgeEncoding);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use starknet::macros::felt;

    /// Tests the parsing of a calldata that is a Cartridge controller deployment calldata with WebAuthn.
    /// Taken from mainnet: <https://voyager.online/event/678075_19_0>
    #[tokio::test]
    async fn test_parse_cartridge_controller_calldata_webauthn() {
        let event = Event {
            keys: vec![felt!(
                "0x26b160f10156dea0639bec90696772c640b9706a47f5b8c52ea1abe5858b34d"
            )],
            data: vec![
                felt!("0x48e13ef7ab79637afd38a4b022862a7e6f3fd934f194c435d7e7b17bac06715"),
                felt!("0x412392b1436bae7f127fdf8ccea730bb8e52cb654491389b38d1ade7a5dc0d7"),
                felt!("0x0"),
                felt!("0x24a9edbfa7082accfceabf6a92d7160086f346d622f28741bf1c651c412c9ab"),
                felt!("0x1e"),
                felt!("0x0"),
                felt!("0x4"),
                felt!("0x16"),
                felt!("0x68"),
                felt!("0x74"),
                felt!("0x74"),
                felt!("0x70"),
                felt!("0x73"),
                felt!("0x3a"),
                felt!("0x2f"),
                felt!("0x2f"),
                felt!("0x78"),
                felt!("0x2e"),
                felt!("0x63"),
                felt!("0x61"),
                felt!("0x72"),
                felt!("0x74"),
                felt!("0x72"),
                felt!("0x69"),
                felt!("0x64"),
                felt!("0x67"),
                felt!("0x65"),
                felt!("0x2e"),
                felt!("0x67"),
                felt!("0x67"),
                felt!("0x9d0aec9905466c9adf79584fa75fed3"),
                felt!("0x20a97ec3f8efbc2aca0cf7cabb420b4a"),
                felt!("0x30910fae3f3451a26071c3afc453425e"),
                felt!("0xa4e54fa48a6c3f34444687c2552b157f"),
                felt!("0x1"),
                felt!("0x676c69686d"),
            ],
            from_address: felt!(
                "0x041a78e741e5af2fec34b695679bc6891742439f7afb8484ecd7766661ad02bf"
            ),
        };

        let udc_event = UdcContractDeployedEvent::cairo_deserialize(&event.data, 0).unwrap();

        assert!(is_cartridge_controller(&udc_event).await.unwrap());
        assert_eq!(parse_cairo_short_string(&udc_event.salt).unwrap(), "glihm");
    }

    /// Tests the parsing of a calldata for a Cartridge controller that has invalid salt.
    /// Taken from mainnet: <https://voyager.online/event/1364488_7_182>
    #[tokio::test]
    async fn test_parse_cartridge_controller_invalid_salt() {
        let event = Event {
            keys: vec![felt!(
                "0x26b160f10156dea0639bec90696772c640b9706a47f5b8c52ea1abe5858b34d"
            )],
            data: vec![
                felt!("0x1d21ea9b21623d3298c1b207080fb573a36cd11ba244e5f91f6d0184690f8fd"),
                felt!("0x494ab036657fea16b10064bfd2d3a7666f546aab3724e7f8559cfed07584202"),
                felt!("0x0"),
                felt!("0x32e17891b6cc89e0c3595a3df7cee760b5993744dc8dfef2bd4d443e65c0f40"),
                felt!("0x1e"),
                felt!("0x0"),
                felt!("0x4"),
                felt!("0x16"),
                felt!("0x68"),
                felt!("0x74"),
                felt!("0x74"),
                felt!("0x70"),
                felt!("0x73"),
                felt!("0x3a"),
                felt!("0x2f"),
                felt!("0x2f"),
                felt!("0x78"),
                felt!("0x2e"),
                felt!("0x63"),
                felt!("0x61"),
                felt!("0x72"),
                felt!("0x74"),
                felt!("0x72"),
                felt!("0x69"),
                felt!("0x64"),
                felt!("0x67"),
                felt!("0x65"),
                felt!("0x2e"),
                felt!("0x67"),
                felt!("0x67"),
                felt!("0x9d0aec9905466c9adf79584fa75fed3"),
                felt!("0x20a97ec3f8efbc2aca0cf7cabb420b4a"),
                felt!("0xce942e6fdf18ac48aca69c39d9419c2f"),
                felt!("0x1248229349df98a8ed21e4eaa337df35"),
                felt!("0x1"),
                felt!("0x76acb5e81b78b7e01241565c5cead1a30b93b608b44868c72936cb96d49243f"),
            ],
            from_address: felt!(
                "0x041a78e741e5af2fec34b695679bc6891742439f7afb8484ecd7766661ad02bf"
            ),
        };

        let udc_event = UdcContractDeployedEvent::cairo_deserialize(&event.data, 0).unwrap();

        assert!(!is_cartridge_controller(&udc_event).await.unwrap());
        assert!(parse_cairo_short_string(&udc_event.salt).is_err());
    }

    /// Tests the parsing of a calldata that is a Cartridge controller deployment calldata with EIP191.
    /// Taken from mainnet: <https://voyager.online/event/1464416_90_0>
    #[tokio::test]
    async fn test_parse_cartridge_controller_calldata_eip191() {
        let event = Event {
            keys: vec![felt!(
                "0x26b160f10156dea0639bec90696772c640b9706a47f5b8c52ea1abe5858b34d"
            )],
            data: vec![
                felt!("0x48ca8934c1aa23fe7977ce6992263ca43fce4e4fda8c609ccafed3871e3c7df"),
                felt!("0x3f8f10688ab647ea14f20f8effa7bb6a66cc2f3982eb4bf20a8c08e05b9dbfb"),
                felt!("0x0"),
                felt!("0x743c83c41ce99ad470aa308823f417b2141e02e04571f5c0004e743556e7faf"),
                felt!("0x4"),
                felt!("0x0"),
                felt!("0x3"),
                felt!("0x9792e6f077846a7a7e98876b6d3be790df2272fa"),
                felt!("0x1"),
                felt!("0x676c69686d2d646973636f7264"),
            ],
            from_address: felt!(
                "0x041a78e741e5af2fec34b695679bc6891742439f7afb8484ecd7766661ad02bf"
            ),
        };

        let udc_event = UdcContractDeployedEvent::cairo_deserialize(&event.data, 0).unwrap();

        assert!(is_cartridge_controller(&udc_event).await.unwrap());
        assert_eq!(
            parse_cairo_short_string(&udc_event.salt).unwrap(),
            "glihm-discord"
        );
    }

    /// Tests the parsing of a calldata that is a false positive (not a Cartridge controller, but similar calldata structure).
    /// Taken from sepolia: <https://sepolia.voyager.online/event/56083_1_0>
    #[tokio::test]
    async fn test_parse_cartridge_controller_calldata_false_positive() {
        let event = Event {
            keys: vec![felt!(
                "0x26b160f10156dea0639bec90696772c640b9706a47f5b8c52ea1abe5858b34d"
            )],
            data: vec![
                felt!("0x4369f9e67071787d6df4170d3c02743516479d876c2a9d0cc3da3f67f044dc2"),
                felt!("0x2f7cc642ce3db18dc24ec5b8df5f8fd306e10786d68316de6aaf9941f81eeca"),
                felt!("0x1"),
                felt!("0x3a4981ca2c6de58d229cf3e2ca3a35135bad75b9f9ee4c3b6cbfc26ce3c2a4c"),
                felt!("0x7"),
                felt!("0x1"),
                felt!("0x5"),
                felt!("0x12470f7aba85c8b81d63137dd5925d6ee114952b"),
                felt!("0x109b4a318a4f5ddcbca6349b45f881b4137deafb"),
                felt!("0x1ea62d73edf8ac05dfcea1a34b9796e937a29eff"),
                felt!("0x2c59617248994d12816ee1fa77ce0a64eeb456bf"),
                felt!("0x83cba8c619fb629b81a65c2e67fe15cf3e3c9747"),
                felt!("0x718b425c79dd2d9b6f91d4735e11652281f00add994897dabda8b824cbae88"),
            ],
            from_address: felt!(
                "0x041a78e741e5af2fec34b695679bc6891742439f7afb8484ecd7766661ad02bf"
            ),
        };

        let udc_event = UdcContractDeployedEvent::cairo_deserialize(&event.data, 0).unwrap();

        assert!(!is_cartridge_controller(&udc_event).await.unwrap());
    }

    /// Tests the controller lookup functionality.
    #[tokio::test]
    async fn test_fetch_controller_username() {
        // Test with a known controller address.
        let known_address = "0x048E13Ef7AB79637afd38a4B022862a7e6F3fd934f194C435D7e7b17bAC06715";
        assert!(fetch_controller_username(known_address)
            .await
            .unwrap()
            .is_some());

        // Test with a non-controller address.
        let unknown_address = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        assert!(fetch_controller_username(unknown_address)
            .await
            .unwrap()
            .is_none());
    }
}
