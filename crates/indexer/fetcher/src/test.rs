use std::collections::HashMap;
use std::sync::Arc;

use starknet::core::types::{BlockId, BlockWithReceipts, MaybePendingBlockWithReceipts};
use starknet::macros::felt;
use starknet::providers::jsonrpc::HttpTransport;
use starknet::providers::{JsonRpcClient, Provider};
use starknet_crypto::Felt;
use torii_sqlite::Cursor;
use url::Url;

use crate::{Fetcher, FetcherConfig};

const CARTRIDGE_NODE_MAINNET: &str = "https://api.cartridge.gg/x/starknet/mainnet";
const ETERNUM_ADDRESS: Felt =
    felt!("0x5c6d0020a9927edca9ddc984b97305439c0b32a1ec8d3f0eaf6291074cc9799");

/// Get a block with receipts from the provider.
///
/// To avoid fetching here, we may use a pre-fetched file instead.
/// This however requires more setup and more data committed to the repo.
async fn get_block_with_receipts<P: Provider + Send + Sync + std::fmt::Debug + 'static>(
    provider: &P,
    block_number: u64,
) -> BlockWithReceipts {
    match provider
        .get_block_with_receipts(BlockId::Number(block_number))
        .await
        .unwrap()
    {
        MaybePendingBlockWithReceipts::Block(block) => block,
        _ => panic!("Expected a block, got a pending block"),
    }
}

#[tokio::test]
async fn test_range_one_block() {
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(
        Url::parse(CARTRIDGE_NODE_MAINNET).unwrap(),
    )));

    let eternum_block = 1435856;

    let fetcher = Fetcher::new(provider.clone(), FetcherConfig {
        blocks_chunk_size: 1,
        ..Default::default()
    });

    // To index 1435856, the cursor must actually be one block behind.
    let cursors = HashMap::from([(
        ETERNUM_ADDRESS,
        Cursor {
            last_pending_block_tx: None,
            head: Some(eternum_block - 1),
            last_block_timestamp: None,
        },
    )]);

    let result = fetcher.fetch(&cursors).await.unwrap();

    let expected = get_block_with_receipts(&provider, eternum_block).await;

    let torii_block = &result.range.blocks[&eternum_block];

    // Expecting the block right after the cursor head + the chunk size.
    assert_eq!(result.range.blocks.len(), 2);
    assert_eq!(torii_block.block_hash, Some(expected.block_hash));
    assert_eq!(torii_block.timestamp, expected.timestamp);

    for (torii_tx_hash, _torii_tx) in torii_block.transactions.iter() {
        let expected_tx = expected
            .transactions
            .iter()
            .find(|tx| tx.transaction.transaction_hash() == torii_tx_hash);
        assert!(expected_tx.is_some());
        assert_eq!(
            torii_tx_hash,
            expected_tx.unwrap().transaction.transaction_hash()
        );
    }
}

// TODO: test with indexing flags to verify transactions content and raw events.
// Test with several blocks in the range.
