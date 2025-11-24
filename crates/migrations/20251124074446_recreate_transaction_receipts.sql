-- Drop the old unused transaction_receipts table
DROP TABLE IF EXISTS transaction_receipts;

-- Recreate transaction_receipts table with comprehensive fields
CREATE TABLE transaction_receipts (
    id TEXT NOT NULL PRIMARY KEY,
    transaction_hash TEXT NOT NULL,
    actual_fee_amount TEXT NOT NULL,
    actual_fee_unit TEXT NOT NULL,
    execution_status TEXT NOT NULL,
    finality_status TEXT NOT NULL,
    revert_reason TEXT,
    execution_resources TEXT NOT NULL,
    block_hash TEXT NOT NULL,
    block_number INTEGER NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (transaction_hash)
);

-- Add index on block_number for efficient queries
CREATE INDEX idx_transaction_receipts_block_number ON transaction_receipts(block_number);

