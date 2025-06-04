-- Drop the unused last_pending_block_tx column
-- and rename the last_pending_block_contract_tx column to last_pending_block_event_id
ALTER TABLE contracts DROP COLUMN last_pending_block_tx;
ALTER TABLE contracts RENAME COLUMN last_pending_block_contract_tx TO last_pending_block_event_id;