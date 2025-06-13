-- Add migration script here
ALTER TABLE contracts RENAME COLUMN last_pending_block_event_id TO last_pending_block_tx;