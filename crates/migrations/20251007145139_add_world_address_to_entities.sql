-- Add world_address column to entities table
-- This allows torii to properly support indexing multiple worlds simultaneously
-- Entity IDs will be scoped by world address to prevent collisions

-- Add world_address to entities
ALTER TABLE entities ADD COLUMN world_address TEXT NOT NULL DEFAULT '';

-- Add world_address to historical_entities if it exists
-- Wrapped in a conditional since this table may not exist in all schemas
CREATE TABLE IF NOT EXISTS entities_historical_temp AS SELECT * FROM entities_historical LIMIT 0;
DROP TABLE IF EXISTS entities_historical_temp;

-- For tables that exist, add world_address
-- entities_historical
ALTER TABLE entities_historical ADD COLUMN world_address TEXT NOT NULL DEFAULT '';

-- event_messages
ALTER TABLE event_messages ADD COLUMN world_address TEXT NOT NULL DEFAULT '';

-- event_messages_historical
ALTER TABLE event_messages_historical ADD COLUMN world_address TEXT NOT NULL DEFAULT '';

-- Create indexes on world_address for efficient filtering
CREATE INDEX idx_entities_world_address ON entities (world_address);
CREATE INDEX idx_entities_historical_world_address ON entities_historical (world_address);
CREATE INDEX idx_event_messages_world_address ON event_messages (world_address);
CREATE INDEX idx_event_messages_historical_world_address ON event_messages_historical (world_address);

-- Create composite index for common query patterns (world_address + id)
CREATE INDEX idx_entities_world_id ON entities (world_address, id);
CREATE INDEX idx_event_messages_world_id ON event_messages (world_address, id);

-- Add world_address to model tables
-- Since models belong to a specific world, track which world registered them
ALTER TABLE models ADD COLUMN world_address TEXT NOT NULL DEFAULT '';
CREATE INDEX idx_models_world_address ON models (world_address);

-- Add world_address to tokens to track which world they belong to
ALTER TABLE tokens ADD COLUMN world_address TEXT NOT NULL DEFAULT '';
CREATE INDEX idx_tokens_world_address ON tokens (world_address);

-- Add world_address to token_balances
ALTER TABLE token_balances ADD COLUMN world_address TEXT NOT NULL DEFAULT '';
CREATE INDEX idx_token_balances_world_address ON token_balances (world_address);
