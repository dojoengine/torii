-- Add world_address column to entities table
-- This allows torii to properly support indexing multiple worlds simultaneously
-- Entity IDs will be scoped by world address to prevent collisions

-- Add world_address columns to all relevant tables
ALTER TABLE entities ADD COLUMN world_address TEXT NOT NULL DEFAULT '';
ALTER TABLE entities_historical ADD COLUMN world_address TEXT NOT NULL DEFAULT '';
ALTER TABLE event_messages ADD COLUMN world_address TEXT NOT NULL DEFAULT '';
ALTER TABLE event_messages_historical ADD COLUMN world_address TEXT NOT NULL DEFAULT '';
ALTER TABLE models ADD COLUMN world_address TEXT NOT NULL DEFAULT '';

-- Populate world_address for all tables from contracts table (get first WORLD contract)
-- This ensures existing databases have a valid world_address
UPDATE entities
SET world_address = (SELECT contract_address FROM contracts WHERE contract_type = 'WORLD' LIMIT 1)
WHERE world_address = '' OR world_address IS NULL;

UPDATE entities_historical
SET world_address = (SELECT contract_address FROM contracts WHERE contract_type = 'WORLD' LIMIT 1)
WHERE world_address = '' OR world_address IS NULL;

UPDATE event_messages
SET world_address = (SELECT contract_address FROM contracts WHERE contract_type = 'WORLD' LIMIT 1)
WHERE world_address = '' OR world_address IS NULL;

UPDATE event_messages_historical
SET world_address = (SELECT contract_address FROM contracts WHERE contract_type = 'WORLD' LIMIT 1)
WHERE world_address = '' OR world_address IS NULL;

UPDATE models
SET world_address = (SELECT contract_address FROM contracts WHERE contract_type = 'WORLD' LIMIT 1)
WHERE world_address = '' OR world_address IS NULL;

-- Create indexes on world_address for efficient filtering
CREATE INDEX idx_entities_world_address ON entities (world_address);
CREATE INDEX idx_entities_historical_world_address ON entities_historical (world_address);
CREATE INDEX idx_event_messages_world_address ON event_messages (world_address);
CREATE INDEX idx_event_messages_historical_world_address ON event_messages_historical (world_address);
CREATE INDEX idx_models_world_address ON models (world_address);
