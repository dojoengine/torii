-- Add world_address column to entities table
-- This allows torii to properly support indexing multiple worlds simultaneously
-- Entity IDs will be scoped by world address to prevent collisions

-- Add world_address columns to all relevant tables
-- Also add unscoped ID columns for efficient querying
ALTER TABLE entities ADD COLUMN world_address TEXT NOT NULL DEFAULT '';
ALTER TABLE entities ADD COLUMN entity_id TEXT NOT NULL DEFAULT '';
ALTER TABLE entities_historical ADD COLUMN world_address TEXT NOT NULL DEFAULT '';
ALTER TABLE entities_historical ADD COLUMN entity_id TEXT NOT NULL DEFAULT '';
ALTER TABLE event_messages ADD COLUMN world_address TEXT NOT NULL DEFAULT '';
ALTER TABLE event_messages ADD COLUMN entity_id TEXT NOT NULL DEFAULT '';
ALTER TABLE event_messages_historical ADD COLUMN world_address TEXT NOT NULL DEFAULT '';
ALTER TABLE event_messages_historical ADD COLUMN entity_id TEXT NOT NULL DEFAULT '';
ALTER TABLE models ADD COLUMN world_address TEXT NOT NULL DEFAULT '';
ALTER TABLE models ADD COLUMN model_selector TEXT NOT NULL DEFAULT '';

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

-- Create indexes on unscoped IDs for queries
CREATE INDEX idx_entities_entity_id ON entities (entity_id);
CREATE INDEX idx_entities_historical_entity_id ON entities_historical (entity_id);
CREATE INDEX idx_event_messages_entity_id ON event_messages (entity_id);
CREATE INDEX idx_event_messages_historical_entity_id ON event_messages_historical (entity_id);
CREATE INDEX idx_models_model_selector ON models (model_selector);

-- Create composite indexes for common query patterns (world_address + unscoped_id)
CREATE INDEX idx_entities_world_entity ON entities (world_address, entity_id);
CREATE INDEX idx_entities_historical_world_entity ON entities_historical (world_address, entity_id);
CREATE INDEX idx_event_messages_world_entity ON event_messages (world_address, entity_id);
CREATE INDEX idx_event_messages_historical_world_entity ON event_messages_historical (world_address, entity_id);
CREATE INDEX idx_models_world_selector ON models (world_address, model_selector);
