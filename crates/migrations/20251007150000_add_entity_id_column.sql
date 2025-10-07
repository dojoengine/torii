-- Add entity_id column to store just the entity hash (without world_address prefix)
-- This avoids string parsing on every entity conversion and improves query performance

-- Add entity_id columns to all entity-related tables
ALTER TABLE entities ADD COLUMN entity_id TEXT NOT NULL DEFAULT '';
ALTER TABLE entities_historical ADD COLUMN entity_id TEXT NOT NULL DEFAULT '';
ALTER TABLE event_messages ADD COLUMN entity_id TEXT NOT NULL DEFAULT '';
ALTER TABLE event_messages_historical ADD COLUMN entity_id TEXT NOT NULL DEFAULT '';

-- Populate entity_id from id column (extract part after colon)
-- Format: id = "world_address:entity_id", we want just "entity_id"
UPDATE entities 
SET entity_id = CASE 
    WHEN id LIKE '%:%' THEN substr(id, instr(id, ':') + 1)
    ELSE id
END;

UPDATE entities_historical 
SET entity_id = CASE 
    WHEN id LIKE '%:%' THEN substr(id, instr(id, ':') + 1)
    ELSE id
END;

UPDATE event_messages 
SET entity_id = CASE 
    WHEN id LIKE '%:%' THEN substr(id, instr(id, ':') + 1)
    ELSE id
END;

UPDATE event_messages_historical 
SET entity_id = CASE 
    WHEN id LIKE '%:%' THEN substr(id, instr(id, ':') + 1)
    ELSE id
END;

-- Create indexes on entity_id for efficient lookups by non-scoped ID
CREATE INDEX idx_entities_entity_id ON entities (entity_id);
CREATE INDEX idx_entities_historical_entity_id ON entities_historical (entity_id);
CREATE INDEX idx_event_messages_entity_id ON event_messages (entity_id);
CREATE INDEX idx_event_messages_historical_entity_id ON event_messages_historical (entity_id);

-- Composite indexes for queries that filter by world_address + non-scoped entity_id
-- These are more efficient than using world_address + scoped id
CREATE INDEX idx_entities_world_entity ON entities (world_address, entity_id);
CREATE INDEX idx_entities_historical_world_entity ON entities_historical (world_address, entity_id);
CREATE INDEX idx_event_messages_world_entity ON event_messages (world_address, entity_id);
CREATE INDEX idx_event_messages_historical_world_entity ON event_messages_historical (world_address, entity_id);

