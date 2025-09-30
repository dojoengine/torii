-- Optimization migration for entity queries
-- This migration adds critical indexes to improve query performance for large datasets

-- Composite index for entity_model covering query pattern: filtering by model_id and joining to entity_id
-- This is the most critical optimization as queries typically filter by model_id first
CREATE INDEX IF NOT EXISTS idx_entity_model_model_entity ON entity_model (model_id, entity_id);

-- Covering index for common query pattern: model_id + entity_id + historical_counter
-- Allows some queries to be satisfied without accessing the entities table
CREATE INDEX IF NOT EXISTS idx_entity_model_covering ON entity_model (model_id, entity_id, historical_counter);

-- Composite index on entities for efficient ordering and filtering
-- event_id is used for ordering in most queries, combining with id helps with pagination
CREATE INDEX IF NOT EXISTS idx_entities_event_id_id ON entities (event_id DESC, id);

-- Index for executed_at ordering (less common but still needed)
CREATE INDEX IF NOT EXISTS idx_entities_executed_at_id ON entities (executed_at DESC, id);

-- Index for updated_at with id for pagination support
CREATE INDEX IF NOT EXISTS idx_entities_updated_at_id ON entities (updated_at DESC, id);

-- Composite index for entities_historical for better performance
CREATE INDEX IF NOT EXISTS idx_entities_historical_model_event ON entities_historical (model_id, event_id DESC);
CREATE INDEX IF NOT EXISTS idx_entities_historical_id_model ON entities_historical (id, model_id);

-- Add statistics for query planner
ANALYZE entity_model;
ANALYZE entities;
ANALYZE entities_historical;

