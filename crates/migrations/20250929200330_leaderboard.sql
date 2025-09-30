-- Create aggregations table for aggregating and ranking model updates
CREATE TABLE IF NOT EXISTS aggregations (
    id TEXT NOT NULL PRIMARY KEY,
    aggregator_id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    value TEXT NOT NULL,              -- Normalized hex for ordering (zero-padded, biased for signed)
    display_value TEXT NOT NULL,      -- Original value for display (decimal or hex as-is)
    model_id TEXT NOT NULL,
    metadata TEXT,                    -- For Avg aggregation (stores sum and count as JSON)
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Index for efficient querying by entity_id within an aggregator
CREATE INDEX idx_aggregations_entity ON aggregations(aggregator_id, entity_id);

-- Index for efficient value-based queries (critical for ROW_NUMBER() performance)
-- Values are normalized to zero-padded hex (64 chars), so lexicographic ordering = numerical ordering
CREATE INDEX idx_aggregations_value ON aggregations(aggregator_id, value DESC);

-- Index for model_id lookups
CREATE INDEX idx_aggregations_model ON aggregations(model_id);