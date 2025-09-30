-- Create leaderboard table for aggregating and ranking model updates
CREATE TABLE IF NOT EXISTS leaderboard (
    id TEXT NOT NULL PRIMARY KEY,
    leaderboard_id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    score TEXT NOT NULL,              -- Normalized hex for ordering (zero-padded, biased for signed)
    display_score TEXT NOT NULL,      -- Original value for display (decimal or hex as-is)
    model_id TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Index for efficient querying by entity_id within a leaderboard
CREATE INDEX idx_leaderboard_entity ON leaderboard(leaderboard_id, entity_id);

-- Index for efficient score-based queries (critical for ROW_NUMBER() performance)
-- Scores are normalized to zero-padded hex (64 chars), so lexicographic ordering = numerical ordering
CREATE INDEX idx_leaderboard_score ON leaderboard(leaderboard_id, score DESC);

-- Index for model_id lookups
CREATE INDEX idx_leaderboard_model ON leaderboard(model_id);