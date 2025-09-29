-- Create leaderboard table for aggregating and ranking model updates
CREATE TABLE IF NOT EXISTS leaderboard (
    id TEXT NOT NULL PRIMARY KEY,
    leaderboard_id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    score TEXT NOT NULL,
    position INTEGER NOT NULL DEFAULT 0,
    model_id TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Index for efficient querying by leaderboard_id and position
CREATE INDEX idx_leaderboard_id_position ON leaderboard(leaderboard_id, position);

-- Index for efficient querying by entity_id within a leaderboard
CREATE INDEX idx_leaderboard_entity ON leaderboard(leaderboard_id, entity_id);

-- Index for efficient score-based queries
CREATE INDEX idx_leaderboard_score ON leaderboard(leaderboard_id, score DESC);

-- Index for model_id lookups
CREATE INDEX idx_leaderboard_model ON leaderboard(model_id);