-- Create activities table for tracking user sessions and actions
-- This pre-aggregates transaction data to avoid expensive window function queries
CREATE TABLE IF NOT EXISTS activities (
    id TEXT NOT NULL PRIMARY KEY,  -- Format: {caller_address}:{session_start_timestamp}
    caller_address TEXT NOT NULL,
    session_start DATETIME NOT NULL,
    session_end DATETIME NOT NULL,
    action_count INTEGER NOT NULL DEFAULT 1,
    entrypoints TEXT NOT NULL,  -- JSON object mapping entrypoint names to call counts: {"approve": 5, "set_slot": 42}
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Index for efficient querying by caller
CREATE INDEX idx_activities_caller ON activities(caller_address, session_end DESC);

-- Index for efficient time-based queries
CREATE INDEX idx_activities_time ON activities(session_end DESC);

-- Index for efficient caller + time lookups
CREATE INDEX idx_activities_caller_time ON activities(caller_address, session_start, session_end);
