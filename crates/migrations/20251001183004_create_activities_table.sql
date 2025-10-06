-- Create activities table for tracking user sessions and actions
-- This pre-aggregates transaction data to avoid expensive window function queries
-- Activities are grouped by world address and namespace for better organization
CREATE TABLE IF NOT EXISTS activities (
    id TEXT NOT NULL PRIMARY KEY,  -- Format: {world_address}:{namespace}:{caller_address}:{session_start_timestamp}
    world_address TEXT NOT NULL,
    namespace TEXT NOT NULL,
    caller_address TEXT NOT NULL,
    session_start DATETIME NOT NULL,
    session_end DATETIME NOT NULL,
    action_count INTEGER NOT NULL DEFAULT 1,
    actions TEXT NOT NULL,  -- JSON object mapping action names to call counts: {"approve": 5, "set_slot": 42}
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Index for efficient querying by world and namespace
CREATE INDEX idx_activities_world_namespace ON activities(world_address, namespace, session_end DESC);

-- Index for efficient querying by caller within a world/namespace
CREATE INDEX idx_activities_caller ON activities(world_address, namespace, caller_address, session_end DESC);

-- Index for efficient time-based queries
CREATE INDEX idx_activities_time ON activities(session_end DESC);

-- Index for efficient caller + time lookups
CREATE INDEX idx_activities_caller_time ON activities(world_address, namespace, caller_address, session_start, session_end);
