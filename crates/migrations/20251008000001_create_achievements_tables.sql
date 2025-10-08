-- Create achievements table to store achievement definitions
CREATE TABLE IF NOT EXISTS achievements (
    id TEXT PRIMARY KEY NOT NULL,
    world_address TEXT NOT NULL,
    hidden INTEGER NOT NULL DEFAULT 0,
    index_num INTEGER NOT NULL,
    points INTEGER NOT NULL DEFAULT 0,
    start TEXT NOT NULL,
    end TEXT NOT NULL,
    group_name TEXT NOT NULL,
    icon TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    tasks TEXT NOT NULL,  -- JSON array of task definitions
    data TEXT,  -- Additional JSON metadata
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create achievement progressions table to track player progress
CREATE TABLE IF NOT EXISTS achievement_progressions (
    id TEXT PRIMARY KEY NOT NULL,  -- Format: "achievement_id:player_id:task_id"
    achievement_id TEXT NOT NULL,
    player_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    count INTEGER NOT NULL DEFAULT 0,
    completed INTEGER NOT NULL DEFAULT 0,  -- 0 = not completed, 1 = completed
    completed_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (achievement_id) REFERENCES achievements(id) ON DELETE CASCADE
);

-- Create player achievement summary table for aggregated stats
CREATE TABLE IF NOT EXISTS player_achievements (
    id TEXT PRIMARY KEY NOT NULL,  -- Format: "world_address:player_id"
    world_address TEXT NOT NULL,
    player_id TEXT NOT NULL,
    total_points INTEGER NOT NULL DEFAULT 0,
    completed_achievements INTEGER NOT NULL DEFAULT 0,
    total_achievements INTEGER NOT NULL DEFAULT 0,
    completion_percentage REAL NOT NULL DEFAULT 0.0,
    last_achievement_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(world_address, player_id)
);

-- Create indices for efficient queries
CREATE INDEX IF NOT EXISTS idx_achievements_world ON achievements(world_address);
CREATE INDEX IF NOT EXISTS idx_achievements_group ON achievements(group_name);
CREATE INDEX IF NOT EXISTS idx_achievements_index ON achievements(index_num);

CREATE INDEX IF NOT EXISTS idx_progressions_achievement ON achievement_progressions(achievement_id);
CREATE INDEX IF NOT EXISTS idx_progressions_player ON achievement_progressions(player_id);
CREATE INDEX IF NOT EXISTS idx_progressions_task ON achievement_progressions(task_id);
CREATE INDEX IF NOT EXISTS idx_progressions_completed ON achievement_progressions(completed);
CREATE INDEX IF NOT EXISTS idx_progressions_player_achievement ON achievement_progressions(player_id, achievement_id);

CREATE INDEX IF NOT EXISTS idx_player_achievements_world ON player_achievements(world_address);
CREATE INDEX IF NOT EXISTS idx_player_achievements_player ON player_achievements(player_id);
CREATE INDEX IF NOT EXISTS idx_player_achievements_points ON player_achievements(total_points DESC);
CREATE INDEX IF NOT EXISTS idx_player_achievements_completion ON player_achievements(completion_percentage DESC);

