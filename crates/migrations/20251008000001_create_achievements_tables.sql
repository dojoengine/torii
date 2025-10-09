-- Create achievements table to store achievement definitions
CREATE TABLE IF NOT EXISTS achievements (
    id TEXT PRIMARY KEY NOT NULL,  -- Format: "world_address:namespace:entity_id"
    world_address TEXT NOT NULL,
    namespace TEXT NOT NULL,
    entity_id TEXT NOT NULL,  -- Original entity ID from the chain
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
    total_completions INTEGER NOT NULL DEFAULT 0,  -- How many players completed this achievement
    completion_rate REAL NOT NULL DEFAULT 0.0,  -- Percentage of active players who completed (0.0-100.0)
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create achievement tasks table to map tasks to their parent achievement
CREATE TABLE IF NOT EXISTS achievement_tasks (
    id TEXT PRIMARY KEY NOT NULL,  -- Format: "world_address:namespace:achievement_id:task_id"
    achievement_id TEXT NOT NULL,
    task_id TEXT NOT NULL,  -- The task ID from on-chain
    world_address TEXT NOT NULL,
    namespace TEXT NOT NULL,
    description TEXT NOT NULL,
    total INTEGER NOT NULL,  -- Target count to complete
    total_completions INTEGER NOT NULL DEFAULT 0,  -- How many players completed this task
    completion_rate REAL NOT NULL DEFAULT 0.0,  -- Percentage of active players who completed (0.0-100.0)
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (achievement_id) REFERENCES achievements(id) ON DELETE CASCADE
);

-- Create achievement progressions table to track player progress
CREATE TABLE IF NOT EXISTS achievement_progressions (
    id TEXT PRIMARY KEY NOT NULL,  -- Format: "world_address:namespace:task_id:player_id"
    task_id TEXT NOT NULL,  -- References achievement_tasks.task_id (NOT the composite ID)
    world_address TEXT NOT NULL,
    namespace TEXT NOT NULL,
    player_id TEXT NOT NULL,
    count INTEGER NOT NULL DEFAULT 0,
    completed INTEGER NOT NULL DEFAULT 0,  -- 0 = not completed, 1 = completed
    completed_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create player achievement summary table for aggregated stats
CREATE TABLE IF NOT EXISTS player_achievements (
    id TEXT PRIMARY KEY NOT NULL,  -- Format: "world_address:namespace:player_id"
    world_address TEXT NOT NULL,
    namespace TEXT NOT NULL,
    player_id TEXT NOT NULL,
    total_points INTEGER NOT NULL DEFAULT 0,
    completed_achievements INTEGER NOT NULL DEFAULT 0,
    total_achievements INTEGER NOT NULL DEFAULT 0,
    completion_percentage REAL NOT NULL DEFAULT 0.0,
    last_achievement_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create indices for efficient queries
CREATE INDEX IF NOT EXISTS idx_achievements_world ON achievements(world_address);
CREATE INDEX IF NOT EXISTS idx_achievements_namespace ON achievements(namespace);
CREATE INDEX IF NOT EXISTS idx_achievements_world_namespace ON achievements(world_address, namespace);
CREATE INDEX IF NOT EXISTS idx_achievements_group ON achievements(group_name);
CREATE INDEX IF NOT EXISTS idx_achievements_index ON achievements(index_num);
CREATE INDEX IF NOT EXISTS idx_achievements_completion_rate ON achievements(world_address, namespace, completion_rate DESC);
CREATE INDEX IF NOT EXISTS idx_achievements_rarity ON achievements(world_address, namespace, total_completions ASC);  -- For finding rarest achievements

CREATE INDEX IF NOT EXISTS idx_achievement_tasks_achievement ON achievement_tasks(achievement_id);
CREATE INDEX IF NOT EXISTS idx_achievement_tasks_task ON achievement_tasks(world_address, namespace, task_id);
CREATE INDEX IF NOT EXISTS idx_achievement_tasks_completion_rate ON achievement_tasks(world_address, namespace, completion_rate DESC);
CREATE INDEX IF NOT EXISTS idx_achievement_tasks_rarity ON achievement_tasks(world_address, namespace, total_completions ASC);  -- For finding hardest tasks

CREATE INDEX IF NOT EXISTS idx_progressions_task ON achievement_progressions(world_address, namespace, task_id);
CREATE INDEX IF NOT EXISTS idx_progressions_player ON achievement_progressions(player_id);
CREATE INDEX IF NOT EXISTS idx_progressions_completed ON achievement_progressions(completed);
CREATE INDEX IF NOT EXISTS idx_progressions_player_namespace ON achievement_progressions(world_address, namespace, player_id);

CREATE INDEX IF NOT EXISTS idx_player_achievements_world ON player_achievements(world_address);
CREATE INDEX IF NOT EXISTS idx_player_achievements_namespace ON player_achievements(namespace);
CREATE INDEX IF NOT EXISTS idx_player_achievements_world_namespace ON player_achievements(world_address, namespace);
CREATE INDEX IF NOT EXISTS idx_player_achievements_player ON player_achievements(player_id);
CREATE INDEX IF NOT EXISTS idx_player_achievements_points ON player_achievements(world_address, namespace, total_points DESC);
CREATE INDEX IF NOT EXISTS idx_player_achievements_completion ON player_achievements(world_address, namespace, completion_percentage DESC);

