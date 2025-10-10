use chrono::Utc;
use dojo_types::schema::Ty;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, Transaction as SqlxTransaction};
use torii_sqlite_types::PlayerAchievementStats;
use tracing::{info, warn};

use crate::{error::ParseError, executor::error::ExecutorQueryError};

pub(crate) const LOG_TARGET: &str = "torii::sqlite::executor::achievement";

pub type QueryResult<T> = std::result::Result<T, ExecutorQueryError>;

/// Represents a task definition from the trophy creation model
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AchievementTaskDefinition {
    pub id: String,
    pub description: String,
    pub total: u32,
}

/// Process achievement registration (trophy creation)
/// This is called when a new achievement is registered in the system
pub async fn register_achievement(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    world_address: &str,
    namespace: &str,
    entity: &Ty,
) -> QueryResult<Option<String>> {
    // Extract achievement data from the entity
    let entity_id = extract_field_value(entity, "id")?.ok_or_else(|| {
        ExecutorQueryError::LeaderboardFieldExtraction(
            "Could not extract 'id' from achievement entity".to_string(),
        )
    })?;

    // Construct globally unique achievement ID: world:namespace:entity_id
    let achievement_id = format!("{}:{}:{}", world_address, namespace, entity_id);

    let hidden = extract_field_value(entity, "hidden")?
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(0);

    let index_num = extract_field_value(entity, "index")?
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(0);

    let points = extract_field_value(entity, "points")?
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(0);

    let start = extract_field_value(entity, "start")?.unwrap_or_default();
    let end = extract_field_value(entity, "end")?.unwrap_or_default();
    let group_name = extract_field_value(entity, "group").unwrap_or_default();
    let icon = extract_field_value(entity, "icon")?.unwrap_or_default();
    let title = extract_field_value(entity, "title")?.unwrap_or_default();
    let description = extract_field_value(entity, "description")?.unwrap_or_default();
    let tasks: String = extract_field_value(entity, "tasks")?.unwrap_or_else(|| "[]".to_string());
    let data = extract_field_value(entity, "data")?;

    // Upsert the achievement
    sqlx::query(
        "INSERT INTO achievements 
         (id, world_address, namespace, entity_id, hidden, index_num, points, start, end, group_name, icon, title, description, tasks, data) 
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET
         hidden=EXCLUDED.hidden,
         index_num=EXCLUDED.index_num,
         points=EXCLUDED.points,
         start=EXCLUDED.start,
         end=EXCLUDED.end,
         group_name=EXCLUDED.group_name,
         icon=EXCLUDED.icon,
         title=EXCLUDED.title,
         description=EXCLUDED.description,
         tasks=EXCLUDED.tasks,
         data=EXCLUDED.data,
         updated_at=CURRENT_TIMESTAMP",
    )
    .bind(&achievement_id)
    .bind(world_address)
    .bind(namespace)
    .bind(&entity_id)
    .bind(hidden)
    .bind(index_num)
    .bind(points)
    .bind(&start)
    .bind(&end)
    .bind(&group_name)
    .bind(&icon)
    .bind(&title)
    .bind(&description)
    .bind(&tasks)
    .bind(&data)
    .execute(&mut **tx)
    .await?;

    // Parse and insert tasks into achievement_tasks table
    let task_definitions: Vec<AchievementTaskDefinition> =
        serde_json::from_str(&tasks).unwrap_or_default();

    for task in &task_definitions {
        let task_composite_id = format!(
            "{}:{}:{}:{}",
            world_address, namespace, achievement_id, task.id
        );

        sqlx::query(
            "INSERT INTO achievement_tasks 
             (id, achievement_id, task_id, world_address, namespace, description, total) 
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
             description=EXCLUDED.description,
             total=EXCLUDED.total",
        )
        .bind(&task_composite_id)
        .bind(&achievement_id)
        .bind(&task.id)
        .bind(world_address)
        .bind(namespace)
        .bind(&task.description)
        .bind(task.total as i32)
        .execute(&mut **tx)
        .await?;
    }

    info!(
        target: LOG_TARGET,
        achievement_id = %achievement_id,
        world = %world_address,
        namespace = %namespace,
        entity_id = %entity_id,
        title = %title,
        points = %points,
        task_count = %task_definitions.len(),
        "Registered achievement with tasks"
    );

    Ok(Some(achievement_id))
}

/// Process achievement progression (trophy progression)
/// This is called when a player makes progress on a task
pub async fn update_achievement_progression(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    world_address: &str,
    namespace: &str,
    entity: &Ty,
) -> QueryResult<Option<AchievementProgressionResult>> {
    // Extract player_id and task_id from the entity
    let player_id = extract_field_value(entity, "player_id")?.ok_or_else(|| {
        ExecutorQueryError::LeaderboardFieldExtraction(
            "Could not extract 'player_id' or 'player' from progression entity".to_string(),
        )
    })?;

    let task_id = extract_field_value(entity, "task_id")?.ok_or_else(|| {
        ExecutorQueryError::LeaderboardFieldExtraction(
            "Could not extract 'task_id' or 'task' from progression entity".to_string(),
        )
    })?;

    let count = extract_field_value(entity, "count")?
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(1);

    // Look up the achievement_id and target from the achievement_tasks table
    let task_info: Option<(String, i32)> = sqlx::query_as(
        "SELECT achievement_id, total FROM achievement_tasks 
         WHERE world_address = ? AND namespace = ? AND task_id = ?",
    )
    .bind(world_address)
    .bind(namespace)
    .bind(&task_id)
    .fetch_optional(&mut **tx)
    .await?;

    let (achievement_id, task_target) = if let Some((ach_id, target)) = task_info {
        (ach_id, target)
    } else {
        warn!(
            target: LOG_TARGET,
            task_id = %task_id,
            world = %world_address,
            namespace = %namespace,
            "Task not found in achievement_tasks table"
        );
        return Ok(None);
    };

    let progression_id = format!("{}:{}:{}:{}", world_address, namespace, task_id, player_id);

    // Check if task is completed
    let completed = count >= task_target;

    let completed_at = if completed { Some(Utc::now()) } else { None };

    // Upsert the progression
    sqlx::query(
        "INSERT INTO achievement_progressions 
         (id, task_id, world_address, namespace, player_id, count, completed, completed_at) 
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET
         count=EXCLUDED.count,
         completed=EXCLUDED.completed,
         completed_at=EXCLUDED.completed_at,
         updated_at=CURRENT_TIMESTAMP",
    )
    .bind(&progression_id)
    .bind(&task_id)
    .bind(world_address)
    .bind(namespace)
    .bind(&player_id)
    .bind(count)
    .bind(if completed { 1 } else { 0 })
    .bind(completed_at)
    .execute(&mut **tx)
    .await?;

    info!(
        target: LOG_TARGET,
        achievement_id = %achievement_id,
        player_id = %player_id,
        task_id = %task_id,
        count = %count,
        target = %task_target,
        completed = %completed,
        "Updated achievement progression"
    );

    // Calculate overall achievement completion for this player
    let overall_status = calculate_achievement_status(tx, &achievement_id, &player_id).await?;

    // Update task completion stats if this task was just completed
    if completed {
        update_task_completion_stats(tx, world_address, namespace, &task_id).await?;
    }

    // Update achievement completion stats if this achievement was just completed
    if overall_status.completed {
        update_achievement_completion_stats(tx, &achievement_id).await?;
    }

    // Update player achievement stats on every progression
    // This ensures the stats table always reflects current progress
    update_player_achievement_stats(tx, world_address, namespace, &player_id).await?;

    Ok(Some(AchievementProgressionResult {
        progression_id,
        achievement_id: achievement_id.to_string(),
        player_id,
        task_id,
        count,
        task_completed: completed,
        achievement_completed: overall_status.completed,
        achievement_progress: overall_status.progress,
        total_points: overall_status.points,
    }))
}

/// Result of an achievement progression update
#[derive(Debug, Clone)]
pub struct AchievementProgressionResult {
    pub progression_id: String,
    pub achievement_id: String,
    pub player_id: String,
    pub task_id: String,
    pub count: i32,
    pub task_completed: bool,
    pub achievement_completed: bool,
    pub achievement_progress: f32, // 0.0 to 1.0
    pub total_points: i32,
}

/// Overall achievement status for a player
#[derive(Debug)]
struct AchievementStatus {
    completed: bool,
    progress: f32,
    points: i32,
}

/// Calculate the overall completion status of an achievement for a player
async fn calculate_achievement_status(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    achievement_id: &str,
    player_id: &str,
) -> QueryResult<AchievementStatus> {
    // Get achievement tasks
    let achievement_data: Option<(String, i32)> =
        sqlx::query_as("SELECT tasks, points FROM achievements WHERE id = ?")
            .bind(achievement_id)
            .fetch_optional(&mut **tx)
            .await?;

    let (tasks_json, points) = achievement_data.unwrap_or_else(|| ("[]".to_string(), 0));

    // Parse tasks to get total count
    let tasks: Vec<AchievementTaskDefinition> =
        serde_json::from_str(&tasks_json).unwrap_or_default();
    let total_tasks = tasks.len();

    if total_tasks == 0 {
        return Ok(AchievementStatus {
            completed: false,
            progress: 0.0,
            points: 0,
        });
    }

    // Count completed tasks for this player by joining through achievement_tasks
    let completed_tasks: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM achievement_progressions ap
         JOIN achievement_tasks at ON at.task_id = ap.task_id 
             AND at.world_address = ap.world_address 
             AND at.namespace = ap.namespace
         WHERE at.achievement_id = ? AND ap.player_id = ? AND ap.completed = 1",
    )
    .bind(achievement_id)
    .bind(player_id)
    .fetch_one(&mut **tx)
    .await?;

    let progress = completed_tasks as f32 / total_tasks as f32;
    let completed = completed_tasks as usize == total_tasks;

    Ok(AchievementStatus {
        completed,
        progress,
        points: if completed { points } else { 0 },
    })
}

/// Get all achievements for a player with their completion status
pub async fn get_player_achievements(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    world_address: &str,
    namespace: &str,
    player_id: &str,
) -> QueryResult<Vec<PlayerAchievement>> {
    // Get all achievements for this world and namespace
    let achievements: Vec<(String, String, i32, String)> = sqlx::query_as(
        "SELECT id, title, points, tasks FROM achievements WHERE world_address = ? AND namespace = ? ORDER BY index_num"
    )
    .bind(world_address)
    .bind(namespace)
    .fetch_all(&mut **tx)
    .await?;

    let mut result = Vec::new();

    for (achievement_id, title, points, _tasks_json) in achievements {
        let status = calculate_achievement_status(tx, &achievement_id, player_id).await?;

        result.push(PlayerAchievement {
            achievement_id,
            title,
            points,
            completed: status.completed,
            progress: status.progress,
            earned_points: status.points,
        });
    }

    Ok(result)
}

/// Represents a player's achievement with completion status
#[derive(Debug, Clone)]
pub struct PlayerAchievement {
    pub achievement_id: String,
    pub title: String,
    pub points: i32,
    pub completed: bool,
    pub progress: f32,
    pub earned_points: i32,
}

/// Get total achievement points for a player across all achievements
pub async fn get_player_total_points(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    world_address: &str,
    namespace: &str,
    player_id: &str,
) -> QueryResult<i32> {
    let achievements = get_player_achievements(tx, world_address, namespace, player_id).await?;
    Ok(achievements.iter().map(|a| a.earned_points).sum())
}

/// Update aggregated player achievement statistics
/// This recalculates and updates the player_achievements table
pub async fn update_player_achievement_stats(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    world_address: &str,
    namespace: &str,
    player_id: &str,
) -> QueryResult<PlayerAchievementStats> {
    let stats_id = format!("{}:{}:{}", world_address, namespace, player_id);

    // Get total number of achievements for this world and namespace
    let total_achievements: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM achievements WHERE world_address = ? AND namespace = ?",
    )
    .bind(world_address)
    .bind(namespace)
    .fetch_one(&mut **tx)
    .await?;

    // Get all player achievements with completion status
    let player_achievements =
        get_player_achievements(tx, world_address, namespace, player_id).await?;

    // Calculate stats
    let completed_achievements = player_achievements.iter().filter(|a| a.completed).count() as i32;
    let total_points: i32 = player_achievements.iter().map(|a| a.earned_points).sum();
    let completion_percentage = if total_achievements > 0 {
        (completed_achievements as f64 / total_achievements as f64) * 100.0
    } else {
        0.0
    };

    // Get timestamp of last completed task across all achievements
    let last_achievement_at: Option<chrono::DateTime<Utc>> = sqlx::query_scalar(
        "SELECT MAX(ap.completed_at)
         FROM achievement_progressions ap
         WHERE ap.world_address = ? AND ap.namespace = ? AND ap.player_id = ? AND ap.completed = 1",
    )
    .bind(world_address)
    .bind(namespace)
    .bind(player_id)
    .fetch_optional(&mut **tx)
    .await?
    .flatten();

    // Upsert player stats
    sqlx::query(
        "INSERT INTO player_achievements 
         (id, world_address, namespace, player_id, total_points, completed_achievements, total_achievements, 
          completion_percentage, last_achievement_at) 
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET
         total_points=EXCLUDED.total_points,
         completed_achievements=EXCLUDED.completed_achievements,
         total_achievements=EXCLUDED.total_achievements,
         completion_percentage=EXCLUDED.completion_percentage,
         last_achievement_at=EXCLUDED.last_achievement_at,
         updated_at=CURRENT_TIMESTAMP",
    )
    .bind(&stats_id)
    .bind(world_address)
    .bind(namespace)
    .bind(player_id)
    .bind(total_points)
    .bind(completed_achievements)
    .bind(total_achievements)
    .bind(completion_percentage)
    .bind(last_achievement_at)
    .execute(&mut **tx)
    .await?;

    info!(
        target: LOG_TARGET,
        world = %world_address,
        namespace = %namespace,
        player = %player_id,
        total_points = %total_points,
        completed = %completed_achievements,
        total = %total_achievements,
        "Updated player achievement stats"
    );

    // Fetch the created/updated stats from the database to ensure we have all fields
    let stats: PlayerAchievementStats = sqlx::query_as(
        "SELECT id, world_address, namespace, player_id, total_points, completed_achievements, 
             total_achievements, completion_percentage, last_achievement_at, created_at, updated_at
             FROM player_achievements 
             WHERE id = ?",
    )
    .bind(&stats_id)
    .fetch_one(&mut **tx)
    .await?;

    Ok(stats)
}

/// Get player achievement statistics
pub async fn get_player_stats(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    world_address: &str,
    namespace: &str,
    player_id: &str,
) -> QueryResult<Option<PlayerAchievementStats>> {
    let stats: Option<(
        String,
        String,
        String,
        String,
        i32,
        i32,
        i32,
        f64,
        Option<chrono::DateTime<Utc>>,
        chrono::DateTime<Utc>,
        chrono::DateTime<Utc>,
    )> = sqlx::query_as(
        "SELECT id, world_address, namespace, player_id, total_points, completed_achievements, 
             total_achievements, completion_percentage, last_achievement_at, created_at, updated_at
             FROM player_achievements 
             WHERE world_address = ? AND namespace = ? AND player_id = ?",
    )
    .bind(world_address)
    .bind(namespace)
    .bind(player_id)
    .fetch_optional(&mut **tx)
    .await?;

    Ok(stats.map(
        |(
            id,
            world_address,
            namespace,
            player_id,
            total_points,
            completed_achievements,
            total_achievements,
            completion_percentage,
            last_achievement_at,
            created_at,
            updated_at,
        )| {
            PlayerAchievementStats {
                id,
                world_address,
                namespace,
                player_id,
                total_points,
                completed_achievements,
                total_achievements,
                completion_percentage,
                last_achievement_at,
                created_at,
                updated_at,
            }
        },
    ))
}

/// Get achievement leaderboard (top players by points)
pub async fn get_achievement_leaderboard(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    world_address: &str,
    namespace: &str,
    limit: i64,
) -> QueryResult<Vec<PlayerAchievementStats>> {
    let results: Vec<(
        String,
        String,
        String,
        String,
        i32,
        i32,
        i32,
        f64,
        Option<chrono::DateTime<Utc>>,
        chrono::DateTime<Utc>,
        chrono::DateTime<Utc>,
    )> = sqlx::query_as(
        "SELECT id, world_address, namespace, player_id, total_points, completed_achievements, 
             total_achievements, completion_percentage, last_achievement_at, created_at, updated_at
             FROM player_achievements 
             WHERE world_address = ? AND namespace = ?
             ORDER BY total_points DESC, completed_achievements DESC
             LIMIT ?",
    )
    .bind(world_address)
    .bind(namespace)
    .bind(limit)
    .fetch_all(&mut **tx)
    .await?;

    Ok(results
        .into_iter()
        .map(
            |(
                id,
                world_address,
                namespace,
                player_id,
                total_points,
                completed_achievements,
                total_achievements,
                completion_percentage,
                last_achievement_at,
                created_at,
                updated_at,
            )| {
                PlayerAchievementStats {
                    id,
                    world_address,
                    namespace,
                    player_id,
                    total_points,
                    completed_achievements,
                    total_achievements,
                    completion_percentage,
                    last_achievement_at,
                    created_at,
                    updated_at,
                }
            },
        )
        .collect())
}

/// Update completion statistics for a specific task
/// Calculates how many players have completed this task and the completion rate
async fn update_task_completion_stats(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    world_address: &str,
    namespace: &str,
    task_id: &str,
) -> QueryResult<()> {
    // Count unique players who completed this task
    let total_completions: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT player_id) FROM achievement_progressions 
         WHERE world_address = ? AND namespace = ? AND task_id = ? AND completed = 1",
    )
    .bind(world_address)
    .bind(namespace)
    .bind(task_id)
    .fetch_one(&mut **tx)
    .await?;

    // Count total unique players who have any progression in this namespace
    let total_players: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT player_id) FROM achievement_progressions 
         WHERE world_address = ? AND namespace = ?",
    )
    .bind(world_address)
    .bind(namespace)
    .fetch_one(&mut **tx)
    .await?;

    let completion_rate = if total_players > 0 {
        (total_completions as f64 / total_players as f64) * 100.0
    } else {
        0.0
    };

    // Update the task stats
    sqlx::query(
        "UPDATE achievement_tasks 
         SET total_completions = ?, completion_rate = ? 
         WHERE world_address = ? AND namespace = ? AND task_id = ?",
    )
    .bind(total_completions)
    .bind(completion_rate)
    .bind(world_address)
    .bind(namespace)
    .bind(task_id)
    .execute(&mut **tx)
    .await?;

    info!(
        target: LOG_TARGET,
        world = %world_address,
        namespace = %namespace,
        task_id = %task_id,
        total_completions = %total_completions,
        completion_rate = %completion_rate,
        "Updated task completion stats"
    );

    Ok(())
}

/// Update completion statistics for a specific achievement
/// Calculates how many players have completed all tasks in this achievement
async fn update_achievement_completion_stats(
    tx: &mut SqlxTransaction<'_, Sqlite>,
    achievement_id: &str,
) -> QueryResult<()> {
    // Parse world_address and namespace from achievement_id (format: world:namespace:entity_id)
    let parts: Vec<&str> = achievement_id.split(':').collect();
    if parts.len() < 3 {
        warn!(
            target: LOG_TARGET,
            achievement_id = %achievement_id,
            "Invalid achievement_id format for stats update"
        );
        return Ok(());
    }
    let world_address = parts[0];
    let namespace = parts[1];

    // Get all task IDs for this achievement
    let task_ids: Vec<String> =
        sqlx::query_scalar("SELECT task_id FROM achievement_tasks WHERE achievement_id = ?")
            .bind(achievement_id)
            .fetch_all(&mut **tx)
            .await?;

    if task_ids.is_empty() {
        return Ok(());
    }

    // Count players who have completed ALL tasks for this achievement
    // A player has completed the achievement if they have completed all its tasks
    let placeholders = task_ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
    let query = format!(
        "SELECT COUNT(DISTINCT player_id) FROM achievement_progressions 
         WHERE world_address = ? AND namespace = ? AND task_id IN ({}) AND completed = 1
         GROUP BY player_id 
         HAVING COUNT(DISTINCT task_id) = ?",
        placeholders
    );

    let mut query_builder = sqlx::query_scalar::<_, i64>(&query);
    query_builder = query_builder.bind(world_address).bind(namespace);
    for task_id in &task_ids {
        query_builder = query_builder.bind(task_id);
    }
    query_builder = query_builder.bind(task_ids.len() as i64);

    let completions: Vec<i64> = query_builder.fetch_all(&mut **tx).await?;
    let total_completions = completions.len() as i64;

    // Count total unique players who have any progression in this namespace
    let total_players: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT player_id) FROM achievement_progressions 
         WHERE world_address = ? AND namespace = ?",
    )
    .bind(world_address)
    .bind(namespace)
    .fetch_one(&mut **tx)
    .await?;

    let completion_rate = if total_players > 0 {
        (total_completions as f64 / total_players as f64) * 100.0
    } else {
        0.0
    };

    // Update the achievement stats
    sqlx::query(
        "UPDATE achievements 
         SET total_completions = ?, completion_rate = ? 
         WHERE id = ?",
    )
    .bind(total_completions)
    .bind(completion_rate)
    .bind(achievement_id)
    .execute(&mut **tx)
    .await?;

    info!(
        target: LOG_TARGET,
        achievement_id = %achievement_id,
        total_completions = %total_completions,
        completion_rate = %completion_rate,
        "Updated achievement completion stats"
    );

    Ok(())
}

/// Helper function to extract a field value from a Ty entity
pub fn extract_field_value(ty: &Ty, field_name: &str) -> QueryResult<Option<String>> {
    Ok(match ty {
        Ty::Struct(s) => {
            for member in &s.children {
                if member.name == field_name {
                    return ty_to_string(&member.ty);
                }
            }
            None
        }
        Ty::Primitive(p) => Some(p.to_sql_value()),
        Ty::Enum(e) => Some(e.to_sql_value()),
        Ty::ByteArray(b) => Some(b.clone()),
        _ => None,
    })
}

/// Helper to convert Ty to string value
fn ty_to_string(ty: &Ty) -> QueryResult<Option<String>> {
    Ok(match ty {
        Ty::Primitive(p) => Some(p.to_sql_value()),
        Ty::Enum(e) => Some(e.to_sql_value()),
        Ty::ByteArray(b) => Some(b.clone()),
        Ty::Struct(_) => Some(
            serde_json::to_string(&ty.to_json_value()?)
                .map_err(|e| ExecutorQueryError::Parse(ParseError::FromJsonStr(e)))?,
        ),
        Ty::Array(_) => Some(
            serde_json::to_string(&ty.to_json_value()?)
                .map_err(|e| ExecutorQueryError::Parse(ParseError::FromJsonStr(e)))?,
        ),
        Ty::Tuple(_) => Some(
            serde_json::to_string(&ty.to_json_value()?)
                .map_err(|e| ExecutorQueryError::Parse(ParseError::FromJsonStr(e)))?,
        ),
        Ty::FixedSizeArray(_) => Some(
            serde_json::to_string(&ty.to_json_value()?)
                .map_err(|e| ExecutorQueryError::Parse(ParseError::FromJsonStr(e)))?,
        ),
    })
}
