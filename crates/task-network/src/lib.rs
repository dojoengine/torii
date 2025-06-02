mod error;

pub use error::TaskNetworkError;

pub type Result<T> = std::result::Result<T, TaskNetworkError>;

use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;

use futures_util::future::try_join_all;
use tokio::sync::Semaphore;
use torii_adigraphmap::AcyclicDigraphMap;
use tracing::{debug, error};

const LOG_TARGET: &str = "torii::task_network";

#[derive(Debug)]
pub struct TaskNetwork<K, T>
where
    K: Eq + Hash + Clone + std::fmt::Debug + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    tasks: AcyclicDigraphMap<K, T>,
    semaphore: Arc<Semaphore>,
}

impl<K, T> TaskNetwork<K, T>
where
    K: Eq + Hash + Clone + std::fmt::Debug + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    pub fn new(max_concurrent_tasks: usize) -> Self {
        Self {
            tasks: AcyclicDigraphMap::new(),
            semaphore: Arc::new(Semaphore::new(max_concurrent_tasks)),
        }
    }

    pub fn add_task(&mut self, task_id: K, task: T) -> Result<()> {
        self.tasks
            .add_node(task_id, task)
            .map_err(TaskNetworkError::GraphError)?;
        Ok(())
    }

    pub fn add_task_with_dependencies(
        &mut self,
        task_id: K,
        task: T,
        dependencies: Vec<K>,
    ) -> Result<()> {
        self.tasks
            .add_node(task_id.clone(), task)
            .map_err(TaskNetworkError::GraphError)?;

        for dep in dependencies {
            if self.tasks.contains_key(&dep) {
                self.add_dependency(dep, task_id.clone())?;
            } else {
                debug!(
                    target: LOG_TARGET,
                    task_id = ?task_id,
                    dependency = ?dep,
                    "Ignoring non-existent dependency."
                );
            }
        }

        Ok(())
    }

    pub fn add_dependency(&mut self, from: K, to: K) -> Result<()> {
        self.tasks
            .add_dependency(&from, &to)
            .map_err(TaskNetworkError::GraphError)
    }

    pub async fn process_tasks<F, Fut, O, E>(&mut self, task_handler: F) -> Result<()>
    where
        F: Fn(K, T) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<O, E>> + Send,
        O: Send,
        E: std::error::Error + Send + Sync + 'static,
    {
        if self.tasks.is_empty() {
            return Ok(());
        }

        let task_levels = self.tasks.topo_sort_by_level();
        let semaphore = self.semaphore.clone();

        for (level_idx, level_tasks) in task_levels.iter().enumerate() {
            debug!(
                target: LOG_TARGET,
                level = level_idx,
                task_count = level_tasks.len(),
                "Processing task level."
            );

            let mut handles = Vec::with_capacity(level_tasks.len());

            for (task_id, task) in level_tasks {
                let task_handler = task_handler.clone();
                let semaphore = semaphore.clone();
                let task_clone = task.clone();
                let task_id = task_id.clone();

                handles.push(tokio::spawn(async move {
                    let _permit = semaphore
                        .acquire()
                        .await
                        .map_err(TaskNetworkError::SemaphoreError)?;

                    debug!(
                        target: LOG_TARGET,
                        task_id = ?task_id,
                        level = level_idx,
                        "Processing task."
                    );

                    match task_handler(task_id.clone(), task_clone).await {
                        Ok(_) => Ok(()),
                        Err(e) => {
                            error!(
                                target: LOG_TARGET,
                                error = %e,
                                task_id = ?task_id,
                                level = level_idx,
                                "Error processing task."
                            );
                            Err(TaskNetworkError::TaskError(Box::new(e)))
                        }
                    }
                }));
            }

            let results = try_join_all(handles)
                .await
                .map_err(TaskNetworkError::JoinError)?;
            for result in results {
                result?;
            }
        }

        self.tasks.clear();

        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn get_mut(&mut self, task_id: &K) -> Option<&mut T> {
        self.tasks.get_mut(task_id)
    }

    pub fn clear(&mut self) {
        self.tasks.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_task_execution() {
        let mut manager = TaskNetwork::<u64, String>::new(4);

        manager.add_task(1, "Task 1".to_string()).unwrap();
        manager.add_task(2, "Task 2".to_string()).unwrap();

        let results = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let results_clone = results.clone();
        manager
            .process_tasks(move |id, task| {
                let results = results_clone.clone();
                async move {
                    let mut locked_results = results.lock().await;
                    locked_results.push((id, task));
                    Ok::<_, std::io::Error>(())
                }
            })
            .await
            .unwrap();

        let final_results = results.lock().await;
        assert_eq!(final_results.len(), 2);
    }

    #[tokio::test]
    async fn test_non_existent_dependencies() {
        let mut manager = TaskNetwork::<u64, String>::new(4);

        manager
            .add_task_with_dependencies(1, "Task 1".to_string(), vec![99, 100])
            .unwrap();

        manager
            .add_task_with_dependencies(2, "Task 2".to_string(), vec![1, 100])
            .unwrap();

        manager
            .add_task_with_dependencies(3, "Task 3".to_string(), vec![2])
            .unwrap();

        let executed = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let executed_clone = executed.clone();
        manager
            .process_tasks(move |id, _task| {
                let executed = executed_clone.clone();
                async move {
                    let mut locked = executed.lock().await;
                    locked.push(id);
                    Ok::<_, std::io::Error>(())
                }
            })
            .await
            .unwrap();

        let result = executed.lock().await;
        assert_eq!(result.len(), 3);

        assert_eq!(result[0], 1);
        assert_eq!(result[1], 2);
        assert_eq!(result[2], 3);
    }

    #[tokio::test]
    async fn test_dependency_ordering() {
        let mut manager = TaskNetwork::<u64, String>::new(4);

        manager.add_task(1, "Task 1".to_string()).unwrap();
        manager.add_task(2, "Task 2".to_string()).unwrap();
        manager.add_dependency(1, 2).unwrap();

        let executed = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let executed_clone = executed.clone();
        manager
            .process_tasks(move |id, _task| {
                let executed = executed_clone.clone();
                async move {
                    let mut locked = executed.lock().await;
                    locked.push(id);
                    Ok::<_, std::io::Error>(())
                }
            })
            .await
            .unwrap();

        let result = executed.lock().await;
        assert_eq!(result[0], 1); // Task 1 should be executed first
        assert_eq!(result[1], 2); // Task 2 should be executed second
    }

    #[tokio::test]
    async fn test_level_parallel_execution() {
        let mut manager = TaskNetwork::<u64, String>::new(4);

        manager.add_task(1, "Task 1".to_string()).unwrap();
        manager.add_task(2, "Task 2".to_string()).unwrap();
        manager.add_task(3, "Task 3".to_string()).unwrap();
        manager.add_task(4, "Task 4".to_string()).unwrap();
        manager.add_task(5, "Task 5".to_string()).unwrap();

        manager.add_dependency(1, 3).unwrap();
        manager.add_dependency(2, 3).unwrap();
        manager.add_dependency(2, 4).unwrap();
        manager.add_dependency(3, 5).unwrap();
        manager.add_dependency(4, 5).unwrap();

        let executed_levels = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let currently_executing = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let executed_clone = executed_levels.clone();
        let current_clone = currently_executing.clone();

        manager
            .process_tasks(move |id, _task| {
                let executed_levels = executed_clone.clone();
                let currently_executing = current_clone.clone();

                async move {
                    {
                        let mut current = currently_executing.lock().await;
                        current.push(id);
                    }

                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

                    {
                        let current_executing = {
                            let current = currently_executing.lock().await;
                            current.clone() // Clone the data so we can release the lock
                        };

                        {
                            let mut executed = executed_levels.lock().await;
                            executed.push((id, current_executing));
                        }

                        {
                            let mut current = currently_executing.lock().await;
                            if let Some(pos) = current.iter().position(|&x| x == id) {
                                current.remove(pos);
                            }
                        }
                    }

                    Ok::<_, std::io::Error>(())
                }
            })
            .await
            .unwrap();

        let result = executed_levels.lock().await;

        let mut observed_task_order = result.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        observed_task_order.sort(); // Sort to make comparison easier
        assert_eq!(observed_task_order, vec![1, 2, 3, 4, 5]);

        let task_1_execution = result.iter().find(|(id, _)| *id == 1).unwrap();
        let task_2_execution = result.iter().find(|(id, _)| *id == 2).unwrap();
        let task_3_execution = result.iter().find(|(id, _)| *id == 3).unwrap();
        let task_4_execution = result.iter().find(|(id, _)| *id == 4).unwrap();

        assert!(task_1_execution.1.contains(&2) || task_2_execution.1.contains(&1));

        assert!(task_3_execution.1.contains(&4) || task_4_execution.1.contains(&3));
    }

    #[tokio::test]
    async fn test_custom_task_id_type() {
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        struct CustomTaskId(String);

        let mut manager = TaskNetwork::<CustomTaskId, String>::new(4);

        manager
            .add_task(CustomTaskId("task1".to_string()), "Task 1".to_string())
            .unwrap();
        manager
            .add_task(CustomTaskId("task2".to_string()), "Task 2".to_string())
            .unwrap();
        manager
            .add_dependency(
                CustomTaskId("task1".to_string()),
                CustomTaskId("task2".to_string()),
            )
            .unwrap();

        let executed = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let executed_clone = executed.clone();
        manager
            .process_tasks(move |id, _task| {
                let executed = executed_clone.clone();
                async move {
                    let mut locked = executed.lock().await;
                    locked.push(id);
                    Ok::<_, std::io::Error>(())
                }
            })
            .await
            .unwrap();

        let result = executed.lock().await;
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], CustomTaskId("task1".to_string()));
        assert_eq!(result[1], CustomTaskId("task2".to_string()));
    }
}
