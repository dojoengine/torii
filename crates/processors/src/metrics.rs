//! Shared metrics definitions for processors.
//!
//! This module provides consistent metric naming and helper functions for all processors.

use metrics::{counter, histogram};
use std::time::Instant;

/// Processor metrics helper functions
pub struct ProcessorMetrics;

impl ProcessorMetrics {
    /// Record the processing time for a specific processor
    pub fn record_processing_time(processor_name: &str, duration: std::time::Duration) {
        histogram!(
            "torii_processor_duration_seconds",
            "processor" => processor_name.to_string()
        )
        .record(duration.as_secs_f64());
    }

    /// Record the processing time using a timer
    pub fn start_timer(processor_name: &str) -> ProcessorTimer {
        ProcessorTimer {
            start: Instant::now(),
            processor_name: processor_name.to_string(),
        }
    }

    /// Increment the counter for successful processing
    pub fn increment_success(processor_name: &str) {
        counter!(
            "torii_processor_events_processed_total",
            "processor" => processor_name.to_string(),
            "status" => "success"
        )
        .increment(1);
    }

    /// Increment the counter for failed processing
    pub fn increment_error(processor_name: &str) {
        counter!(
            "torii_processor_events_processed_total",
            "processor" => processor_name.to_string(),
            "status" => "error"
        )
        .increment(1);
    }

    /// Record the number of items processed (e.g., transfers, models, etc.)
    pub fn record_items_processed(processor_name: &str, item_type: &str, count: u64) {
        counter!(
            "torii_processor_items_processed_total",
            "processor" => processor_name.to_string(),
            "item_type" => item_type.to_string()
        )
        .increment(count);
    }

    /// Record the number of specific operations (e.g., models registered, transfers processed)
    pub fn record_operation(operation: &str, count: u64) {
        counter!(
            "torii_processor_operations_total",
            "operation" => operation.to_string()
        )
        .increment(count);
    }

    /// Record the duration of specific operations
    pub fn record_operation_duration(operation: &str, duration: std::time::Duration) {
        histogram!(
            "torii_processor_operation_duration_seconds",
            "operation" => operation.to_string()
        )
        .record(duration.as_secs_f64());
    }

    /// Record cache operations
    pub fn record_cache_operation(operation: &str, status: &str) {
        counter!(
            "torii_processor_cache_operations_total",
            "operation" => operation.to_string(),
            "status" => status.to_string()
        )
        .increment(1);
    }

    /// Record metadata operations
    pub fn record_metadata_operation(operation: &str, status: &str) {
        counter!(
            "torii_processor_metadata_operations_total",
            "operation" => operation.to_string(),
            "status" => status.to_string()
        )
        .increment(1);
    }
}

/// A timer that automatically records the duration when dropped
pub struct ProcessorTimer {
    start: Instant,
    processor_name: String,
}

impl Drop for ProcessorTimer {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        ProcessorMetrics::record_processing_time(&self.processor_name, duration);
    }
}

/// Executor metrics helper functions
pub struct ExecutorMetrics;

impl ExecutorMetrics {
    /// Record query execution time
    pub fn record_query_duration(query_type: &str, duration: std::time::Duration) {
        histogram!(
            "torii_executor_query_duration_seconds",
            "query_type" => query_type.to_string()
        )
        .record(duration.as_secs_f64());
    }

    /// Record query execution count
    pub fn record_query_execution(query_type: &str, status: &str) {
        counter!(
            "torii_executor_queries_total",
            "query_type" => query_type.to_string(),
            "status" => status.to_string()
        )
        .increment(1);
    }

    /// Record transaction operations
    pub fn record_transaction_operation(operation: &str, status: &str) {
        counter!(
            "torii_executor_transaction_operations_total",
            "operation" => operation.to_string(),
            "status" => status.to_string()
        )
        .increment(1);
    }

    /// Record broker message publishing
    pub fn record_broker_message(message_type: &str, status: &str) {
        counter!(
            "torii_executor_broker_messages_total",
            "message_type" => message_type.to_string(),
            "status" => status.to_string()
        )
        .increment(1);
    }

    /// Record balance diff operations
    pub fn record_balance_operation(operation: &str, count: u64) {
        counter!(
            "torii_executor_balance_operations_total",
            "operation" => operation.to_string()
        )
        .increment(count);
    }

    /// Start a query timer
    pub fn start_query_timer(query_type: &str) -> QueryTimer {
        QueryTimer {
            start: Instant::now(),
            query_type: query_type.to_string(),
        }
    }
}

/// A timer that automatically records query duration when dropped
pub struct QueryTimer {
    start: Instant,
    query_type: String,
}

impl Drop for QueryTimer {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        ExecutorMetrics::record_query_duration(&self.query_type, duration);
    }
}
