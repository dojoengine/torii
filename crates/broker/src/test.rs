use crate::memory::MemoryBroker;
use crate::types::Update;
use futures_util::StreamExt;
use tokio::time::{timeout, Duration};

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    struct TestMessage {
        id: u32,
        content: String,
    }

    #[tokio::test]
    async fn test_publish_and_subscribe() {
        let mut stream = MemoryBroker::<Update<TestMessage>>::subscribe();

        let msg = TestMessage {
            id: 1,
            content: "Hello World".to_string(),
        };

        // Publish a non-optimistic message
        MemoryBroker::publish(Update::new(msg.clone(), false));

        // Should receive the message
        let received = timeout(Duration::from_millis(100), stream.next()).await;
        assert!(received.is_ok());
        let received_msg = received.unwrap().unwrap();
        assert_eq!(received_msg, msg);
    }

    #[tokio::test]
    async fn test_subscribe_raw_receives_all_messages() {
        let mut stream = MemoryBroker::<Update<TestMessage>>::subscribe_raw();

        let optimistic_msg = TestMessage {
            id: 1,
            content: "Optimistic".to_string(),
        };

        let non_optimistic_msg = TestMessage {
            id: 2,
            content: "Non-Optimistic".to_string(),
        };

        // Publish both optimistic and non-optimistic messages
        MemoryBroker::publish(Update::new(optimistic_msg.clone(), true));
        MemoryBroker::publish(Update::new(non_optimistic_msg.clone(), false));

        // Should receive optimistic message
        let received1 = timeout(Duration::from_millis(100), stream.next()).await;
        assert!(received1.is_ok());
        let update1 = received1.unwrap().unwrap();
        assert!(update1.is_optimistic());
        assert_eq!(update1.inner, optimistic_msg);

        // Should receive non-optimistic message
        let received2 = timeout(Duration::from_millis(100), stream.next()).await;
        assert!(received2.is_ok());
        let update2 = received2.unwrap().unwrap();
        assert!(!update2.is_optimistic());
        assert_eq!(update2.inner, non_optimistic_msg);
    }

    #[tokio::test]
    async fn test_subscribe_filters_optimistic_messages() {
        let mut stream = MemoryBroker::<Update<TestMessage>>::subscribe();

        let optimistic_msg = TestMessage {
            id: 1,
            content: "Should be filtered".to_string(),
        };

        let non_optimistic_msg = TestMessage {
            id: 2,
            content: "Should pass through".to_string(),
        };

        // Publish optimistic message first (should be filtered out)
        MemoryBroker::publish(Update::new(optimistic_msg, true));

        // Publish non-optimistic message (should pass through)
        MemoryBroker::publish(Update::new(non_optimistic_msg.clone(), false));

        // Should only receive the non-optimistic message
        let received = timeout(Duration::from_millis(100), stream.next()).await;
        assert!(received.is_ok());
        let received_msg = received.unwrap().unwrap();
        assert_eq!(received_msg, non_optimistic_msg);

        // Should not receive any more messages immediately
        let no_more = timeout(Duration::from_millis(50), stream.next()).await;
        assert!(no_more.is_err()); // Should timeout
    }

    #[tokio::test]
    async fn test_subscribe_optimistic_only_receives_optimistic() {
        let mut stream = MemoryBroker::<Update<TestMessage>>::subscribe_optimistic();

        let optimistic_msg = TestMessage {
            id: 1,
            content: "Optimistic message".to_string(),
        };

        let non_optimistic_msg = TestMessage {
            id: 2,
            content: "Non-optimistic message".to_string(),
        };

        // Publish non-optimistic message first (should be filtered out)
        MemoryBroker::publish(Update::new(non_optimistic_msg, false));

        // Publish optimistic message (should pass through)
        MemoryBroker::publish(Update::new(optimistic_msg.clone(), true));

        // Should only receive the optimistic message
        let received = timeout(Duration::from_millis(100), stream.next()).await;
        assert!(received.is_ok());
        let received_msg = received.unwrap().unwrap();
        assert_eq!(received_msg, optimistic_msg);

        // Should not receive any more messages immediately
        let no_more = timeout(Duration::from_millis(50), stream.next()).await;
        assert!(no_more.is_err()); // Should timeout
    }

    #[tokio::test]
    async fn test_multiple_subscribers_receive_same_message() {
        let mut stream1 = MemoryBroker::<Update<TestMessage>>::subscribe();
        let mut stream2 = MemoryBroker::<Update<TestMessage>>::subscribe();
        let mut stream3 = MemoryBroker::<Update<TestMessage>>::subscribe_raw();

        let msg = TestMessage {
            id: 42,
            content: "Broadcast message".to_string(),
        };

        // Publish a message
        MemoryBroker::publish(Update::new(msg.clone(), false));

        // All subscribers should receive the message
        let received1 = timeout(Duration::from_millis(100), stream1.next()).await;
        let received2 = timeout(Duration::from_millis(100), stream2.next()).await;
        let received3 = timeout(Duration::from_millis(100), stream3.next()).await;

        assert!(received1.is_ok());
        assert!(received2.is_ok());
        assert!(received3.is_ok());

        assert_eq!(received1.unwrap().unwrap(), msg);
        assert_eq!(received2.unwrap().unwrap(), msg);

        let raw_update = received3.unwrap().unwrap();
        assert_eq!(raw_update.inner, msg);
        assert!(!raw_update.is_optimistic());
    }

    #[tokio::test]
    async fn test_late_subscriber_misses_early_messages() {
        let msg1 = TestMessage {
            id: 1,
            content: "Early message".to_string(),
        };

        // Publish before any subscriber exists
        MemoryBroker::publish(Update::new(msg1, false));

        // Create subscriber after publishing
        let mut stream = MemoryBroker::<Update<TestMessage>>::subscribe();

        let msg2 = TestMessage {
            id: 2,
            content: "Late message".to_string(),
        };

        // Publish after subscriber is created
        MemoryBroker::publish(Update::new(msg2.clone(), false));

        // Should only receive the late message
        let received = timeout(Duration::from_millis(100), stream.next()).await;
        assert!(received.is_ok());
        let received_msg = received.unwrap().unwrap();
        assert_eq!(received_msg, msg2);

        // Should not receive the early message
        let no_more = timeout(Duration::from_millis(50), stream.next()).await;
        assert!(no_more.is_err()); // Should timeout
    }

    #[tokio::test]
    async fn test_stream_cleanup_on_drop() {
        // This test verifies that dropping a stream properly cleans up the sender
        let msg = TestMessage {
            id: 1,
            content: "Test message".to_string(),
        };

        // Create and immediately drop a stream
        {
            let _stream = MemoryBroker::<Update<TestMessage>>::subscribe();
            // Stream gets dropped here
        }

        // Verify we can still use the broker normally
        let mut new_stream = MemoryBroker::<Update<TestMessage>>::subscribe();
        MemoryBroker::publish(Update::new(msg.clone(), false));

        let received = timeout(Duration::from_millis(100), new_stream.next()).await;
        assert!(received.is_ok());
        let received_msg = received.unwrap().unwrap();
        assert_eq!(received_msg, msg);
    }

    #[tokio::test]
    async fn test_concurrent_publishing_and_subscribing() {
        use tokio::task;

        let mut stream = MemoryBroker::<Update<TestMessage>>::subscribe_raw();

        // Spawn concurrent publishing tasks
        let publish_handle = task::spawn(async {
            for i in 0..10 {
                let msg = TestMessage {
                    id: i,
                    content: format!("Message {}", i),
                };
                MemoryBroker::publish(Update::new(msg, i % 2 == 0)); // Alternate optimistic flag
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });

        // Collect messages
        let mut received_messages = Vec::new();
        for _ in 0..10 {
            if let Ok(Some(update)) = timeout(Duration::from_millis(200), stream.next()).await {
                received_messages.push((update.inner.id, update.is_optimistic()));
            }
        }

        publish_handle.await.unwrap();

        // Verify we received all messages
        assert_eq!(received_messages.len(), 10);

        // Verify the optimistic flags are correct
        for (id, is_optimistic) in received_messages {
            assert_eq!(is_optimistic, id % 2 == 0);
        }
    }

    #[tokio::test]
    async fn test_with_subscribers_function() {
        // Test the with_subscribers utility function
        let initial_count =
            MemoryBroker::<Update<TestMessage>>::with_subscribers(|senders| senders.0.len());

        let _stream1 = MemoryBroker::<Update<TestMessage>>::subscribe();
        let _stream2 = MemoryBroker::<Update<TestMessage>>::subscribe_raw();

        let count_with_subscribers =
            MemoryBroker::<Update<TestMessage>>::with_subscribers(|senders| senders.0.len());

        // Should have 2 more subscribers now
        assert_eq!(count_with_subscribers, initial_count + 2);
    }
}
