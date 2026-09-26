use std::time::Duration;

use ydb::{
    AnonymousCredentials, Client, ClientBuilder, ConsumerBuilder, CreateTopicOptionsBuilder,
};

use super::config::{YdbTopicCodec, YdbTopicSinkConfig};
use crate::{
    config::{SinkConfig, SinkContext},
    test_util::{
        components::{SINK_TAGS, run_and_assert_sink_compliance},
        random_lines_with_stream, random_string, trace_init,
    },
};

const CONSUMER: &str = "vector-test-consumer";

fn ydb_endpoint() -> String {
    std::env::var("YDB_ENDPOINT").unwrap_or_else(|_| "grpc://ydb:2136/local".into())
}

async fn client() -> Client {
    ClientBuilder::new_from_connection_string(ydb_endpoint())
        .expect("valid connection string")
        .with_credentials(AnonymousCredentials::new())
        .build()
        .await
        .expect("YDB client")
}

async fn create_topic(client: &Client, topic: &str) {
    let consumer = ConsumerBuilder::default()
        .name(CONSUMER.to_owned())
        .build()
        .expect("consumer");
    client
        .topic_client()
        .create_topic(
            topic.to_owned(),
            CreateTopicOptionsBuilder::default()
                .consumers(vec![consumer])
                .build()
                .expect("topic options"),
        )
        .await
        .expect("create topic");
}

async fn read_messages(client: &Client, topic: &str, count: usize) -> Vec<String> {
    let mut reader = client
        .topic_client()
        .create_reader(CONSUMER, topic.to_owned())
        .await
        .expect("topic reader");

    let mut result = Vec::with_capacity(count);
    while result.len() < count {
        let batch = tokio::time::timeout(Duration::from_secs(30), reader.read_batch())
            .await
            .expect("timed out waiting for messages")
            .expect("read batch");
        let commit_marker = batch.get_commit_marker();
        for mut message in batch.messages {
            let data = message
                .read_and_take()
                .await
                .expect("message data")
                .unwrap_or_default();
            result.push(String::from_utf8(data).expect("utf-8 message"));
        }
        reader.commit(commit_marker).expect("commit");
    }
    result
}

async fn write_and_read(codec: YdbTopicCodec) {
    trace_init();

    let topic = format!("vector-test-{}", random_string(10).to_lowercase());
    let client = client().await;
    create_topic(&client, &topic).await;

    let config: YdbTopicSinkConfig = serde_yaml::from_str(&format!(
        r#"
        endpoint: "{}"
        topic: "{topic}"
        producer_id: "vector-integration-test"
        encoding:
          codec: "text"
        "#,
        ydb_endpoint()
    ))
    .expect("config");
    let config = YdbTopicSinkConfig { codec, ..config };

    let (sink, healthcheck) = config
        .build(SinkContext::default())
        .await
        .expect("sink should build");
    healthcheck.await.expect("healthcheck should pass");

    let num_events = 100;
    let (lines, events) = random_lines_with_stream(100, num_events, None);
    run_and_assert_sink_compliance(sink, events, &SINK_TAGS).await;

    let received = read_messages(&client, &topic, num_events).await;
    assert_eq!(received, lines, "messages must be delivered in order");

    client
        .topic_client()
        .drop_topic(topic)
        .await
        .expect("drop topic");
}

#[tokio::test]
async fn ydb_topic_raw() {
    write_and_read(YdbTopicCodec::Raw).await;
}

#[tokio::test]
async fn ydb_topic_gzip() {
    write_and_read(YdbTopicCodec::Gzip).await;
}

#[tokio::test]
async fn ydb_topic_auto_codec() {
    write_and_read(YdbTopicCodec::Auto).await;
}

#[tokio::test]
async fn ydb_topic_healthcheck_fails_for_missing_topic() {
    trace_init();

    let config: YdbTopicSinkConfig = serde_yaml::from_str(&format!(
        r#"
        endpoint: "{}"
        topic: "does-not-exist-{}"
        encoding:
          codec: "json"
        "#,
        ydb_endpoint(),
        random_string(10).to_lowercase()
    ))
    .expect("config");

    let (_sink, healthcheck) = config
        .build(SinkContext::default())
        .await
        .expect("sink should build");
    assert!(healthcheck.await.is_err());
}
