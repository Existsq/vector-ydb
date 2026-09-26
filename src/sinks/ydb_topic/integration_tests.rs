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

fn random_topic() -> String {
    format!("vector-test-{}", random_string(10).to_lowercase())
}

/// Sink config with small batches, so that many requests are in flight at once.
fn sink_config(topic: &str, extra: &str) -> YdbTopicSinkConfig {
    serde_yaml::from_str(&format!(
        r#"
        endpoint: "{}"
        topic: "{topic}"
        producer_id: "vector-integration-test"
        encoding:
          codec: "text"
        batch:
          max_events: 100
        {extra}
        "#,
        ydb_endpoint()
    ))
    .expect("config")
}

async fn write_and_read(config: YdbTopicSinkConfig, num_events: usize) {
    trace_init();

    let client = client().await;
    create_topic(&client, &config.topic).await;

    let (sink, healthcheck) = config
        .build(SinkContext::default())
        .await
        .expect("sink should build");
    healthcheck.await.expect("healthcheck should pass");

    let (lines, events) = random_lines_with_stream(100, num_events, None);
    run_and_assert_sink_compliance(sink, events, &SINK_TAGS).await;

    let received = read_messages(&client, &config.topic, num_events).await;
    assert_eq!(
        received, lines,
        "messages must be delivered once and in order"
    );

    client
        .topic_client()
        .drop_topic(config.topic.clone())
        .await
        .expect("drop topic");
}

async fn write_and_read_with_codec(codec: YdbTopicCodec) {
    let config = YdbTopicSinkConfig {
        codec,
        ..sink_config(&random_topic(), "")
    };
    write_and_read(config, 10_000).await;
}

/// Creates a YDB user with the given password.
async fn create_user(user: &str, password: &str) {
    client()
        .await
        .query_client()
        .exec(format!(
            "CREATE USER {user} PASSWORD '{password}'; GRANT ALL ON `/local` TO {user};"
        ))
        .await
        .expect("create user");
}

#[tokio::test]
async fn ydb_topic_raw() {
    write_and_read_with_codec(YdbTopicCodec::Raw).await;
}

#[tokio::test]
async fn ydb_topic_gzip() {
    write_and_read_with_codec(YdbTopicCodec::Gzip).await;
}

#[tokio::test]
async fn ydb_topic_auto_codec() {
    write_and_read_with_codec(YdbTopicCodec::Auto).await;
}

#[tokio::test]
async fn ydb_topic_single_request_in_flight() {
    let config = sink_config(&random_topic(), "request:\n          concurrency: none");
    write_and_read(config, 1_000).await;
}

#[tokio::test]
async fn ydb_topic_static_auth() {
    let user = format!("vector{}", random_string(8).to_lowercase());
    create_user(&user, "secret").await;

    let auth = format!(
        "auth:\n          strategy: static\n          user: {user}\n          password: secret"
    );
    write_and_read(sink_config(&random_topic(), &auth), 100).await;
}

#[tokio::test]
async fn ydb_topic_static_auth_wrong_password() {
    trace_init();

    let user = format!("vector{}", random_string(8).to_lowercase());
    create_user(&user, "secret").await;

    let auth = format!(
        "auth:\n          strategy: static\n          user: {user}\n          password: wrong"
    );
    let config = sink_config(&random_topic(), &auth);
    let (_sink, healthcheck) = config
        .build(SinkContext::default())
        .await
        .expect("sink should build");
    let error = healthcheck.await.expect_err("healthcheck should fail");
    assert!(
        error.to_string().contains("Invalid password"),
        "unexpected error: {error}"
    );
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
