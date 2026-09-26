use ydb::YdbError;

use super::{
    config::{ParsedEndpoint, YdbAuthConfig, YdbTopicCodec, YdbTopicSinkConfig, parse_endpoint},
    service::is_retriable,
};
use crate::config::ValidatedSink;

#[test]
fn generate_config() {
    crate::test_util::test_generate_config::<YdbTopicSinkConfig>();
}

fn config(yaml: &str) -> YdbTopicSinkConfig {
    serde_yaml::from_str(yaml).expect("config should parse")
}

#[test]
fn parses_minimal_config() {
    let config = config(
        r#"
        endpoint: "grpc://localhost:2136/local"
        topic: "vector"
        encoding:
          codec: "json"
        "#,
    );
    assert_eq!(config.codec, YdbTopicCodec::Auto);
    assert!(matches!(config.auth, YdbAuthConfig::Anonymous));
    assert!(config.validate().is_ok());
}

#[test]
fn parses_auth_strategies() {
    let config = config(
        r#"
        endpoint: "grpcs://ydb.example.com:2135/?database=/ru-central1/b1g/etn"
        topic: "vector"
        codec: "gzip"
        producer_id: "vector-1"
        auth:
          strategy: "static"
          user: "root"
          password: "secret"
        encoding:
          codec: "text"
        "#,
    );
    assert_eq!(config.codec, YdbTopicCodec::Gzip);
    assert!(matches!(config.auth, YdbAuthConfig::Static { .. }));

    let config = self::config(
        r#"
        endpoint: "grpc://localhost:2136/local"
        topic: "vector"
        auth:
          strategy: "access_token"
          token: "t1.abc"
        encoding:
          codec: "json"
        "#,
    );
    assert!(matches!(config.auth, YdbAuthConfig::AccessToken { .. }));
}

#[test]
fn parses_go_sdk_connection_strings() {
    assert_eq!(
        parse_endpoint("grpc://localhost:2136/local").unwrap(),
        ParsedEndpoint {
            endpoint: "grpc://localhost:2136".to_owned(),
            database: "/local".to_owned(),
        }
    );
    assert_eq!(
        parse_endpoint(
            "grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1g/etn"
        )
        .unwrap(),
        ParsedEndpoint {
            endpoint: "grpcs://ydb.serverless.yandexcloud.net:2135".to_owned(),
            database: "/ru-central1/b1g/etn".to_owned(),
        }
    );
}

#[test]
fn rejects_invalid_connection_strings() {
    for endpoint in [
        "localhost:2136",
        "http://localhost:2136/local",
        "grpc://localhost/local",
        "grpc://localhost:2136",
        "grpc://localhost:2136/",
    ] {
        assert!(
            parse_endpoint(endpoint).is_err(),
            "`{endpoint}` should be rejected"
        );
    }
}

#[test]
fn validate_rejects_invalid_settings() {
    for extra in [
        r#"topic: """#,
        r#"topic: "vector"
        producer_id: """#,
        r#"topic: "vector"
        partition_id: -1"#,
    ] {
        let config = config(&format!(
            r#"
        endpoint: "grpc://localhost:2136/local"
        {extra}
        encoding:
          codec: "json"
        "#
        ));
        assert!(config.validate().is_err(), "`{extra}` should be rejected");
    }
}

#[test]
fn retriable_errors() {
    assert!(is_retriable(&YdbError::DeadlineExceeded));
    assert!(is_retriable(&YdbError::Transport("broken pipe".to_owned())));
    assert!(is_retriable(&YdbError::Custom(
        "message writer was closed".to_owned()
    )));
    assert!(!is_retriable(&YdbError::Convert("bad value".to_owned())));
    assert!(!is_retriable(&YdbError::Custom(
        "codec is not supported".to_owned()
    )));
}
