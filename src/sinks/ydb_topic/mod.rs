//! The `ydb_topic` sink.
//!
//! Publishes events to a [YDB topic][ydb_topics] using the native Topic API
//! (`Ydb.Topic.V1.TopicService/StreamWrite`), the same protocol used by the
//! official Go SDK (`github.com/ydb-platform/ydb-go-sdk/v3/topic`). Messages
//! written by this sink are readable by Go consumers without any extra setup:
//! only the `raw` and `gzip` codecs are used, which `ydb-go-sdk` decodes out of
//! the box.
//!
//! [ydb_topics]: https://ydb.tech/docs/en/concepts/topic

mod config;
mod request_builder;
mod service;
mod sink;

#[cfg(all(test, feature = "ydb-integration-tests"))]
mod integration_tests;
#[cfg(test)]
mod tests;

pub use config::YdbTopicSinkConfig;
