use std::{path::PathBuf, sync::Arc};

use futures::FutureExt;
use snafu::Snafu;
use vector_lib::sensitive_string::SensitiveString;
use ydb::{
    AccessTokenCredentials, AnonymousCredentials, Client, ClientBuilder, Codec,
    DescribeTopicOptionsBuilder, FromEnvCredentials, HasGrpcOptions, MetadataUrlCredentials,
    ServiceAccountCredentials, StaticCredentials,
};

use super::{
    service::{YdbTopicRetryLogic, YdbTopicService},
    sink::YdbTopicSink,
};
use crate::{
    config::ValidatedSink,
    sinks::{prelude::*, util::service::TowerRequestConfigDefaults},
};

/// Batch defaults for the `ydb_topic` sink.
#[derive(Clone, Copy, Debug, Default)]
pub struct YdbTopicDefaultBatchSettings;

impl SinkBatchSettings for YdbTopicDefaultBatchSettings {
    const MAX_EVENTS: Option<usize> = Some(1000);
    // YDB limits a single `StreamWrite` gRPC message to 64 MiB; stay well below it.
    const MAX_BYTES: Option<usize> = Some(8 * 1024 * 1024);
    const TIMEOUT_SECS: f64 = 1.0;
}

/// Request defaults for the `ydb_topic` sink.
///
/// A single in-flight request keeps the order of messages within the producer
/// session, which is what YDB topic consumers (including `ydb-go-sdk` readers)
/// usually rely on.
#[derive(Clone, Copy, Debug, Default)]
pub struct YdbTopicTowerRequestConfigDefaults;

impl TowerRequestConfigDefaults for YdbTopicTowerRequestConfigDefaults {
    const CONCURRENCY: Concurrency = Concurrency::None;
}

/// Compression codec applied to messages written to the topic.
///
/// All of the supported codecs can be decoded by the YDB Go SDK
/// (`ydb-go-sdk/v3/topic`) readers without registering custom decoders.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum YdbTopicCodec {
    /// Pick the codec (`raw` or `gzip`) that produces the smallest payload, among those allowed by the topic.
    #[default]
    Auto,

    /// Write messages without compression.
    Raw,

    /// Compress messages with gzip.
    Gzip,
}

impl YdbTopicCodec {
    pub(super) const fn selection(self) -> ydb::CodecSelection {
        match self {
            Self::Auto => ydb::CodecSelection::Auto,
            Self::Raw => ydb::CodecSelection::Fixed(Codec::RAW),
            Self::Gzip => ydb::CodecSelection::Fixed(Codec::GZIP),
        }
    }
}

/// Authentication settings for YDB.
#[configurable_component]
#[derive(Clone, Debug, Default)]
#[serde(rename_all = "snake_case", tag = "strategy")]
#[configurable(metadata(
    docs::enum_tag_description = "The authentication strategy used to connect to YDB.

See the YDB [authentication documentation][ydb_auth_docs] for details about each strategy.

[ydb_auth_docs]: https://ydb.tech/docs/en/reference/ydb-sdk/auth"
))]
pub enum YdbAuthConfig {
    /// Anonymous access (no credentials).
    #[default]
    Anonymous,

    /// A static access token, such as an IAM token.
    AccessToken {
        /// The access token.
        token: SensitiveString,
    },

    /// Login and password of a YDB built-in user.
    Static {
        /// The user name.
        #[configurable(metadata(docs::examples = "root"))]
        user: String,

        /// The password.
        password: SensitiveString,
    },

    /// Authorized key file of a Yandex Cloud service account.
    ServiceAccountKey {
        /// Path to the JSON service account key file.
        #[configurable(metadata(docs::examples = "/etc/vector/sa-key.json"))]
        key_file: PathBuf,
    },

    /// Token from the Yandex Cloud virtual machine metadata service.
    Metadata,

    /// Credentials selected from environment variables, the same way the YDB Go SDK
    /// (`ydb-go-sdk-auth-environ`) does.
    ///
    /// The following variables are checked in order: `YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS`,
    /// `YDB_ANONYMOUS_CREDENTIALS`, `YDB_METADATA_CREDENTIALS`, `YDB_ACCESS_TOKEN_CREDENTIALS`.
    Environment,
}

/// TLS settings for the connection to YDB.
///
/// TLS is enabled by using the `grpcs://` scheme in `endpoint`.
#[configurable_component]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct YdbTlsConfig {
    /// Path to a PEM-encoded CA certificate used to verify the YDB server certificate.
    ///
    /// When not set, the system root certificates are used.
    #[configurable(metadata(docs::examples = "/etc/ssl/certs/ydb-ca.pem"))]
    pub ca_file: Option<PathBuf>,
}

/// Configuration for the `ydb_topic` sink.
#[configurable_component(sink("ydb_topic", "Publish observability events to YDB topics."))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct YdbTopicSinkConfig {
    /// The YDB connection string.
    ///
    /// Uses the same format as the YDB Go SDK (`ydb.Open`): `grpc://` or `grpcs://` (TLS) scheme,
    /// host, explicit port, and the database either as the URL path or as the `database` query
    /// parameter.
    #[configurable(metadata(docs::examples = "grpc://localhost:2136/local"))]
    #[configurable(metadata(
        docs::examples = "grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1gxxx/etnxxx"
    ))]
    pub endpoint: String,

    /// The topic path to write to.
    ///
    /// A relative path is resolved against the database.
    #[configurable(metadata(docs::examples = "vector/logs"))]
    pub topic: String,

    /// The producer ID of the write session.
    ///
    /// The producer ID is also used as the message group ID, so all messages written by
    /// this sink go to the same partition, as with the YDB Go SDK defaults. Setting a stable
    /// producer ID lets YDB deduplicate messages resent after reconnects.
    ///
    /// When not set, a random UUID is generated each time the sink starts.
    #[configurable(metadata(docs::examples = "vector-node-1"))]
    pub producer_id: Option<String>,

    /// Write all messages to the given partition instead of routing them by producer ID.
    #[configurable(metadata(docs::examples = 0))]
    pub partition_id: Option<i64>,

    #[serde(default)]
    pub codec: YdbTopicCodec,

    #[serde(default)]
    pub auth: YdbAuthConfig,

    pub tls: Option<YdbTlsConfig>,

    pub encoding: EncodingConfig,

    #[serde(default)]
    pub batch: BatchConfig<YdbTopicDefaultBatchSettings>,

    #[serde(default)]
    pub request: TowerRequestConfig<YdbTopicTowerRequestConfigDefaults>,

    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,
}

impl GenerateConfig for YdbTopicSinkConfig {
    fn generate_config() -> serde_json::Value {
        serde_yaml::from_str(indoc::indoc! {r#"
            endpoint: "grpc://localhost:2136/local"
            topic: "vector"
            encoding:
              codec: "json"
        "#})
        .unwrap()
    }
}

#[derive(Debug, Snafu)]
pub(super) enum YdbConfigError {
    #[snafu(display("invalid `endpoint`: {message}"))]
    InvalidEndpoint { message: String },
    #[snafu(display("`topic` must not be empty"))]
    EmptyTopic,
    #[snafu(display("`producer_id` must not be empty"))]
    EmptyProducerId,
    #[snafu(display("`partition_id` must not be negative"))]
    NegativePartitionId,
}

/// Endpoint and database parsed from the connection string.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ParsedEndpoint {
    /// `scheme://host:port`
    pub(super) endpoint: String,
    pub(super) database: String,
}

/// Parses a YDB connection string in the format accepted by the Go SDK.
///
/// The YDB Rust SDK panics on connection strings without an explicit port, so
/// they are rejected here with a readable error instead.
pub(super) fn parse_endpoint(endpoint: &str) -> Result<ParsedEndpoint, YdbConfigError> {
    let invalid = |message: String| YdbConfigError::InvalidEndpoint { message };

    let url = url::Url::parse(endpoint).map_err(|error| invalid(error.to_string()))?;
    match url.scheme() {
        "grpc" | "grpcs" => {}
        other => {
            return Err(invalid(format!(
                "unsupported scheme `{other}`, expected `grpc` or `grpcs`"
            )));
        }
    }
    let host = url
        .host_str()
        .filter(|host| !host.is_empty())
        .ok_or_else(|| invalid("missing host".to_owned()))?;
    let port = url
        .port()
        .ok_or_else(|| invalid("missing port, for example `grpc://localhost:2136`".to_owned()))?;

    let database = url
        .query_pairs()
        .find_map(|(key, value)| (key == "database").then(|| value.into_owned()))
        .unwrap_or_else(|| url.path().to_owned());
    if database.is_empty() || database == "/" {
        return Err(invalid(
            "missing database, set it as the URL path or the `database` query parameter".to_owned(),
        ));
    }

    Ok(ParsedEndpoint {
        endpoint: format!("{}://{host}:{port}", url.scheme()),
        database,
    })
}

#[derive(Clone, Debug)]
pub struct ValidatedYdbTopicSink {
    endpoint: ParsedEndpoint,
    batch_settings: BatcherSettings,
    request_settings: crate::sinks::util::TowerRequestSettings,
}

#[async_trait::async_trait]
#[typetag::serde(name = "ydb_topic")]
impl SinkConfig for YdbTopicSinkConfig {
    fn input(&self) -> Input {
        Input::new(self.encoding.config().input_type() & (DataType::Log | DataType::Metric))
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

#[async_trait::async_trait]
impl ValidatedSink for YdbTopicSinkConfig {
    type Validated = ValidatedYdbTopicSink;

    fn validate(&self) -> crate::Result<ValidatedYdbTopicSink> {
        let endpoint = parse_endpoint(&self.endpoint)?;
        if self.topic.is_empty() {
            return Err(Box::new(YdbConfigError::EmptyTopic));
        }
        if self.producer_id.as_deref() == Some("") {
            return Err(Box::new(YdbConfigError::EmptyProducerId));
        }
        if self.partition_id.is_some_and(|id| id < 0) {
            return Err(Box::new(YdbConfigError::NegativePartitionId));
        }
        self.encoding.validate()?;

        Ok(ValidatedYdbTopicSink {
            endpoint,
            batch_settings: self.batch.into_batcher_settings()?,
            request_settings: self.request.into_settings(),
        })
    }

    async fn build(
        &self,
        validated: &ValidatedYdbTopicSink,
        _cx: SinkContext,
    ) -> crate::Result<(VectorSink, Healthcheck)> {
        let ValidatedYdbTopicSink {
            endpoint,
            batch_settings,
            request_settings,
        } = validated.clone();

        let client = Arc::new(self.build_client(&endpoint).await?);
        let healthcheck = healthcheck(Arc::clone(&client), self.topic.clone()).boxed();

        let writer = YdbTopicService::new(client, self.writer_options());
        let service = ServiceBuilder::new()
            .settings(request_settings, YdbTopicRetryLogic)
            .service(writer.clone());

        let transformer = self.encoding.transformer();
        let serializer = self.encoding.build()?;
        let encoder = Encoder::<()>::new(serializer);

        let sink = YdbTopicSink::new(service, writer, batch_settings, transformer, encoder);

        Ok((VectorSink::from_event_streamsink(sink), healthcheck))
    }
}

impl YdbTopicSinkConfig {
    pub(super) fn writer_options(&self) -> ydb::TopicWriterOptions {
        let partitioning = match self.partition_id {
            Some(id) => ydb::PartitioningStrategy::PartitionId(id),
            None => ydb::PartitioningStrategy::ByProducerId,
        };
        ydb::TopicWriterOptions::builder()
            .topic_path(self.topic.clone())
            .maybe_producer_id(self.producer_id.clone())
            .partitioning(partitioning)
            .codec_selector(self.codec.selection())
            .build()
    }

    async fn build_client(&self, endpoint: &ParsedEndpoint) -> crate::Result<Client> {
        let mut builder = ClientBuilder::new_from_connection_string(self.endpoint.as_str())?;

        let ca_file = self.tls.as_ref().and_then(|tls| tls.ca_file.as_ref());
        if let Some(ca_file) = ca_file {
            builder = builder.load_certificate(ca_file)?;
        }

        builder = match &self.auth {
            YdbAuthConfig::Anonymous => builder.with_credentials(AnonymousCredentials::new()),
            YdbAuthConfig::AccessToken { token } => {
                builder.with_credentials(AccessTokenCredentials::from(token.inner()))
            }
            YdbAuthConfig::Static { user, password } => {
                let mut credentials = StaticCredentials::new(
                    user.clone(),
                    password.inner().to_owned(),
                    endpoint.endpoint.parse()?,
                    endpoint.database.clone(),
                );
                if let Some(ca_file) = ca_file {
                    credentials = credentials.load_certificate(ca_file)?;
                }
                builder.with_credentials(credentials)
            }
            YdbAuthConfig::ServiceAccountKey { key_file } => {
                builder.with_credentials(ServiceAccountCredentials::from_file(key_file)?)
            }
            YdbAuthConfig::Metadata => builder.with_credentials(MetadataUrlCredentials::new()),
            YdbAuthConfig::Environment => builder.with_credentials(FromEnvCredentials::new()?),
        };

        Ok(builder.build().await?)
    }
}

async fn healthcheck(client: Arc<Client>, topic: String) -> crate::Result<()> {
    let options = DescribeTopicOptionsBuilder::default().build()?;
    client.topic_client().describe_topic(topic, options).await?;
    Ok(())
}
