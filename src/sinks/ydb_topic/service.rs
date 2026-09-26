use std::{
    sync::Arc,
    task::{Context, Poll},
};

use bytes::Bytes;
use snafu::Snafu;
use tokio::sync::Mutex;
use ydb::{Client, TopicWriter, TopicWriterMessage, TopicWriterOptions, YdbError};

use super::request_builder::YdbTopicMessage;
use crate::sinks::prelude::*;

/// A batch of encoded events written to the topic as one message each.
#[derive(Clone)]
pub(super) struct YdbTopicRequest {
    pub(super) messages: Vec<Bytes>,
    finalizers: EventFinalizers,
    metadata: RequestMetadata,
}

impl YdbTopicRequest {
    pub(super) fn new(mut messages: Vec<YdbTopicMessage>) -> Self {
        let metadata = RequestMetadata::from_batch(
            messages
                .iter()
                .map(|message| message.get_metadata().clone()),
        );
        let finalizers = messages.take_finalizers();
        let messages = messages.into_iter().map(|message| message.body).collect();
        Self {
            messages,
            finalizers,
            metadata,
        }
    }
}

impl Finalizable for YdbTopicRequest {
    fn take_finalizers(&mut self) -> EventFinalizers {
        std::mem::take(&mut self.finalizers)
    }
}

impl MetaDescriptive for YdbTopicRequest {
    fn get_metadata(&self) -> &RequestMetadata {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut RequestMetadata {
        &mut self.metadata
    }
}

pub(super) struct YdbTopicResponse {
    events_byte_size: GroupedCountByteSize,
    bytes_sent: usize,
}

impl DriverResponse for YdbTopicResponse {
    fn event_status(&self) -> EventStatus {
        EventStatus::Delivered
    }

    fn events_sent(&self) -> &GroupedCountByteSize {
        &self.events_byte_size
    }

    fn bytes_sent(&self) -> Option<usize> {
        Some(self.bytes_sent)
    }
}

#[derive(Debug, Snafu)]
pub(super) enum YdbTopicError {
    #[snafu(display("Failed to create YDB topic writer: {source}"))]
    CreateWriter { source: YdbError },

    #[snafu(display("Failed to write message to YDB topic: {source}"))]
    Write { source: YdbError },
}

impl YdbTopicError {
    const fn source_error(&self) -> &YdbError {
        match self {
            Self::CreateWriter { source } | Self::Write { source } => source,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct YdbTopicRetryLogic;

impl RetryLogic for YdbTopicRetryLogic {
    type Error = YdbTopicError;
    type Request = YdbTopicRequest;
    type Response = YdbTopicResponse;

    fn is_retriable_error(&self, error: &Self::Error) -> bool {
        is_retriable(error.source_error())
    }
}

// YDB operation status codes (`Ydb.StatusIds.StatusCode`) that indicate a transient failure.
const YDB_STATUS_ABORTED: i32 = 400040;
const YDB_STATUS_UNAVAILABLE: i32 = 400050;
const YDB_STATUS_OVERLOADED: i32 = 400060;
const YDB_STATUS_TIMEOUT: i32 = 400090;
const YDB_STATUS_BAD_SESSION: i32 = 400100;
const YDB_STATUS_SESSION_EXPIRED: i32 = 400150;
const YDB_STATUS_UNDETERMINED: i32 = 400170;
const YDB_STATUS_SESSION_BUSY: i32 = 400190;

// gRPC status codes that indicate a transient failure.
const GRPC_CANCELLED: i32 = 1;
const GRPC_UNKNOWN: i32 = 2;
const GRPC_DEADLINE_EXCEEDED: i32 = 4;
const GRPC_RESOURCE_EXHAUSTED: i32 = 8;
const GRPC_ABORTED: i32 = 10;
const GRPC_INTERNAL: i32 = 13;
const GRPC_UNAVAILABLE: i32 = 14;

/// Writes to a topic are always safe to retry: the writer is recreated after
/// an error and, with a stable `producer_id`, YDB deduplicates messages by
/// their sequence numbers.
pub(super) fn is_retriable(error: &YdbError) -> bool {
    match error {
        YdbError::TransportDial(_) | YdbError::Transport(_) | YdbError::DeadlineExceeded => true,
        YdbError::TransportGRPCStatus(status) => matches!(
            status.code() as i32,
            GRPC_CANCELLED
                | GRPC_UNKNOWN
                | GRPC_DEADLINE_EXCEEDED
                | GRPC_RESOURCE_EXHAUSTED
                | GRPC_ABORTED
                | GRPC_INTERNAL
                | GRPC_UNAVAILABLE
        ),
        YdbError::YdbStatusError(status) => matches!(
            status.operation_status,
            YDB_STATUS_ABORTED
                | YDB_STATUS_UNAVAILABLE
                | YDB_STATUS_OVERLOADED
                | YDB_STATUS_TIMEOUT
                | YDB_STATUS_BAD_SESSION
                | YDB_STATUS_SESSION_EXPIRED
                | YDB_STATUS_UNDETERMINED
                | YDB_STATUS_SESSION_BUSY
        ),
        // The writer reports a closed or broken write session this way.
        YdbError::Custom(message) => message.contains("writer was closed"),
        _ => false,
    }
}

struct Inner {
    client: Arc<Client>,
    options: TopicWriterOptions,
    writer: Mutex<Option<Arc<TopicWriter>>>,
}

/// Writes batches of messages to a YDB topic through a single long-lived
/// write session.
///
/// The write session is created lazily and recreated after any error, since
/// the YDB topic writer does not recover from fatal errors by itself.
#[derive(Clone)]
pub(super) struct YdbTopicService {
    inner: Arc<Inner>,
}

impl YdbTopicService {
    pub(super) fn new(client: Arc<Client>, options: TopicWriterOptions) -> Self {
        Self {
            inner: Arc::new(Inner {
                client,
                options,
                writer: Mutex::new(None),
            }),
        }
    }
}

impl YdbTopicService {
    /// Gracefully closes the write session, if any.
    ///
    /// Dropping a `TopicWriter` aborts its background tasks, so it is stopped
    /// explicitly once the sink has no more requests in flight.
    pub(super) async fn shutdown(&self) {
        let writer = self.inner.writer.lock().await.take();
        if let Some(writer) = writer.and_then(|writer| Arc::try_unwrap(writer).ok())
            && let Err(error) = writer.stop().await
        {
            warn!(message = "Failed to gracefully close YDB topic writer.", %error);
        }
    }
}

impl Inner {
    async fn write(&self, messages: Vec<Bytes>) -> Result<(), YdbTopicError> {
        // The lock is held while the batch is enqueued, so that the messages of
        // a batch are contiguous in the topic even with concurrent requests.
        let mut guard = self.writer.lock().await;
        let writer = match guard.as_ref() {
            Some(writer) => Arc::clone(writer),
            None => {
                let writer = self
                    .client
                    .topic_client()
                    .create_writer_with_params(self.options.clone())
                    .await
                    .map_err(|source| YdbTopicError::CreateWriter { source })?;
                let writer = Arc::new(writer);
                *guard = Some(Arc::clone(&writer));
                writer
            }
        };

        let mut acks = Vec::with_capacity(messages.len());
        for body in messages {
            match writer
                .write_with_ack_future(TopicWriterMessage::new(body.to_vec()))
                .await
            {
                Ok(ack) => acks.push(ack),
                Err(source) => {
                    *guard = None;
                    return Err(YdbTopicError::Write { source });
                }
            }
        }
        drop(guard);

        if let Err(source) = futures::future::try_join_all(acks).await {
            let mut guard = self.writer.lock().await;
            if guard
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &writer))
            {
                *guard = None;
            }
            return Err(YdbTopicError::Write { source });
        }

        Ok(())
    }
}

impl Service<YdbTopicRequest> for YdbTopicService {
    type Response = YdbTopicResponse;
    type Error = YdbTopicError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: YdbTopicRequest) -> Self::Future {
        let inner = Arc::clone(&self.inner);
        let YdbTopicRequest {
            messages, metadata, ..
        } = request;

        Box::pin(async move {
            let bytes_sent = messages.iter().map(Bytes::len).sum();
            let events_byte_size = metadata.into_events_estimated_json_encoded_byte_size();

            inner.write(messages).await?;

            Ok(YdbTopicResponse {
                events_byte_size,
                bytes_sent,
            })
        })
    }
}
