use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::future::Shared;
use snafu::Snafu;
use tokio::sync::{OnceCell, mpsc, oneshot};
use tracing::Instrument;
use ydb::{Client, TopicWriter, TopicWriterMessage, TopicWriterOptions, YdbError};

use super::{config::ClientFactory, dashboard::Stats};
use crate::sinks::prelude::*;

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(0);

/// A batch of encoded events written to the topic as one message each.
#[derive(Clone)]
pub(super) struct YdbTopicRequest {
    /// Identifies the request across retries.
    id: u64,
    pub(super) messages: Vec<Bytes>,
    finalizers: EventFinalizers,
    metadata: RequestMetadata,
}

impl YdbTopicRequest {
    pub(super) fn new(
        messages: Vec<Bytes>,
        finalizers: EventFinalizers,
        metadata: RequestMetadata,
    ) -> Self {
        Self {
            id: NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed),
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
    #[snafu(display("Failed to connect to YDB: {error}"))]
    Connect { error: crate::Error },

    #[snafu(display("Failed to create YDB topic writer: {source}"))]
    CreateWriter { source: YdbError },

    #[snafu(display("Failed to write message to YDB topic: {source}"))]
    Write { source: YdbError },

    #[snafu(display("The YDB topic writer task has stopped"))]
    Closed,
}

#[derive(Clone, Debug, Default)]
pub(super) struct YdbTopicRetryLogic;

impl RetryLogic for YdbTopicRetryLogic {
    type Error = YdbTopicError;
    type Request = YdbTopicRequest;
    type Response = YdbTopicResponse;

    fn is_retriable_error(&self, error: &Self::Error) -> bool {
        match error {
            YdbTopicError::Connect { error } => error
                .downcast_ref::<YdbError>()
                .is_none_or(is_retriable_connect_error),
            YdbTopicError::CreateWriter { source } | YdbTopicError::Write { source } => {
                is_retriable(source)
            }
            YdbTopicError::Closed => false,
        }
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

/// Connecting fails with a generic error when YDB is unreachable, so only
/// errors explicitly reported by the server are treated as permanent.
fn is_retriable_connect_error(error: &YdbError) -> bool {
    match error {
        YdbError::YdbStatusError(_) | YdbError::TransportGRPCStatus(_) => is_retriable(error),
        _ => true,
    }
}

/// Whether a failed write should be retried with a new write session.
///
/// Transient transport and server errors are retried; errors caused by the
/// request itself (bad topic, permissions, unsupported codec) are not.
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

/// Acknowledgements of all messages of one request.
type SharedAcks = Shared<BoxFuture<'static, Result<(), YdbError>>>;

type EnqueueResult = Result<(u64, SharedAcks), YdbTopicError>;

/// Requests whose messages are enqueued to a writer and are not confirmed yet.
///
/// Requests older than this many requests are assumed to be abandoned (their
/// retries were exhausted) and are forgotten.
const MAX_PENDING_REQUESTS: u64 = 10_000;

/// Lazily connected YDB client, shared by the writer task and the healthcheck.
struct LazyClient {
    factory: ClientFactory,
    client: OnceCell<Client>,
}

impl LazyClient {
    async fn get(&self) -> Result<&Client, YdbTopicError> {
        self.client
            .get_or_try_init(|| self.factory.connect())
            .await
            .map_err(|error| YdbTopicError::Connect { error })
    }
}

enum Command {
    /// Enqueue the messages of a request to the writer.
    Enqueue {
        request_id: u64,
        messages: Vec<Bytes>,
        reply: oneshot::Sender<EnqueueResult>,
    },
    /// All acknowledgements of a request were received, or one of them failed.
    Finished {
        request_id: u64,
        failed_generation: Option<u64>,
    },
    /// Close the write session gracefully.
    Shutdown { reply: oneshot::Sender<()> },
}

/// Writes batches of messages to a YDB topic through a single long-lived
/// write session.
///
/// The write session is owned by a background task. Requests are passed to it
/// through a channel from `call`, which the driver invokes in request order, so
/// batches are enqueued in order even with several requests in flight.
///
/// Requests are idempotent while the write session is alive: when a request
/// times out, its messages stay in the writer and are still delivered, so a
/// retry of the same request waits for their acknowledgements instead of
/// writing the messages again.
#[derive(Clone)]
pub(super) struct YdbTopicService {
    commands: mpsc::UnboundedSender<Command>,
    client: Arc<LazyClient>,
    stats: Arc<Stats>,
}

impl YdbTopicService {
    pub(super) fn new(
        factory: ClientFactory,
        options: TopicWriterOptions,
        stats: Arc<Stats>,
    ) -> Self {
        let client = Arc::new(LazyClient {
            factory,
            client: OnceCell::new(),
        });
        let (commands, receiver) = mpsc::unbounded_channel();
        let task = WriterTask {
            client: Arc::clone(&client),
            options,
            writer: None,
            next_generation: 0,
            pending: HashMap::new(),
            stats: Arc::clone(&stats),
        };
        tokio::spawn(task.run(receiver).in_current_span());
        Self {
            commands,
            client,
            stats,
        }
    }

    /// Returns the YDB client, connecting on first use.
    pub(super) async fn client(&self) -> Result<&Client, YdbTopicError> {
        self.client.get().await
    }

    /// Gracefully closes the write session, if any.
    ///
    /// Dropping a `TopicWriter` aborts its background tasks, so it is stopped
    /// explicitly once the sink has no more requests in flight.
    pub(super) async fn shutdown(&self) {
        let (reply, done) = oneshot::channel();
        if self.commands.send(Command::Shutdown { reply }).is_ok() {
            _ = done.await;
        }
    }
}

struct CurrentWriter {
    generation: u64,
    writer: TopicWriter,
}

struct PendingRequest {
    generation: u64,
    messages: usize,
    acks: SharedAcks,
}

struct WriterTask {
    client: Arc<LazyClient>,
    options: TopicWriterOptions,
    writer: Option<CurrentWriter>,
    next_generation: u64,
    pending: HashMap<u64, PendingRequest>,
    stats: Arc<Stats>,
}

impl WriterTask {
    async fn run(mut self, mut commands: mpsc::UnboundedReceiver<Command>) {
        while let Some(command) = commands.recv().await {
            match command {
                Command::Enqueue {
                    request_id,
                    messages,
                    reply,
                } => {
                    // The requester may be gone (for example after a timeout).
                    // The messages are enqueued anyway, so that its retry finds them.
                    _ = reply.send(self.enqueue(request_id, messages).await);
                }
                Command::Finished {
                    request_id,
                    failed_generation,
                } => {
                    if let Some(pending) = self.pending.remove(&request_id) {
                        self.stats.unacked_add(-(pending.messages as i64));
                    }
                    if let Some(generation) = failed_generation {
                        self.invalidate(generation);
                    }
                }
                Command::Shutdown { reply } => {
                    self.stop().await;
                    _ = reply.send(());
                    return;
                }
            }
        }
        self.stop().await;
    }

    async fn enqueue(&mut self, request_id: u64, messages: Vec<Bytes>) -> EnqueueResult {
        if let Some(current) = &self.writer
            && let Some(previous) = self
                .pending
                .get(&request_id)
                .filter(|previous| previous.generation == current.generation)
        {
            return Ok((current.generation, previous.acks.clone()));
        }

        let current = match &mut self.writer {
            Some(current) => current,
            None => {
                let writer = self
                    .client
                    .get()
                    .await?
                    .topic_client()
                    .create_writer_with_params(self.options.clone())
                    .await
                    .map_err(|source| YdbTopicError::CreateWriter { source })?;
                self.next_generation += 1;
                self.stats.session_opened();
                self.writer.insert(CurrentWriter {
                    generation: self.next_generation,
                    writer,
                })
            }
        };
        let generation = current.generation;

        let count = messages.len();
        let mut acks = Vec::with_capacity(count);
        for body in messages {
            match current
                .writer
                .write_with_ack_future(TopicWriterMessage::new(Vec::from(body)))
                .await
            {
                Ok(ack) => acks.push(ack),
                Err(source) => {
                    self.invalidate(generation);
                    return Err(YdbTopicError::Write { source });
                }
            }
        }

        let acks = futures::future::try_join_all(acks)
            .map(|result| result.map(|_| ()))
            .boxed()
            .shared();

        self.forget(|id, _| id.saturating_add(MAX_PENDING_REQUESTS) <= request_id);
        self.stats.unacked_add(count as i64);
        self.pending.insert(
            request_id,
            PendingRequest {
                generation,
                messages: count,
                acks: acks.clone(),
            },
        );
        Ok((generation, acks))
    }

    /// Drops the writer after an error, unless it was already replaced.
    ///
    /// Messages of a dropped writer are never acknowledged, so the requests
    /// that wrote them must write them again when retried.
    fn invalidate(&mut self, generation: u64) {
        if self
            .writer
            .as_ref()
            .is_some_and(|current| current.generation == generation)
        {
            self.writer = None;
            self.stats.session_closed();
        }
        self.forget(|_, pending| pending.generation == generation);
    }

    /// Removes the pending requests matching `predicate`.
    fn forget(&mut self, predicate: impl Fn(u64, &PendingRequest) -> bool) {
        let stats = &self.stats;
        self.pending.retain(|id, pending| {
            let forget = predicate(*id, pending);
            if forget {
                stats.unacked_add(-(pending.messages as i64));
            }
            !forget
        });
    }

    async fn stop(&mut self) {
        self.forget(|_, _| true);
        self.stats.session_closed();
        if let Some(current) = self.writer.take()
            && let Err(error) = current.writer.stop().await
        {
            warn!(message = "Failed to gracefully close YDB topic writer.", %error);
        }
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
        let YdbTopicRequest {
            id: request_id,
            messages,
            metadata,
            ..
        } = request;
        let bytes_sent = messages.iter().map(Bytes::len).sum();
        let count = messages.len();
        let events_byte_size = metadata.into_events_estimated_json_encoded_byte_size();

        // Sending here rather than in the returned future keeps the order in
        // which the driver issues the requests.
        let (reply, enqueued) = oneshot::channel();
        let sent = self.commands.send(Command::Enqueue {
            request_id,
            messages,
            reply,
        });
        let commands = self.commands.clone();
        let stats = Arc::clone(&self.stats);
        stats.request_started();

        Box::pin(async move {
            let result = async {
                sent.map_err(|_| YdbTopicError::Closed)?;
                let (generation, acks) = enqueued.await.map_err(|_| YdbTopicError::Closed)??;

                let result = acks.await;
                _ = commands.send(Command::Finished {
                    request_id,
                    failed_generation: result.is_err().then_some(generation),
                });
                result.map_err(|source| YdbTopicError::Write { source })
            }
            .await;
            stats.request_finished(count, bytes_sent, result.is_ok());
            result?;

            Ok(YdbTopicResponse {
                events_byte_size,
                bytes_sent,
            })
        })
    }
}
