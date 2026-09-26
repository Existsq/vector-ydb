use std::sync::Arc;

use super::{
    dashboard::{Dashboard, Stats},
    encoder::YdbTopicEncoder,
    service::{YdbTopicRetryLogic, YdbTopicService},
};
use crate::sinks::prelude::*;

pub(super) struct YdbTopicSink {
    service: Svc<YdbTopicService, YdbTopicRetryLogic>,
    /// Handle to the underlying service, used to close the write session on shutdown.
    writer: YdbTopicService,
    batch_settings: BatcherSettings,
    transformer: Transformer,
    encoder: Encoder<()>,
    stats: Arc<Stats>,
    /// Title of the dashboard, when it is enabled.
    dashboard: Option<String>,
}

impl YdbTopicSink {
    pub(super) const fn new(
        service: Svc<YdbTopicService, YdbTopicRetryLogic>,
        writer: YdbTopicService,
        batch_settings: BatcherSettings,
        transformer: Transformer,
        encoder: Encoder<()>,
        stats: Arc<Stats>,
        dashboard: Option<String>,
    ) -> Self {
        Self {
            service,
            writer,
            batch_settings,
            transformer,
            encoder,
            stats,
            dashboard,
        }
    }

    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let encoder = Arc::new(YdbTopicEncoder {
            transformer: self.transformer,
            encoder: self.encoder,
        });

        let _dashboard = self
            .dashboard
            .map(|title| Dashboard::start(Arc::clone(&self.stats), self.batch_settings, title));

        let (stats_in, stats_out) = (Arc::clone(&self.stats), Arc::clone(&self.stats));
        let batch_settings = self.batch_settings;
        let result = input
            .map(move |event| {
                stats_in.event_in(event.size_of());
                event
            })
            .batched(self.batch_settings.as_byte_size_config())
            .map(move |events: Vec<Event>| {
                let bytes = events.iter().map(ByteSizeOf::size_of).sum();
                stats_out.batch_out(events.len(), bytes, &batch_settings);
                events
            })
            .concurrent_map(default_request_builder_concurrency_limit(), move |events| {
                let encoder = Arc::clone(&encoder);
                Box::pin(async move { encoder.encode_batch(events) })
            })
            .filter_map(future::ready)
            .into_driver(self.service)
            .protocol("grpc")
            .run()
            .await;

        self.writer.shutdown().await;
        result
    }
}

#[async_trait]
impl StreamSink<Event> for YdbTopicSink {
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}
