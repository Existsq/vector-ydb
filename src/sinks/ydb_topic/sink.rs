use std::sync::Arc;

use super::{
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
}

impl YdbTopicSink {
    pub(super) const fn new(
        service: Svc<YdbTopicService, YdbTopicRetryLogic>,
        writer: YdbTopicService,
        batch_settings: BatcherSettings,
        transformer: Transformer,
        encoder: Encoder<()>,
    ) -> Self {
        Self {
            service,
            writer,
            batch_settings,
            transformer,
            encoder,
        }
    }

    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let encoder = Arc::new(YdbTopicEncoder {
            transformer: self.transformer,
            encoder: self.encoder,
        });

        let result = input
            .batched(self.batch_settings.as_byte_size_config())
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
