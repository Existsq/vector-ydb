use super::{
    request_builder::{YdbTopicEncoder, YdbTopicRequestBuilder},
    service::{YdbTopicRequest, YdbTopicRetryLogic, YdbTopicService},
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
        let request_builder = YdbTopicRequestBuilder {
            encoder: YdbTopicEncoder {
                encoder: self.encoder,
                transformer: self.transformer,
            },
        };

        let result = input
            .request_builder(default_request_builder_concurrency_limit(), request_builder)
            .filter_map(|request| async move {
                match request {
                    Err(error) => {
                        emit!(SinkRequestBuildError { error });
                        None
                    }
                    Ok(message) => Some(message),
                }
            })
            .batched(self.batch_settings.as_byte_size_config())
            .map(YdbTopicRequest::new)
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
