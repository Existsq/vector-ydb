use std::io;

use bytes::{Bytes, BytesMut};
use tokio_util::codec::Encoder as _;

use crate::sinks::prelude::*;

/// A single encoded event, ready to be written as one topic message.
#[derive(Clone)]
pub(super) struct YdbTopicMessage {
    pub(super) body: Bytes,
    finalizers: EventFinalizers,
    metadata: RequestMetadata,
}

impl Finalizable for YdbTopicMessage {
    fn take_finalizers(&mut self) -> EventFinalizers {
        std::mem::take(&mut self.finalizers)
    }
}

impl MetaDescriptive for YdbTopicMessage {
    fn get_metadata(&self) -> &RequestMetadata {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut RequestMetadata {
        &mut self.metadata
    }
}

impl ByteSizeOf for YdbTopicMessage {
    fn size_of(&self) -> usize {
        // Used by the batcher, so that `batch.max_bytes` bounds the encoded
        // size of the messages sent to YDB.
        self.body.len()
    }

    fn allocated_bytes(&self) -> usize {
        0
    }
}

pub(super) struct YdbTopicEncoder {
    pub(super) encoder: Encoder<()>,
    pub(super) transformer: Transformer,
}

impl encoding::Encoder<Event> for YdbTopicEncoder {
    fn encode_input(
        &self,
        mut input: Event,
        writer: &mut dyn io::Write,
    ) -> io::Result<(usize, GroupedCountByteSize)> {
        self.transformer.transform(&mut input);

        let mut byte_size = telemetry().create_request_count_byte_size();
        byte_size.add_event(&input, input.estimated_json_encoded_size_of());

        let mut body = BytesMut::new();
        let mut encoder = self.encoder.clone();
        encoder
            .encode(input, &mut body)
            .map_err(|error| io::Error::other(format!("unable to encode event: {error}")))?;

        write_all(writer, 1, body.as_ref())?;

        Ok((body.len(), byte_size))
    }
}

pub(super) struct YdbTopicRequestBuilder {
    pub(super) encoder: YdbTopicEncoder,
}

impl RequestBuilder<Event> for YdbTopicRequestBuilder {
    type Metadata = EventFinalizers;
    type Events = Event;
    type Encoder = YdbTopicEncoder;
    type Payload = Bytes;
    type Request = YdbTopicMessage;
    type Error = io::Error;

    fn compression(&self) -> Compression {
        // Compression is handled by the topic codec, so that consumers can
        // decode the messages transparently.
        Compression::None
    }

    fn encoder(&self) -> &Self::Encoder {
        &self.encoder
    }

    fn split_input(
        &self,
        mut input: Event,
    ) -> (Self::Metadata, RequestMetadataBuilder, Self::Events) {
        let builder = RequestMetadataBuilder::from_event(&input);
        let finalizers = input.take_finalizers();
        (finalizers, builder, input)
    }

    fn build_request(
        &self,
        finalizers: Self::Metadata,
        metadata: RequestMetadata,
        payload: EncodeResult<Self::Payload>,
    ) -> Self::Request {
        YdbTopicMessage {
            body: payload.into_payload(),
            finalizers,
            metadata,
        }
    }
}
