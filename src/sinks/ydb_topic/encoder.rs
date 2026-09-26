use bytes::BytesMut;
use vector_lib::internal_event::{ComponentEventsDropped, UNINTENTIONAL};

use super::service::YdbTopicRequest;
use crate::sinks::prelude::*;

/// Encodes a batch of events into topic messages, one message per event.
///
/// Whole batches are encoded at once, so that encoding costs one task per
/// batch instead of one task per event.
pub(super) struct YdbTopicEncoder {
    pub(super) transformer: Transformer,
    pub(super) encoder: Encoder<()>,
}

impl YdbTopicEncoder {
    /// Returns `None` if none of the events could be encoded.
    pub(super) fn encode_batch(&self, events: Vec<Event>) -> Option<YdbTopicRequest> {
        let mut encoder = self.encoder.clone();
        let mut messages = Vec::with_capacity(events.len());
        let mut finalizers = EventFinalizers::default();
        let mut events_byte_size = 0;
        let mut encoded_byte_size = 0;
        let mut json_byte_size = telemetry().create_request_count_byte_size();

        for mut event in events {
            let event_finalizers = event.take_finalizers();
            let size = event.size_of();
            self.transformer.transform(&mut event);
            let json_size = event.estimated_json_encoded_size_of();
            let mut event_json_byte_size = telemetry().create_request_count_byte_size();
            event_json_byte_size.add_event(&event, json_size);

            let mut body = BytesMut::new();
            match encoder.serialize(event, &mut body) {
                Ok(()) => {
                    events_byte_size += size;
                    encoded_byte_size += body.len();
                    json_byte_size += event_json_byte_size;
                    finalizers.merge(event_finalizers);
                    messages.push(body.freeze());
                }
                Err(error) => {
                    emit!(SinkRequestBuildError { error });
                    emit!(ComponentEventsDropped::<UNINTENTIONAL> {
                        count: 1,
                        reason: "Failed to encode event.",
                    });
                    event_finalizers.update_status(EventStatus::Rejected);
                }
            }
        }

        if messages.is_empty() {
            return None;
        }

        let metadata = RequestMetadata::new(
            messages.len(),
            events_byte_size,
            encoded_byte_size,
            encoded_byte_size,
            json_byte_size,
        );
        Some(YdbTopicRequest::new(messages, finalizers, metadata))
    }
}
