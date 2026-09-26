package metadata

components: sinks: ydb_topic: {
	title: "YDB Topic"

	classes: {
		delivery:      "at_least_once"
		development:   "beta"
		egress_method: "batch"
		service_providers: ["Yandex"]
		stateful: false
	}

	features: {
		auto_generated:   true
		acknowledgements: true
		healthcheck: enabled: true
		send: {
			batch: {
				enabled:      true
				max_bytes:    8_388_608
				max_events:   1000
				timeout_secs: 1.0
			}
			compression: enabled: false
			encoding: {
				enabled: true
				codec: {
					enabled: true
					enum: ["json", "text"]
				}
			}
			request: {
				enabled: true
				headers: false
			}
			tls: enabled: false
			to: {
				service: services.ydb
				interface: {
					socket: {
						direction: "outgoing"
						protocols: ["http"]
						ssl: "optional"
					}
				}
			}
		}
	}

	support: {
		requirements: [
			"""
				The topic must exist before Vector starts writing to it. The healthcheck
				fails if the topic cannot be described.
				""",
		]
		warnings: []
	}

	configuration: generated.components.sinks.ydb_topic.configuration

	input: {
		logs: true
		metrics: {
			counter:      true
			distribution: true
			gauge:        true
			histogram:    true
			set:          true
			summary:      true
		}
		traces: false
	}

	how_it_works: {
		protocol: {
			title: "Topic API and Go SDK compatibility"
			body:  """
				The sink writes to [YDB topics](\(urls.ydb_topics)) through the native Topic API
				(`StreamWrite`), the same protocol that the [YDB Go SDK](\(urls.ydb_go_sdk)) uses.
				Each event is encoded with the configured `encoding` and written as a single topic message,
				so it can be consumed by `ydb-go-sdk` readers (`topicreader`) as well as any other YDB SDK.

				Messages are compressed with the `raw` or `gzip` topic codecs only. Both are decoded by the
				Go SDK out of the box, without registering custom decoders.

				The `endpoint` option accepts the same connection string format as the Go SDK's
				`ydb.Open`, for example `grpc://localhost:2136/local` or
				`grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/...`.
				"""
		}
		partitioning: {
			title: "Partitioning and ordering"
			body: """
				As with the YDB Go SDK defaults, the producer ID is also used as the message group ID,
				so all messages written by a Vector instance go to the same partition in order. Set
				`partition_id` to write to a specific partition instead.

				Only one request is in flight by default (`request.concurrency` is `1`), which keeps
				the order of events in the topic. Increasing the concurrency improves throughput but
				the order of batches is no longer guaranteed.
				"""
		}
		delivery_guarantees: {
			title: "Delivery guarantees"
			body: """
				Events are acknowledged only after YDB confirms that every message of the batch is
				written. Failed batches are retried, which may write some messages more than once.
				Set a stable `producer_id` so that YDB can deduplicate messages that the writer
				resends after a reconnect.
				"""
		}
		authentication: {
			title: "Authentication"
			body:  """
				The `auth.strategy` option selects how Vector authenticates to YDB. The `environment`
				strategy reads the same `YDB_*_CREDENTIALS` environment variables as the Go SDK
				`ydb-go-sdk-auth-environ` package. See the [YDB authentication documentation](\(urls.ydb_auth))
				for details.
				"""
		}
	}
}
