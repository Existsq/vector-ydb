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
				fails if the topic cannot be described. Vector starts even if YDB is
				unreachable and connects on the first write.
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

				Several batches can be in flight at once (see `request.concurrency`), but they are
				always handed to the write session in the order they were produced, so the order of
				events in the topic matches the order in which Vector received them. The order can
				only change when a batch is retried after the write session fails and has to be
				recreated. Set `request.concurrency` to `none` to rule this out at the cost of
				throughput.
				"""
		}
		batching: {
			title: "Batching and compression"
			body: """
				A batch is sent when it reaches `batch.max_events`, `batch.max_bytes` or
				`batch.timeout_secs`, whichever comes first. `batch.max_bytes` is compared with the
				in-memory size of the events, which is several times larger than the encoded
				message: a 200-byte log line takes about 1 KiB. With short log lines the
				`batch.max_events` limit is therefore usually reached first.

				Compression (`codec`) is applied to every message separately, so short messages
				barely shrink: `gzip` saved less than 10% of the traffic on 200-byte log lines while
				using about twice as much CPU as `raw`. `auto` compresses messages as well and costs
				as much CPU as `gzip`. Prefer `raw` for short log lines.

				Set `dashboard.enabled` to watch how batches fill up and which limit sends them.
				"""
		}
		delivery_guarantees: {
			title: "Delivery guarantees"
			body: """
				Events are acknowledged only after YDB confirms that every message of the batch is
				written.

				Short outages, restarts and reconnects are handled within the write session: pending
				messages are resent with their original sequence numbers, so YDB deduplicates them.
				When a request times out, its messages stay in the write session and a retry waits for
				them instead of writing them again. Only when the write session itself fails and has
				to be recreated are the unconfirmed messages written again, which may produce
				duplicates. Set a stable `producer_id` so that sequence numbers continue across
				restarts of Vector.
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
