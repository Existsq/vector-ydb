package metadata

generated: components: sinks: ydb_topic: configuration: {
	acknowledgements: {
		description: """
			Controls how acknowledgements are handled for this sink.

			See [End-to-end Acknowledgements][e2e_acks] for more information on how event acknowledgement is handled.

			[e2e_acks]: https://vector.dev/docs/architecture/end-to-end-acknowledgements/
			"""
		required: false
		type:     _schemaDefinitions["vector_core::config::AcknowledgementsConfig"]
	}
	auth: {
		description: "Authentication settings for YDB."
		required:    false
		type: object: options: {
			key_file: {
				description:   "Path to the JSON service account key file."
				relevant_when: "strategy = \"service_account_key\""
				required:      true
				type: string: examples: ["/etc/vector/sa-key.json"]
			}
			password: {
				description:   "The password."
				relevant_when: "strategy = \"static\""
				required:      true
				type: string: {}
			}
			strategy: {
				description: """
					The authentication strategy used to connect to YDB.

					See the YDB [authentication documentation][ydb_auth_docs] for details about each strategy.

					[ydb_auth_docs]: https://ydb.tech/docs/en/reference/ydb-sdk/auth
					"""
				required: false
				type: string: {
					default: "anonymous"
					enum: {
						access_token: "A static access token, such as an IAM token."
						anonymous:    "Anonymous access (no credentials)."
						environment: """
															Credentials selected from environment variables, the same way the YDB Go SDK
															(`ydb-go-sdk-auth-environ`) does.

															The following variables are checked in order: `YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS`,
															`YDB_ANONYMOUS_CREDENTIALS`, `YDB_METADATA_CREDENTIALS`, `YDB_ACCESS_TOKEN_CREDENTIALS`.
															"""
						metadata:            "Token from the Yandex Cloud virtual machine metadata service."
						service_account_key: "Authorized key file of a Yandex Cloud service account."
						static:              "Login and password of a YDB built-in user."
					}
				}
			}
			token: {
				description:   "The access token."
				relevant_when: "strategy = \"access_token\""
				required:      true
				type: string: {}
			}
			user: {
				description:   "The user name."
				relevant_when: "strategy = \"static\""
				required:      true
				type: string: examples: ["root"]
			}
		}
	}
	batch: {
		description: "Event batching behavior."
		required:    false
		type: object: options: {
			max_bytes: {
				description: """
					The maximum size of a batch that is processed by a sink.

					This is based on the uncompressed size of the batched events, before they are
					serialized or compressed.
					"""
				required: false
				type: uint: {
					default: 8388608
					unit:    "bytes"
				}
			}
			max_events: {
				description: "The maximum size of a batch before it is flushed."
				required:    false
				type: uint: {
					default: 1000
					unit:    "events"
				}
			}
			timeout_secs: {
				description: "The maximum age of a batch before it is flushed."
				required:    false
				type: float: {
					default: 1.0
					unit:    "seconds"
				}
			}
		}
	}
	codec: {
		description: """
			Compression codec applied to messages written to the topic.

			All of the supported codecs can be decoded by the YDB Go SDK
			(`ydb-go-sdk/v3/topic`) readers without registering custom decoders.
			"""
		required: false
		type: string: {
			default: "auto"
			enum: {
				auto: "Pick the codec (`raw` or `gzip`) that produces the smallest payload, among those allowed by the topic."
				gzip: "Compress messages with gzip."
				raw:  "Write messages without compression."
			}
		}
	}
	encoding: {
		description: """
			Encoding configuration.
			Configures how events are encoded into raw bytes.
			The selected encoding also determines which input types (logs, metrics, traces) are supported.
			"""
		required: true
		type: object: options: {
			avro: {
				description:   "Apache Avro-specific encoder options."
				relevant_when: "codec = \"avro\""
				required:      true
				type: object: options: schema: {
					description: "The Avro schema."
					required:    true
					type: string: examples: ["{ \"type\": \"record\", \"name\": \"log\", \"fields\": [{ \"name\": \"message\", \"type\": \"string\" }] }"]
				}
			}
			cef: {
				description:   "The CEF Serializer Options."
				relevant_when: "codec = \"cef\""
				required:      true
				type: object: options: {
					device_event_class_id: {
						description: """
																Unique identifier for each event type. Identifies the type of event reported.
																The value length must be less than or equal to 1023.
																"""
						required: true
						type: string: {}
					}
					device_product: {
						description: """
																Identifies the product of a vendor.
																The part of a unique device identifier. No two products can use the same combination of device vendor and device product.
																The value length must be less than or equal to 63.
																"""
						required: true
						type: string: {}
					}
					device_vendor: {
						description: """
																Identifies the vendor of the product.
																The part of a unique device identifier. No two products can use the same combination of device vendor and device product.
																The value length must be less than or equal to 63.
																"""
						required: true
						type: string: {}
					}
					device_version: {
						description: """
																Identifies the version of the problem. The combination of the device product, vendor, and this value make up the unique id of the device that sends messages.
																The value length must be less than or equal to 31.
																"""
						required: true
						type: string: {}
					}
					extensions: {
						description: """
																The collection of key-value pairs. Keys are the keys of the extensions, and values are paths that point to the extension values of a log event.
																The event can have any number of key-value pairs in any order.
																"""
						required: true
						type: object: options: "*": {
							description: "This is a path that points to the extension value of a log event."
							required:    true
							type: string: {}
						}
					}
					name: {
						description: """
																This is a path that points to the human-readable description of a log event.
																The value length must be less than or equal to 512.
																Equals "cef.name" by default.
																"""
						required: true
						type: string: {}
					}
					severity: {
						description: """
																This is a path that points to the field of a log event that reflects importance of the event.

																It must point to a number from 0 to 10.
																0 = lowest_importance, 10 = highest_importance.
																Set to "cef.severity" by default.
																"""
						required: true
						type: string: {}
					}
					version: {
						description: """
																CEF Version. Can be either 0 or 1.
																Set to "0" by default.
																"""
						required: true
						type: string: enum: {
							V0: "CEF specification version 0.1."
							V1: "CEF specification version 1.x."
						}
					}
				}
			}
			codec: {
				description: "The codec to use for encoding events."
				required:    true
				type: string: enum: {
					avro: """
						Encodes an event as an [Apache Avro][apache_avro] message.

						[apache_avro]: https://avro.apache.org/
						"""
					cef: "Encodes an event as a CEF (Common Event Format) formatted message."
					csv: """
						Encodes an event as a CSV message.

						This codec must be configured with fields to encode.
						"""
					gelf: """
						Encodes an event as a [GELF][gelf] message.

						This codec is experimental for the following reason:

						The GELF specification is more strict than the actual Graylog receiver.
						Vector's encoder currently adheres more strictly to the GELF spec, with
						the exception that some characters such as `@`  are allowed in field names.

						Other GELF codecs, such as Loki's, use a [Go SDK][implementation] that is maintained
						by Graylog and is much more relaxed than the GELF spec.

						Going forward, Vector will use that [Go SDK][implementation] as the reference implementation, which means
						the codec might continue to relax the enforcement of the specification.

						[gelf]: https://docs.graylog.org/docs/gelf
						[implementation]: https://github.com/Graylog2/go-gelf/blob/v2/gelf/reader.go
						"""
					json: """
						Encodes an event as [JSON][json].

						[json]: https://www.json.org/
						"""
					logfmt: """
						Encodes an event as a [logfmt][logfmt] message.

						[logfmt]: https://brandur.org/logfmt
						"""
					native: """
						Encodes an event in the [native Protocol Buffers format][vector_native_protobuf].

						This codec is **[experimental][experimental]**.

						[vector_native_protobuf]: https://github.com/vectordotdev/vector/blob/master/lib/vector-core/proto/event.proto
						[experimental]: https://vector.dev/highlights/2022-03-31-native-event-codecs
						"""
					native_json: """
						Encodes an event in the [native JSON format][vector_native_json].

						This codec is **[experimental][experimental]**.

						[vector_native_json]: https://github.com/vectordotdev/vector/blob/master/lib/codecs/tests/data/native_encoding/schema.cue
						[experimental]: https://vector.dev/highlights/2022-03-31-native-event-codecs
						"""
					protobuf: """
						Encodes an event as a [Protobuf][protobuf] message.

						[protobuf]: https://protobuf.dev/
						"""
					raw_message: """
						No encoding.

						This encoding uses the `message` field of a log event.

						Be careful if you are modifying your log events (for example, by using a `remap`
						transform) and removing the message field while doing additional parsing on it, as this
						could lead to the encoding emitting empty strings for the given event.
						"""
					text: """
						Plain text encoding.

						This encoding uses the `message` field of a log event. For metrics, it uses an
						encoding that resembles the Prometheus export format.

						Be careful if you are modifying your log events (for example, by using a `remap`
						transform) and removing the message field while doing additional parsing on it, as this
						could lead to the encoding emitting empty strings for the given event.
						"""
				}
			}
			csv: {
				description:   "The CSV Serializer Options."
				relevant_when: "codec = \"csv\""
				required:      true
				type: object: options: {
					capacity: {
						description: """
																Sets the capacity (in bytes) of the internal buffer used in the CSV writer.
																This defaults to 8192 bytes (8KB).
																"""
						required: false
						type: uint: default: 8192
					}
					delimiter: {
						description: "The field delimiter to use when writing CSV."
						required:    false
						type: ascii_char: default: ","
					}
					double_quote: {
						description: """
																Enables double quote escapes.

																This is enabled by default, but you can disable it. When disabled, quotes in
																field data are escaped instead of doubled.
																"""
						required: false
						type: bool: default: true
					}
					escape: {
						description: """
																The escape character to use when writing CSV.

																In some variants of CSV, quotes are escaped using a special escape character
																like \\ (instead of escaping quotes by doubling them).

																To use this, `double_quotes` needs to be disabled as well; otherwise, this setting is ignored.
																"""
						required: false
						type: ascii_char: default: "\""
					}
					fields: {
						description: """
																Configures the fields that are encoded, as well as the order in which they
																appear in the output.

																If a field is not present in the event, the output for that field is an empty string.

																Values of type `Array`, `Object`, and `Regex` are not supported, and the
																output for any of these types is an empty string.
																"""
						required: true
						type: array: items: type: string: {}
					}
					quote: {
						description: "The quote character to use when writing CSV."
						required:    false
						type: ascii_char: default: "\""
					}
					quote_style: {
						description: "The quoting style to use when writing CSV data."
						required:    false
						type: string: {
							default: "necessary"
							enum: {
								always: "Always puts quotes around every field."
								necessary: """
																			Puts quotes around fields only when necessary.
																			They are necessary when fields contain a quote, delimiter, or record terminator.
																			Quotes are also necessary when writing an empty record
																			(which is indistinguishable from a record with one empty field).
																			"""
								never: "Never writes quotes, even if it produces invalid CSV data."
								non_numeric: """
																			Puts quotes around all fields that are non-numeric.
																			This means that when writing a field that does not parse as a valid float or integer,
																			quotes are used even if they aren't strictly necessary.
																			"""
							}
						}
					}
				}
			}
			except_fields: {
				description: "List of fields that are excluded from the encoded event."
				required:    false
				type: array: items: type: string: {}
			}
			gelf: {
				description:   "The GELF Serializer Options."
				relevant_when: "codec = \"gelf\""
				required:      false
				type: object: options: max_chunk_size: {
					description: """
						Maximum size for each GELF chunked datagram (including 12-byte header).
						Chunking starts when datagrams exceed this size.
						For Graylog target, keep at or below 8192 bytes; for Vector target (`gelf` decoding with `chunked_gelf` framing), up to 65,500 bytes is recommended.
						"""
					required: false
					type: uint: default: 8192
				}
			}
			json: {
				description:   "Options for the JsonSerializer."
				relevant_when: "codec = \"json\""
				required:      false
				type: object: options: pretty: {
					description: "Whether to use pretty JSON formatting."
					required:    false
					type: bool: default: false
				}
			}
			metric_tag_values: {
				description: """
					Controls how metric tag values are encoded.

					When set to `single`, only the last non-bare value of tags are displayed with the
					metric. When set to `full`, all metric tags are exposed as separate assignments.
					When set to `auto`, tag values are encoded using their underlying shape.
					"""
				relevant_when: "codec = \"json\" or codec = \"text\""
				required:      false
				type: string: {
					default: "single"
					enum: {
						auto: """
															Tag values are exposed using their underlying shape: single-value tags as strings,
															multi-value tags as arrays. A length-1 array round-trips as a scalar; use `Full` to
															force array shape.
															"""
						full: "All tags are exposed as arrays of either string or null values."
						single: """
															Tag values are exposed as single strings, the same as they were before this config
															option. Tags with multiple values show the last assigned value, and null values
															are ignored.
															"""
					}
				}
			}
			only_fields: {
				description: "List of fields that are included in the encoded event."
				required:    false
				type: array: items: type: string: {}
			}
			protobuf: {
				description:   "Options for the Protobuf serializer."
				relevant_when: "codec = \"protobuf\""
				required:      true
				type: object: options: {
					desc_file: {
						description: """
																The path to the protobuf descriptor set file.

																This file is the output of `protoc -I <include path> -o <desc output path> <proto>`

																You can read more [here](https://buf.build/docs/reference/images/#how-buf-images-work).
																"""
						required: true
						type: string: examples: ["/etc/vector/protobuf_descriptor_set.desc"]
					}
					message_type: {
						description: "The name of the message type to use for serializing."
						required:    true
						type: string: examples: ["package.Message"]
					}
					use_json_names: {
						description: """
																Use JSON field names (camelCase) instead of protobuf field names (snake_case).

																When enabled, the serializer looks for fields using their JSON names as defined
																in the `.proto` file (for example `jobDescription` instead of `job_description`).

																This is useful when working with data that has already been converted from JSON or
																when interfacing with systems that use JSON naming conventions.
																"""
						required: false
						type: bool: default: false
					}
				}
			}
			timestamp_format: {
				description: "Format used for timestamp fields."
				required:    false
				type: string: enum: {
					rfc3339:    "Represent the timestamp as a RFC 3339 timestamp."
					unix:       "Represent the timestamp as a Unix timestamp."
					unix_float: "Represent the timestamp as a Unix timestamp in floating point."
					unix_ms:    "Represent the timestamp as a Unix timestamp in milliseconds."
					unix_ns:    "Represent the timestamp as a Unix timestamp in nanoseconds."
					unix_us:    "Represent the timestamp as a Unix timestamp in microseconds."
				}
			}
		}
	}
	endpoint: {
		description: """
			The YDB connection string.

			Uses the same format as the YDB Go SDK (`ydb.Open`): `grpc://` or `grpcs://` (TLS) scheme,
			host, explicit port, and the database either as the URL path or as the `database` query
			parameter.
			"""
		required: true
		type: string: examples: ["grpc://localhost:2136/local", "grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1gxxx/etnxxx"]
	}
	partition_id: {
		description: "Write all messages to the given partition instead of routing them by producer ID."
		required:    false
		type: int: examples: [
			0
		]
	}
	producer_id: {
		description: """
			The producer ID of the write session.

			The producer ID is also used as the message group ID, so all messages written by
			this sink go to the same partition, as with the YDB Go SDK defaults. Setting a stable
			producer ID lets YDB deduplicate messages resent after reconnects.

			When not set, a random UUID is generated each time the sink starts.
			"""
		required: false
		type: string: examples: ["vector-node-1"]
	}
	request: {
		description: """
			Middleware settings for outbound requests.

			Various settings can be configured, such as concurrency and rate limits, timeouts, and retry behavior.

			Note that the retry backoff policy follows the Fibonacci sequence.
			"""
		required: false
		type: object: options: {
			adaptive_concurrency: {
				description: """
					Configuration of adaptive concurrency parameters.

					These parameters typically do not require changes from the default, and incorrect values can lead to meta-stable or
					unstable performance and sink behavior. Proceed with caution.
					"""
				required: false
				type: object: options: {
					decrease_ratio: {
						description: """
																The fraction of the current value to set the new concurrency limit when decreasing the limit.

																Valid values are greater than `0` and less than `1`. Smaller values cause the algorithm to scale back rapidly
																when latency increases.

																**Note**: The new limit is rounded down after applying this ratio.
																"""
						required: false
						type: float: default: 0.9
					}
					ewma_alpha: {
						description: """
																The weighting of new measurements compared to older measurements.

																Valid values are greater than `0` and less than `1`.

																ARC uses an exponentially weighted moving average (EWMA) of past RTT measurements as a reference to compare with
																the current RTT. Smaller values cause this reference to adjust more slowly, which may be useful if a service has
																unusually high response variability.
																"""
						required: false
						type: float: default: 0.4
					}
					initial_concurrency: {
						description: """
																The initial concurrency limit to use. If not specified, the initial limit is 1 (no concurrency).

																Datadog recommends setting this value to your service's average limit if you're seeing that it takes a
																long time to ramp up adaptive concurrency after a restart. You can find this value by looking at the
																`adaptive_concurrency_limit` metric.
																"""
						required: false
						type: uint: default: 1
					}
					max_concurrency_limit: {
						description: """
																The maximum concurrency limit.

																The adaptive request concurrency limit does not go above this bound. This is put in place as a safeguard.
																"""
						required: false
						type: uint: default: 200
					}
					rtt_deviation_scale: {
						description: """
																Scale of RTT deviations which are not considered anomalous.

																Valid values are greater than or equal to `0`, and reasonable values range from `1.0` to `3.0`.

																When calculating the past RTT average, a secondary “deviation” value is also computed that indicates how variable
																those values are. That deviation is used when comparing the past RTT average to the current measurements, so we
																can ignore increases in RTT that are within an expected range. This factor is used to scale up the deviation to
																an appropriate range. Larger values cause the algorithm to ignore larger increases in the RTT.
																"""
						required: false
						type: float: default: 2.5
					}
				}
			}
			concurrency: {
				description: """
					Configuration for outbound request concurrency.

					This can be set either to one of the below enum values or to a positive integer, which denotes
					a fixed concurrency limit.
					"""
				required: false
				type: {
					string: {
						default: "adaptive"
						enum: {
							adaptive: """
															Concurrency is managed by Vector's [Adaptive Request Concurrency][arc] feature.

															[arc]: https://vector.dev/docs/architecture/arc/
															"""
							none: """
															A fixed concurrency of 1.

															Only one request can be outstanding at any given time.
															"""
						}
					}
					uint: {}
				}
			}
			rate_limit_duration_secs: {
				description: "The time window used for the `rate_limit_num` option."
				required:    false
				type: uint: {
					default: 1
					unit:    "seconds"
				}
			}
			rate_limit_num: {
				description: "The maximum number of requests allowed within the `rate_limit_duration_secs` time window."
				required:    false
				type: uint: {
					default: 9223372036854775807
					unit:    "requests"
				}
			}
			retry_attempts: {
				description: "The maximum number of retries to make for failed requests."
				required:    false
				type: uint: {
					default: 9223372036854775807
					unit:    "retries"
				}
			}
			retry_initial_backoff_secs: {
				description: """
					The amount of time to wait before attempting the first retry for a failed request.

					After the first retry has failed, the Fibonacci sequence is used to select future backoffs.
					"""
				required: false
				type: uint: {
					default: 1
					unit:    "seconds"
				}
			}
			retry_jitter_mode: {
				description: "The jitter mode to use for retry backoff behavior."
				required:    false
				type: string: {
					default: "Full"
					enum: {
						Full: """
															Full jitter.

															The random delay is anywhere from 0 up to the maximum current delay calculated by the backoff
															strategy.

															Incorporating full jitter into your backoff strategy can greatly reduce the likelihood
															of creating accidental denial of service (DoS) conditions against your own systems when
															many clients are recovering from a failure state.
															"""
						None: "No jitter."
					}
				}
			}
			retry_max_duration_secs: {
				description: "The maximum amount of time to wait between retries."
				required:    false
				type: uint: {
					default: 30
					unit:    "seconds"
				}
			}
			timeout_secs: {
				description: """
					The time a request can take before being aborted.

					Datadog highly recommends that you do not lower this value below the service's internal timeout, as this could
					create orphaned requests, pile on retries, and result in duplicate data downstream.
					"""
				required: false
				type: uint: {
					default: 60
					unit:    "seconds"
				}
			}
		}
	}
	tls: {
		description: """
			TLS settings for the connection to YDB.

			TLS is enabled by using the `grpcs://` scheme in `endpoint`.
			"""
		required: false
		type: object: options: ca_file: {
			description: """
				Path to a PEM-encoded CA certificate used to verify the YDB server certificate.

				When not set, the system root certificates are used.
				"""
			required: false
			type: string: examples: ["/etc/ssl/certs/ydb-ca.pem"]
		}
	}
	topic: {
		description: """
			The topic path to write to.

			A relative path is resolved against the database.
			"""
		required: true
		type: string: examples: ["vector/logs"]
	}
}
