Added a new `ydb_topic` sink that publishes events to [YDB](https://ydb.tech) topics through the
native Topic API. Messages are written with the `raw` or `gzip` codecs and the connection string
uses the YDB Go SDK format, so the data can be consumed by `ydb-go-sdk` topic readers.

authors: existsq
