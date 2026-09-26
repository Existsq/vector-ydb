package metadata

services: ydb: {
	name:     "YDB"
	thing:    "\(name) topics"
	url:      urls.ydb
	versions: null

	description: "[YDB](\(urls.ydb)) is an open source distributed SQL database that also provides persistent [topics](\(urls.ydb_topics)) for message streaming."
}
