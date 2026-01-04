# Changelog

All notable changes to KafkaOps will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2024-01-04

### Added
- Initial release
- Go backend with franz-go Kafka client
- React 19 frontend with Vite
- DLQ message consumption and indexing
- Avro/JSON/String payload decoding
- Schema Registry integration with caching
- SQLite embedded storage with WAL mode
- Virtualized message list (100k+ messages)
- Monaco Editor for payload editing
- Fix & Replay workflow
- Bulk patch operations (PRO feature, disabled by default)
- Docker Compose for local development
- Kafka UI for debugging

### Security
- Local-first architecture - no data exfiltration
- Automatic stripping of X-Exception-* headers on replay
- Preservation of X-Original-* headers
- Read-only Schema Registry access by default
