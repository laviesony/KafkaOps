You are implementing **KafkaOps**, a local-first Kafka Dead Letter Queue (DLQ) remediation IDE.

CORE PRINCIPLES (NON-NEGOTIABLE):
- Local-first: NEVER send Kafka message payloads outside the local machine.
- Kafka client MUST be github.com/twmb/franz-go (pure Go).
- No Sarama. No confluent-kafka-go.
- Handle 100,000+ messages without UI or backend degradation.
- DLQs are write-once Kafka topics containing poison-pill messages.

ARCHITECTURE:
- Backend: Go 1.22+
- Frontend: React 19 + Vite
- Backend exposes REST APIs consumed by frontend.
- Backend runs locally or inside user VPC.
- Embedded SQLite is used for indexing consumed messages.
- Frontend uses virtualization for all message lists.

SECURITY:
- Strip X-Exception-* headers on replay.
- Preserve X-Original-* headers.
- Never exfiltrate message payloads.
- Schema Registry access is read-only unless explicitly requested.

DESERIALIZATION RULES:
- Check Magic Byte (byte[0] == 0x0).
- Bytes 1–4 are schema ID (big-endian uint32).
- Fetch schema from Schema Registry.
- Deserialize using generic Avro.
- Fallback to JSON / string if schema not found.

FRONTEND RULES:
- All lists must use @tanstack/react-virtual.
- Never render raw arrays of messages.
- Monaco Editor is mandatory for payload editing.
- JSON schema validation must occur before replay.

MONETIZATION BOUNDARY:
- Bulk remediation features must be behind feature flags.
- Single-message Fix & Replay must remain free.

Assume the user is an experienced engineer.
Write production-quality code.
Prefer clarity over cleverness.
-- SUMMARY FOR AI CODING AGENTS --

This repository is a local-first DLQ remediation IDE with a Go backend and a React+Vite frontend.
Key constraints you'll rely on:
- Kafka client: `github.com/twmb/franz-go` (pure Go). No Sarama or confluent-kafka-go.
- Backend: Go 1.22+; Frontend: React 19 + Vite. Embedded SQLite is used for indexing.

Quick orientation (where to look):
- Backend entry: `backend/cmd/kafkaops/main.go` (server bootstrap).
- API layer: `backend/internal/api/` (server, routes, handlers).
- Kafka integration: `backend/internal/kafka/` (client, consumer, producer).
- Schema handling: `backend/internal/schema/` (registry + cache).
- Decoding rules: `backend/internal/decode/decoder.go` — follow Magic Byte + schema ID rules.
- Storage/indexing: `backend/internal/store/` (SQLite helpers + message index model).

Project-specific patterns and rules (do not deviate):
- Local-first: Never send message payloads to remote services; keep payloads on disk or in-memory.
- Deserialization pipeline: Check magic byte (0x0); bytes 1–4 are big-endian schema ID; attempt Avro generic decode via Schema Registry; fall back to JSON/string if missing.
- Header handling: Always strip `X-Exception-*` headers before replay; preserve `X-Original-*` headers.
- Performance: Message lists must be virtualized on the frontend and the backend must support bulk reads of 100k+ records via paginated indexes.

Developer workflows (commands & examples):
- Build backend:
	- `cd backend && go build ./...`
- Run backend locally (dev):
	- `cd backend && go run ./cmd/kafkaops`
- Frontend dev (from repo root):
	- `cd frontend && npm install && npm run dev`  # frontend folder: standard Vite app
- Tests: unit tests live next to packages. Run `go test ./...` inside `backend`.

Integration notes and gotchas:
- Schema Registry access should be read-only during normal use. Any write access must be explicitly gated and visible in PRs.
- Use `franz-go` for all Kafka client functionality; wrap client creation in `backend/internal/kafka/client.go` for centralized configuration.
- SQLite DSN: use file-backed DB for local runs, `:memory:` for unit tests.

When editing code, prefer small, well-scoped changes and add tests for behavior you change. If you add new feature flags (bulk remediation), ensure they live behind a clearly named feature flag and default to disabled.

If something in these instructions looks incomplete or you need a code example for a specific integration (franz-go consumer group, Avro generic decoding using schema ID, or the SQLite schema), ask for that snippet and I will produce it.
