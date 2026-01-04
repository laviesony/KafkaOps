# KafkaOps

A **local-first** Kafka Dead Letter Queue (DLQ) remediation IDE. Inspect, fix, and replay poison-pill messages without sending data outside your infrastructure.

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Go](https://img.shields.io/badge/go-1.22+-00ADD8.svg)
![React](https://img.shields.io/badge/react-19-61DAFB.svg)

## Features

- 🔒 **Local-first** — Message payloads never leave your machine
- ⚡ **100k+ message support** — Virtualized lists handle massive DLQs
- 🔍 **Avro/JSON decoding** — Automatic schema detection via Schema Registry
- ✏️ **Monaco Editor** — Edit payloads with syntax highlighting and validation
- 🚀 **Fix & Replay** — One-click message replay to original topics
- 📦 **Bulk Operations** — Patch multiple messages with RFC 6902 JSON Patch (PRO)

## Quick Start

### Using Docker Compose (Recommended)

```bash
# Start everything: Kafka, Schema Registry, KafkaOps
docker-compose up -d

# Open http://localhost:5173
```

### Manual Setup

**Prerequisites:**
- Go 1.22+
- Node.js 20+
- Kafka cluster (local or remote)

**Backend:**
```bash
cd backend
go mod tidy
go run ./cmd/kafkaops
```

**Frontend:**
```bash
cd frontend
npm install
npm run dev
```

## Configuration

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `KAFKAOPS_KAFKA_BROKERS` | Comma-separated Kafka brokers | `localhost:9092` |
| `KAFKAOPS_DLQ_TOPIC` | DLQ topic to consume | — |
| `KAFKAOPS_SCHEMA_REGISTRY_URL` | Schema Registry URL | — |
| `KAFKAOPS_SERVER_ADDR` | Backend HTTP address | `:8080` |
| `KAFKAOPS_SQLITE_DSN` | SQLite database path | `kafkaops.db` |
| `KAFKAOPS_KAFKA_TLS` | Enable TLS | `false` |
| `KAFKAOPS_KAFKA_SASL_MECHANISM` | SASL mechanism | — |
| `KAFKAOPS_KAFKA_SASL_USERNAME` | SASL username | — |
| `KAFKAOPS_KAFKA_SASL_PASSWORD` | SASL password | — |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         Frontend                             │
│  React 19 · Vite · Monaco Editor · @tanstack/react-virtual  │
└──────────────────────────┬──────────────────────────────────┘
                           │ REST API
┌──────────────────────────▼──────────────────────────────────┐
│                         Backend                              │
│  Go 1.22 · franz-go · goavro · modernc.org/sqlite           │
└─────┬──────────────────────┬─────────────────────┬──────────┘
      │                      │                     │
      ▼                      ▼                     ▼
┌───────────┐        ┌─────────────┐       ┌─────────────┐
│   Kafka   │        │   Schema    │       │   SQLite    │
│  Cluster  │        │  Registry   │       │  (embedded) │
└───────────┘        └─────────────┘       └─────────────┘
```

## Security

- ✅ All Kafka credentials stay local
- ✅ Message payloads are never exfiltrated
- ✅ Schema Registry access is read-only by default
- ✅ `X-Exception-*` headers stripped on replay
- ✅ `X-Original-*` headers preserved

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/api/messages` | List messages (paginated) |
| GET | `/api/messages/:id` | Get single message |
| POST | `/api/messages/:id/replay` | Replay message |
| POST | `/api/bulk/preview` | Preview bulk patch (PRO) |
| POST | `/api/bulk/execute` | Execute bulk patch (PRO) |

## Development

```bash
# Run backend with hot reload (using air)
cd backend && air

# Run frontend dev server
cd frontend && npm run dev

# Run tests
cd backend && go test ./...
cd frontend && npm test
```

## License

MIT License - See [LICENSE](LICENSE) for details.

---

Built with ❤️ for Kafka operators who deal with DLQs daily.
