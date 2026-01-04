# Contributing to KafkaOps

Thank you for your interest in contributing to KafkaOps! This document provides guidelines for contributing.

## Development Setup

### Prerequisites

- Go 1.22+
- Node.js 20+
- Docker & Docker Compose
- A Kafka cluster (or use `docker-compose.kafka.yml`)

### Getting Started

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/kafkaops.git
   cd kafkaops
   ```

2. **Start Kafka infrastructure**
   ```bash
   docker-compose -f docker-compose.kafka.yml up -d
   ```

3. **Run the backend**
   ```bash
   cd backend
   go mod tidy
   go run ./cmd/kafkaops
   ```

4. **Run the frontend**
   ```bash
   cd frontend
   npm install
   npm run dev
   ```

## Code Style

### Go (Backend)
- Follow standard Go conventions
- Use `gofmt` for formatting
- Add comments for exported functions
- Write tests for new functionality

### TypeScript/React (Frontend)
- Use TypeScript for all new code
- Follow React 19 best practices
- Use functional components with hooks
- Ensure lists are virtualized for performance

## Project Structure

```
kafkaops/
├── backend/
│   ├── cmd/kafkaops/     # Application entry point
│   └── internal/
│       ├── api/          # HTTP handlers and routing
│       ├── decode/       # Avro/JSON decoding
│       ├── kafka/        # Kafka client (franz-go)
│       ├── schema/       # Schema Registry client
│       └── store/        # SQLite storage
├── frontend/
│   └── src/
│       ├── api/          # API client
│       ├── components/   # React components
│       ├── hooks/        # Custom hooks
│       └── pages/        # Page components
└── docs/                 # Documentation
```

## Pull Request Guidelines

1. **Create a feature branch** from `main`
2. **Write clear commit messages**
3. **Add tests** for new features
4. **Update documentation** if needed
5. **Ensure all tests pass** before submitting

## Key Constraints

When contributing, keep these constraints in mind:

- ✅ Use `franz-go` for Kafka - no Sarama or confluent-kafka-go
- ✅ Frontend lists must be virtualized
- ✅ Never send message payloads to external services
- ✅ Bulk features must be behind feature flags

## Questions?

Open an issue for discussion before starting major changes.
