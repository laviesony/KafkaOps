# Build stage for backend
FROM golang:1.22-alpine AS backend-builder

WORKDIR /app/backend

# Install build dependencies
RUN apk add --no-cache git

# Copy go mod files
COPY backend/go.mod backend/go.sum* ./

# Download dependencies
RUN go mod download

# Copy source code
COPY backend/ .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /kafkaops ./cmd/kafkaops

# Build stage for frontend
FROM node:20-alpine AS frontend-builder

WORKDIR /app/frontend

# Copy package files
COPY frontend/package*.json ./

# Install dependencies
RUN npm ci

# Copy source code
COPY frontend/ .

# Build production bundle
RUN npm run build

# Final runtime stage
FROM alpine:3.19

WORKDIR /app

# Install ca-certificates for HTTPS connections
RUN apk add --no-cache ca-certificates tzdata

# Copy backend binary
COPY --from=backend-builder /kafkaops /app/kafkaops

# Copy frontend build
COPY --from=frontend-builder /app/frontend/dist /app/static

# Create directory for SQLite database
RUN mkdir -p /app/data

# Environment variables
ENV KAFKAOPS_SERVER_ADDR=:8080
ENV KAFKAOPS_SQLITE_DSN=/app/data/kafkaops.db

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

# Run the application
CMD ["/app/kafkaops"]
