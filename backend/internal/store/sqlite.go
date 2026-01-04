package store

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

// DB wraps the SQLite connection with KafkaOps-specific methods.
type DB struct {
	conn *sql.DB
}

// OpenDB opens (or creates) an embedded SQLite database.
// Use file path for production, ":memory:" for tests.
func OpenDB(dsn string) (*DB, error) {
	conn, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite: %w", err)
	}

	// Enable WAL mode for better concurrent read performance
	if _, err := conn.Exec("PRAGMA journal_mode=WAL"); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to enable WAL: %w", err)
	}

	// Enable foreign keys
	if _, err := conn.Exec("PRAGMA foreign_keys=ON"); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to enable foreign keys: %w", err)
	}

	db := &DB{conn: conn}
	if err := db.migrate(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("migration failed: %w", err)
	}

	return db, nil
}

// migrate runs schema migrations.
func (db *DB) migrate() error {
	schema := `
	CREATE TABLE IF NOT EXISTS messages (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		topic TEXT NOT NULL,
		partition INTEGER NOT NULL,
		offset INTEGER NOT NULL,
		key BLOB,
		value BLOB NOT NULL,
		headers TEXT, -- JSON encoded headers
		timestamp INTEGER NOT NULL,
		decoded_payload TEXT, -- JSON encoded decoded payload
		decode_error TEXT,
		created_at INTEGER DEFAULT (strftime('%s', 'now')),
		UNIQUE(topic, partition, offset)
	);

	CREATE INDEX IF NOT EXISTS idx_messages_topic ON messages(topic);
	CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp);
	CREATE INDEX IF NOT EXISTS idx_messages_topic_partition_offset ON messages(topic, partition, offset);
	`

	_, err := db.conn.Exec(schema)
	return err
}

// Conn returns the underlying sql.DB for advanced queries.
func (db *DB) Conn() *sql.DB {
	return db.conn
}

// Close closes the database connection.
func (db *DB) Close() error {
	return db.conn.Close()
}
