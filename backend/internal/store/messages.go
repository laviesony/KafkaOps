package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
)

// MessageIndex represents a DLQ message stored in SQLite.
type MessageIndex struct {
	ID             int64             `json:"id"`
	Topic          string            `json:"topic"`
	Partition      int               `json:"partition"`
	Offset         int64             `json:"offset"`
	Key            []byte            `json:"key,omitempty"`
	Value          []byte            `json:"-"` // Raw value not exposed in JSON
	Headers        map[string]string `json:"headers,omitempty"`
	Timestamp      int64             `json:"timestamp"`
	DecodedPayload any               `json:"decodedPayload,omitempty"`
	DecodeError    string            `json:"decodeError,omitempty"`
}

// MessageStore provides persistence operations for DLQ messages.
type MessageStore struct {
	db *DB
}

// NewMessageStore creates a new message store.
func NewMessageStore(db *DB) *MessageStore {
	return &MessageStore{db: db}
}

// InsertMessage inserts a single message into the store.
func (s *MessageStore) InsertMessage(msg *MessageIndex) error {
	headersJSON, err := json.Marshal(msg.Headers)
	if err != nil {
		return fmt.Errorf("failed to marshal headers: %w", err)
	}

	decodedJSON, err := json.Marshal(msg.DecodedPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal decoded payload: %w", err)
	}

	_, err = s.db.Conn().Exec(`
		INSERT INTO messages (topic, partition, offset, key, value, headers, timestamp, decoded_payload, decode_error)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(topic, partition, offset) DO UPDATE SET
			value = excluded.value,
			headers = excluded.headers,
			decoded_payload = excluded.decoded_payload,
			decode_error = excluded.decode_error
	`, msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value, string(headersJSON), msg.Timestamp, string(decodedJSON), msg.DecodeError)

	return err
}

// InsertBatch inserts multiple messages in a single transaction.
func (s *MessageStore) InsertBatch(messages []*MessageIndex) error {
	tx, err := s.db.Conn().Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO messages (topic, partition, offset, key, value, headers, timestamp, decoded_payload, decode_error)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(topic, partition, offset) DO UPDATE SET
			value = excluded.value,
			headers = excluded.headers,
			decoded_payload = excluded.decoded_payload,
			decode_error = excluded.decode_error
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, msg := range messages {
		headersJSON, _ := json.Marshal(msg.Headers)
		decodedJSON, _ := json.Marshal(msg.DecodedPayload)

		_, err = stmt.Exec(msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value,
			string(headersJSON), msg.Timestamp, string(decodedJSON), msg.DecodeError)
		if err != nil {
			return fmt.Errorf("failed to insert message at offset %d: %w", msg.Offset, err)
		}
	}

	return tx.Commit()
}

// QueryMessagesParams defines filtering parameters for message queries.
type QueryMessagesParams struct {
	Topic  string
	Limit  int
	Offset int
}

// QueryMessages retrieves messages with pagination.
func (s *MessageStore) QueryMessages(params QueryMessagesParams) ([]*MessageIndex, int, error) {
	// Default limit
	if params.Limit <= 0 {
		params.Limit = 50
	}
	if params.Limit > 1000 {
		params.Limit = 1000
	}

	// Count total for pagination
	var total int
	countQuery := "SELECT COUNT(*) FROM messages"
	args := []any{}

	if params.Topic != "" {
		countQuery += " WHERE topic = ?"
		args = append(args, params.Topic)
	}

	err := s.db.Conn().QueryRow(countQuery, args...).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count messages: %w", err)
	}

	// Fetch messages
	query := `SELECT id, topic, partition, offset, key, value, headers, timestamp, decoded_payload, decode_error
		FROM messages`

	if params.Topic != "" {
		query += " WHERE topic = ?"
	}
	query += " ORDER BY timestamp DESC LIMIT ? OFFSET ?"
	args = append(args, params.Limit, params.Offset)

	rows, err := s.db.Conn().Query(query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query messages: %w", err)
	}
	defer rows.Close()

	var messages []*MessageIndex
	for rows.Next() {
		msg := &MessageIndex{}
		var headersJSON, decodedJSON sql.NullString

		err := rows.Scan(&msg.ID, &msg.Topic, &msg.Partition, &msg.Offset, &msg.Key, &msg.Value,
			&headersJSON, &msg.Timestamp, &decodedJSON, &msg.DecodeError)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan message: %w", err)
		}

		if headersJSON.Valid {
			json.Unmarshal([]byte(headersJSON.String), &msg.Headers)
		}
		if decodedJSON.Valid {
			json.Unmarshal([]byte(decodedJSON.String), &msg.DecodedPayload)
		}

		messages = append(messages, msg)
	}

	return messages, total, nil
}

// GetMessageByID retrieves a single message by ID.
func (s *MessageStore) GetMessageByID(id int64) (*MessageIndex, error) {
	msg := &MessageIndex{}
	var headersJSON, decodedJSON sql.NullString

	err := s.db.Conn().QueryRow(`
		SELECT id, topic, partition, offset, key, value, headers, timestamp, decoded_payload, decode_error
		FROM messages WHERE id = ?
	`, id).Scan(&msg.ID, &msg.Topic, &msg.Partition, &msg.Offset, &msg.Key, &msg.Value,
		&headersJSON, &msg.Timestamp, &decodedJSON, &msg.DecodeError)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get message: %w", err)
	}

	if headersJSON.Valid {
		json.Unmarshal([]byte(headersJSON.String), &msg.Headers)
	}
	if decodedJSON.Valid {
		json.Unmarshal([]byte(decodedJSON.String), &msg.DecodedPayload)
	}

	return msg, nil
}

// GetMessageByOffset retrieves a message by topic/partition/offset.
func (s *MessageStore) GetMessageByOffset(topic string, partition int, offset int64) (*MessageIndex, error) {
	msg := &MessageIndex{}
	var headersJSON, decodedJSON sql.NullString

	err := s.db.Conn().QueryRow(`
		SELECT id, topic, partition, offset, key, value, headers, timestamp, decoded_payload, decode_error
		FROM messages WHERE topic = ? AND partition = ? AND offset = ?
	`, topic, partition, offset).Scan(&msg.ID, &msg.Topic, &msg.Partition, &msg.Offset, &msg.Key, &msg.Value,
		&headersJSON, &msg.Timestamp, &decodedJSON, &msg.DecodeError)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get message: %w", err)
	}

	if headersJSON.Valid {
		json.Unmarshal([]byte(headersJSON.String), &msg.Headers)
	}
	if decodedJSON.Valid {
		json.Unmarshal([]byte(decodedJSON.String), &msg.DecodedPayload)
	}

	return msg, nil
}
