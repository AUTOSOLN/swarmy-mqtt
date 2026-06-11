package msgstore

import (
	"database/sql"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Message is a single received MQTT message written to the store.
type Message struct {
	ID          int64
	Timestamp   time.Time
	ConnID      string
	ConnName    string
	Topic       string
	Payload     string
	QoS         int
	Retained    bool
	IsSparkplug bool
}

// Filter controls which messages Query returns.
type Filter struct {
	Search    string // substring match against topic OR payload
	ConnID    string
	Retained  *bool
	Sparkplug *bool
	Limit     int
	Offset    int
}

// QueryResult is the paginated output of Query.
type QueryResult struct {
	Total    int
	Messages []Message
}

// Store wraps an SQLite database for MQTT message persistence.
type Store struct {
	db *sql.DB
}

// Open opens (or creates) the SQLite database at path.
// A nil *Store is valid — Write and Query are no-ops when the receiver is nil.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_journal_mode=WAL&_busy_timeout=5000&_synchronous=NORMAL")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // SQLite allows only one concurrent writer
	if err := migrate(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func migrate(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS messages (
			id           INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp    TEXT NOT NULL,
			conn_id      TEXT NOT NULL,
			conn_name    TEXT NOT NULL,
			topic        TEXT NOT NULL,
			payload      TEXT NOT NULL DEFAULT '',
			qos          INTEGER NOT NULL DEFAULT 0,
			retained     INTEGER NOT NULL DEFAULT 0,
			is_sparkplug INTEGER NOT NULL DEFAULT 0
		);
		CREATE INDEX IF NOT EXISTS idx_ts    ON messages(timestamp);
		CREATE INDEX IF NOT EXISTS idx_topic ON messages(topic);
		CREATE INDEX IF NOT EXISTS idx_conn  ON messages(conn_id);
	`)
	return err
}

// Write stores a single message. No-op when s is nil.
func (s *Store) Write(m Message) error {
	if s == nil {
		return nil
	}
	r, sp := btoi(m.Retained), btoi(m.IsSparkplug)
	_, err := s.db.Exec(
		`INSERT INTO messages (timestamp,conn_id,conn_name,topic,payload,qos,retained,is_sparkplug)
		 VALUES (?,?,?,?,?,?,?,?)`,
		m.Timestamp.UTC().Format(time.RFC3339Nano),
		m.ConnID, m.ConnName, m.Topic, m.Payload, m.QoS, r, sp,
	)
	return err
}

// Query returns paginated messages matching f, newest first.
// No-op (returns empty result) when s is nil.
func (s *Store) Query(f Filter) (QueryResult, error) {
	if s == nil {
		return QueryResult{}, nil
	}
	if f.Limit <= 0 {
		f.Limit = 200
	}
	if f.Limit > 1000 {
		f.Limit = 1000
	}

	wc := buildWhere(f)

	var total int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM messages"+wc.sql, wc.args...).Scan(&total); err != nil {
		return QueryResult{}, err
	}

	rows, err := s.db.Query(
		"SELECT id,timestamp,conn_id,conn_name,topic,payload,qos,retained,is_sparkplug FROM messages"+
			wc.sql+" ORDER BY id DESC LIMIT ? OFFSET ?",
		wc.append(f.Limit, f.Offset)...,
	)
	if err != nil {
		return QueryResult{}, err
	}
	defer rows.Close()

	var msgs []Message
	for rows.Next() {
		var m Message
		var ts string
		var retained, isSp int
		if err := rows.Scan(&m.ID, &ts, &m.ConnID, &m.ConnName, &m.Topic, &m.Payload, &m.QoS, &retained, &isSp); err != nil {
			return QueryResult{}, err
		}
		m.Timestamp, _ = time.Parse(time.RFC3339Nano, ts)
		m.Retained = retained == 1
		m.IsSparkplug = isSp == 1
		msgs = append(msgs, m)
	}
	return QueryResult{Total: total, Messages: msgs}, rows.Err()
}

// whereClause bundles a parameterized SQL fragment with its bound args so they
// cannot be separated or reordered. The sql field contains only hardcoded column
// names and operators — all user-supplied values live exclusively in args.
type whereClause struct {
	sql  string
	args []any
}

func (w whereClause) append(args ...any) []any {
	return append(w.args, args...)
}

func buildWhere(f Filter) whereClause {
	var clauses []string
	var args []any

	if f.ConnID != "" {
		clauses = append(clauses, "conn_id = ?")
		args = append(args, f.ConnID)
	}
	if f.Search != "" {
		clauses = append(clauses, "(topic LIKE ? OR payload LIKE ?)")
		args = append(args, "%"+f.Search+"%", "%"+f.Search+"%")
	}
	if f.Retained != nil {
		clauses = append(clauses, "retained = ?")
		args = append(args, btoi(*f.Retained))
	}
	if f.Sparkplug != nil {
		clauses = append(clauses, "is_sparkplug = ?")
		args = append(args, btoi(*f.Sparkplug))
	}

	if len(clauses) == 0 {
		return whereClause{}
	}
	return whereClause{
		sql:  " WHERE " + strings.Join(clauses, " AND "),
		args: args,
	}
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}
