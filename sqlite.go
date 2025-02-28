// sqlite.go

package lvs

import (
	"database/sql"
	"fmt"
	"log"

	sv "github.com/asg017/sqlite-vec-go-bindings/cgo"

	_ "github.com/mattn/go-sqlite3"
)

// SQLiteDB struct
type SQLiteDB struct {
	db *sql.DB
}

// NewSQLiteDB creates a new client for SQLite.
func NewSQLiteDB(path string) (*SQLiteDB, error) {
	sv.Auto()

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}
	return &SQLiteDB{
		db: db,
	}, nil
}

// Close closes the client.
func (c *SQLiteDB) Close() error {
	sv.Cancel()

	// TODO: clean things up

	return c.db.Close()
}

// Version returns the version string of the database.
func (c *SQLiteDB) Version() (version string, err error) {
	err = c.db.QueryRow("select vec_version() as version").Scan(&version)
	if err != nil {
		return "unknown", fmt.Errorf("failed to get version: %w", err)
	}
	return version, nil
}

// Execute executes a query.
func (c *SQLiteDB) Execute(query string, args ...any) (res sql.Result, err error) {
	res, err = c.db.Exec(query, args...)
	return res, err
}

// Query executes a query and returns the resulting rows.
func (c *SQLiteDB) Query(query string, args ...any) (rows *sql.Rows, err error) {
	rows, err = c.db.Query(query, args...)
	return rows, err
}

// FloatsToSQLiteQueryArg serializes a float32 vector for parameters in SQLite queries.
func FloatsToSQLiteQueryArg(vector []float32) []byte {
	bytes, err := sv.SerializeFloat32(vector)
	if err != nil {
		log.Printf("Failed to serialize vector: %v", err)
	}
	return bytes
}
