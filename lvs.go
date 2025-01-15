package lvs

import (
	"database/sql"
	"fmt"
	"log"

	sv "github.com/asg017/sqlite-vec-go-bindings/cgo"
	_ "github.com/mattn/go-sqlite3"
)

// Client struct
type Client struct {
	db *sql.DB
}

// New creates a new client.
func New(path string) (*Client, error) {
	sv.Auto()

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	return &Client{
		db: db,
	}, nil
}

// Close closes the client.
func (c *Client) Close() error {
	sv.Cancel()

	return c.db.Close()
}

// Version returns the version string of the database.
func (c *Client) Version() (version string, err error) {
	err = c.db.QueryRow("select vec_version() as version").Scan(&version)
	if err != nil {
		return "unknown", fmt.Errorf("failed to get version: %w", err)
	}
	return version, nil
}

// Execute executes a query.
func (c *Client) Execute(query string, args ...any) (res sql.Result, err error) {
	res, err = c.db.Exec(query, args...)
	return res, err
}

// Query executes a query and returns the resulting rows.
func (c *Client) Query(query string, args ...any) (rows *sql.Rows, err error) {
	rows, err = c.db.Query(query, args...)
	return rows, err
}

// FloatsToBytes serializes a float32 vector for parameters in SQLite queries.
func FloatsToBytes(vector []float32) []byte {
	bytes, err := sv.SerializeFloat32(vector)
	if err != nil {
		log.Printf("Failed to serialize vector: %v", err)
	}
	return bytes
}
