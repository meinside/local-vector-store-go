// gorm.go

package lvs

import (
	"database/sql"
	"database/sql/driver"
	"fmt"

	sv "github.com/asg017/sqlite-vec-go-bindings/cgo"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// GormDB struct
type GormDB struct {
	db *gorm.DB
}

// Client returns the client.
func (d *GormDB) Client() *gorm.DB {
	return d.db
}

// Close closes the client.
func (d *GormDB) Close() error {
	sv.Cancel()

	// TODO: clean things up

	return nil
}

// NewGormDB creates a new gorm database client.
func NewGormDB(path string) (*GormDB, error) {
	sv.Auto()

	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to open gorm database: %w", err)
	} else {
		return &GormDB{
			db: db,
		}, nil
	}
}

// Version returns the version string of the database.
func (d *GormDB) Version() (string, error) {
	var v struct {
		Version string
	}

	tx := d.db.Raw("select vec_version() as version").Take(&v)
	if tx.Error != nil {
		return "unknown", fmt.Errorf("failed to get version: %w", tx.Error)
	}

	return v.Version, nil
}

// Execute executes a query.
func (d *GormDB) Execute(query string, args ...any) (numRowsAffected int64, err error) {
	tx := d.db.Exec(query, args...)
	if tx.Error != nil {
		return 0, tx.Error
	}
	return tx.RowsAffected, nil
}

// Query executes a query and returns the resulting rows.
//
// NOTE: returned `rows` should be `.Close()`d after use.
func (d *GormDB) Query(query string, args ...any) (rows *sql.Rows, err error) {
	tx := d.db.Raw(query, args...)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return tx.Rows()
}

// Float32ArrayArg type for Gorm
type Float32ArrayArg []float32

// Value implements the driver.Valuer interface for Float32ArrayArg.
func (f Float32ArrayArg) Value() (driver.Value, error) {
	return []byte(FloatsToSQLiteQueryArg(f)), nil
}

// FloatsToGormQueryArg serializes a float32 vector for Gorm query argument.
func FloatsToGormQueryArg(vector []float32) Float32ArrayArg {
	return Float32ArrayArg(vector)
}
