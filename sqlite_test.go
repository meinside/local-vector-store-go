// sqlite_test.go

package lvs

import (
	"log"
	"os"
	"testing"
)

const (
	testDBPath = `./test.db`
)

// TestSQLite tests the SQLite database.
func TestSQLite(t *testing.T) {
	log.Printf("> testing sqlite database...")

	// remove test db before testing
	_ = os.Remove(testDBPath)

	if client, err := NewSQLiteDB(testDBPath); err != nil {
		t.Fatalf("failed to create client: %s", err)
	} else {
		if version, err := client.Version(); err != nil {
			t.Fatalf("failed to get version of client: %s", err)
		} else {
			log.Printf("database version: %s", version)

			// create virtual table
			if _, err := client.Execute(`create virtual table vec_examples using vec0(sample_embedding float[8])`); err != nil {
				t.Fatalf("failed to create virtual table: %s", err)
			} else {
				log.Printf("created virtual table using vec0")

				// batch-insert
				if res, err := client.Execute(`insert into vec_examples(rowid, sample_embedding)
					values
						(1, '[-0.200, 0.250, 0.341, -0.211, 0.645, 0.935, -0.316, -0.924]'),
						(2, '[0.443, -0.501, 0.355, -0.771, 0.707, -0.708, -0.185, 0.362]'),
						(3, '[0.716, -0.927, 0.134, 0.052, -0.669, 0.793, -0.634, -0.162]')`); err != nil {
					t.Fatalf("failed to insert vectors: %s", err)
				} else {
					rowsAffected, _ := res.RowsAffected()
					if rowsAffected != 3 {
						t.Fatalf("expected 3 rows affected, got %d", rowsAffected)
					}
					log.Printf("inserted 3 vectors")

					// insert a row
					if res, err := client.Execute(`insert into vec_examples(sample_embedding) values (?)`, FloatsToSQLiteQueryArg([]float32{-0.710, 0.330, 0.656, 0.041, -0.990, 0.726, 0.385, -0.958})); err != nil {
						t.Fatalf("failed to insert a vector: %s", err)
					} else {
						rowsAffected, _ := res.RowsAffected()
						if rowsAffected != 1 {
							t.Fatalf("expected 1 row affected, got %d", rowsAffected)
						}
						log.Printf("inserted a vector")
					}

					// select
					if rows, err := client.Query(`select rowid, distance
						from vec_examples
						where sample_embedding match '[0.890, 0.544, 0.825, 0.961, 0.358, 0.0196, 0.521, 0.175]'
						order by distance
						limit 2`); err != nil {
						t.Fatalf("failed to select vectors: %s", err)
					} else {
						for rows.Next() {
							var rowid int
							var distance float64
							if err := rows.Scan(&rowid, &distance); err != nil {
								t.Fatalf("failed to scan row: %s", err)
							} else {
								log.Printf("iterating row: rowid = %d, distance = %f", rowid, distance)
							}
						}
					}

					// TODO: test more things

					// drop table
					if _, err := client.Execute("drop table vec_examples"); err != nil {
						t.Fatalf("failed to drop table: %s", err)
					}
					log.Printf("dropped table")
				}
			}

			if err := client.Close(); err != nil {
				t.Fatalf("failed to close sqlite database: %s", err)
			}
			log.Printf("closed sqlite database")
		}
	}
}
