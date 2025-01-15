package lvs

import (
	"log"
	"testing"
)

const (
	testDBPath = `./test.db`
)

// TestClient tests the client's overall functionalities.
//
// https://github.com/asg017/sqlite-vec#sample-usage
func TestClient(t *testing.T) {
	if client, err := New(testDBPath); err != nil {
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

					// insert a row
					if res, err := client.Execute(`insert into vec_examples(sample_embedding) values (?)`, FloatsToBytes([]float32{-0.710, 0.330, 0.656, 0.041, -0.990, 0.726, 0.385, -0.958})); err != nil {
						t.Fatalf("failed to insert a vector: %s", err)
					} else {
						rowsAffected, _ := res.RowsAffected()
						if rowsAffected != 1 {
							t.Fatalf("expected 1 row affected, got %d", rowsAffected)
						}
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
								log.Printf("rowid: %d, distance: %f", rowid, distance)
							}
						}
					}

					// TODO: test more things

					// drop table
					if _, err := client.Execute("drop table vec_examples"); err != nil {
						t.Fatalf("failed to drop table: %s", err)
					}
				}
			}

			if err := client.Close(); err != nil {
				t.Fatalf("failed to close client: %s", err)
			}
		}
	}
}
