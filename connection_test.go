package impala

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestPinger(t *testing.T) {
	if testing.Short() {
		t.Skip("skip long tests")
	}

	conn := open(t)
	defer conn.Close()
	err := conn.Ping()
	if err != nil {
		t.Errorf("Failed to hit database")
	}
}

func TestSelect(t *testing.T) {
	if testing.Short() {
		t.Skip("skip long tests")
	}

	sampletime, _ := time.Parse(time.RFC3339, "2019-01-01T12:00:00Z")

	tests := []struct {
		sql string
		res interface{}
	}{
		{sql: "1", res: int8(1)},
		{sql: "cast(1 as smallint)", res: int16(1)},
		{sql: "cast(1 as int)", res: int32(1)},
		{sql: "cast(1 as bigint)", res: int64(1)},
		{sql: "cast(1.0 as float)", res: float64(1)},
		{sql: "cast(1.0 as double)", res: float64(1)},
		{sql: "cast(1.0 as real)", res: float64(1)},
		{sql: "'str'", res: "str"},
		{sql: "cast('str' as char(10))", res: "str       "},
		{sql: "cast('str' as varchar(100))", res: "str"},
		{sql: "cast('2019-01-01 12:00:00' as timestamp)", res: sampletime},
	}

	var res interface{}
	db := open(t)
	defer db.Close()

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			err := db.QueryRow(fmt.Sprintf("select %s", tt.sql)).Scan(&res)
			if err != nil {
				t.Error(err)
			}

			if res != tt.res {
				t.Errorf("got: %v (%T), want: %v (%T)", res, res, tt.res, tt.res)
			}
		})
	}
}

func open(t *testing.T) *sql.DB {
	dsn := os.Getenv("IMPALA_DSN")
	if dsn == "" {
		t.Skip("No connection string")
	}
	db, err := sql.Open("impala", dsn)
	if err != nil {
		t.Fatalf("Could not connect to %s: %s", dsn, err)
	}
	return db
}
