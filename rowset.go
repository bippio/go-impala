package impalathing

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	impala "github.com/koblas/impalathing/services/ImpalaService"
	"github.com/koblas/impalathing/services/beeswax"
)

type rowSet struct {
	client  *impala.ImpalaServiceClient
	handle  *beeswax.QueryHandle
	options Options

	// columns    []*tcliservice.TColumnDesc
	columnNames []string

	offset  int
	rowSet  *beeswax.Results
	hasMore bool
	ready   bool

	nextRow []string
}

// A RowSet represents an asyncronous hive operation. You can
// Reattach to a previously submitted hive operation if you
// have a valid thrift client, and the serialized Handle()
// from the prior operation.
type RowSet interface {
	Columns() []string
	Next() bool
	Scan(dest ...interface{}) error
	Poll() (*Status, error)
	Wait() (*Status, error)
}

// Represents job status, including success state and time the
// status was updated.
type Status struct {
	state beeswax.QueryState
	Error error
}

func newRowSet(client *impala.ImpalaServiceClient, handle *beeswax.QueryHandle, options Options) RowSet {
	return &rowSet{client, handle, options, nil, 0, nil, true, false, nil}
}

//
//
//
func (s *Status) IsSuccess() bool {
	return s.state != beeswax.QueryState_EXCEPTION
}

func (s *Status) IsComplete() bool {
	return s.state == beeswax.QueryState_FINISHED
}

// Issue a thrift call to check for the job's current status.
func (r *rowSet) Poll() (*Status, error) {
	state, err := r.client.GetState(r.handle)

	if err != nil {
		return nil, fmt.Errorf("Error getting status: %v", err)
	}

	if state == beeswax.QueryState_EXCEPTION {
		return nil, fmt.Errorf("Exception on Impala side")
	}

	return &Status{state, nil}, nil
}

// Wait until the job is complete, one way or another, returning Status and error.
func (r *rowSet) Wait() (*Status, error) {
	for {
		status, err := r.Poll()

		if err != nil {
			return nil, err
		}

		if status.IsComplete() {
			if status.IsSuccess() {
				r.ready = true
				return status, nil
			}
			return nil, fmt.Errorf("Query failed execution: %s", status.state.String())
		}

		time.Sleep(time.Duration(r.options.PollIntervalSeconds) * time.Second)
	}
}

func (r *rowSet) waitForSuccess() error {
	if !r.ready {
		status, err := r.Wait()
		if err != nil {
			return err
		}
		if !status.IsSuccess() || !r.ready {
			return fmt.Errorf("Unsuccessful query execution: %+v", status)
		}
	}

	return nil
}

// Prepares a row for scanning into memory, by reading data from hive if
// the operation is successful, blocking until the operation is
// complete, if necessary.
// Returns true is a row is available to Scan(), and false if the
// results are empty or any other error occurs.
func (r *rowSet) Next() bool {
	if err := r.waitForSuccess(); err != nil {
		return false
	}

	if r.rowSet == nil || r.offset >= len(r.rowSet.Data) {
		if !r.hasMore {
			return false
		}

		resp, err := r.client.Fetch(r.handle, false, 1000000)
		if err != nil {
			log.Printf("FetchResults failed: %v\n", err)
			return false
		}

		if len(r.columnNames) == 0 {
			r.columnNames = resp.Columns
		}

		r.hasMore = resp.HasMore

		r.rowSet = resp
		r.offset = 0

		// We assume that if we get 0 results back, that's the end
		if len(resp.Data) == 0 {
			return false
		}
	}

	r.nextRow = strings.Split(r.rowSet.Data[r.offset], "\t")
	r.offset++

	return true
}

// Scan the last row prepared via Next() into the destination(s) provided,
// which must be pointers to value types, as in database.sql. Further,
// only pointers of the following types are supported:
//  - int, int16, int32, int64
//  - string, []byte
//  - float64
//   - bool
func (r *rowSet) Scan(dest ...interface{}) error {
	// TODO: Add type checking and conversion between compatible
	// types where possible, as well as some common error checking,
	// like passing nil. database/sql's method is very convenient,
	// for example: http://golang.org/src/pkg/database/sql/convert.go, like 85
	if r.nextRow == nil {
		return errors.New("No row to scan! Did you call Next() first?")
	}

	if len(dest) != len(r.nextRow) {
		return fmt.Errorf("Can't scan into %d arguments with input of length %d", len(dest), len(r.nextRow))
	}

	for i, val := range r.nextRow {
		d := dest[i]
		switch dt := d.(type) {
		case *string:
			*dt = val
		case *int:
			i, _ := strconv.ParseInt(val, 10, 0)
			*dt = int(i)
		case *int64:
			i, _ := strconv.ParseInt(val, 10, 0)
			*dt = int64(i)
		case *int32:
			i, _ := strconv.ParseInt(val, 10, 0)
			*dt = int32(i)
		case *int16:
			i, _ := strconv.ParseInt(val, 10, 0)
			*dt = int16(i)
		case *float64:
			*dt, _ = strconv.ParseFloat(val, 64)
			/*
			   case *[]byte:
			       *dt = []byte(val.(string))
			   case *bool:
			       *dt = val.(bool)
			*/
		default:
			return fmt.Errorf("Can't scan value of type %T with value %v", dt, val)
		}
	}

	return nil
}

// Returns the names of the columns for the given operation,
// blocking if necessary until the information is available.
func (r *rowSet) Columns() []string {
	if r.columnNames == nil {
		if err := r.waitForSuccess(); err != nil {
			return nil
		}
	}

	return r.columnNames
}
