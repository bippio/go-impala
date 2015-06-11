package impalathing

import (
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	impala "github.com/koblas/impalathing/services/impalaservice"
	"github.com/koblas/impalathing/services/beeswax"
)

type Options struct {
	PollIntervalSeconds float64
	BatchSize           int64
}

var (
	DefaultOptions = Options{PollIntervalSeconds: 0.1, BatchSize: 10000}
)

type Connection struct {
	client  *impala.ImpalaServiceClient
	handle  *beeswax.QueryHandle
    transport thrift.TTransport
	options Options
}

func Connect(host string, port int, options Options) (*Connection, error) {
	socket, err := thrift.NewTSocket(fmt.Sprintf("%s:%d", host, port))

	if err != nil {
		return nil, err
	}

	transportFactory := thrift.NewTBufferedTransportFactory(24 * 1024 * 1024)
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	transport := transportFactory.GetTransport(socket)

	if err := transport.Open(); err != nil {
		return nil, err
	}

	client := impala.NewImpalaServiceClientFactory(transport, protocolFactory)

	return &Connection{client, nil, transport, options}, nil
}

func (c *Connection) isOpen() bool {
	return c.client != nil
}

func (c *Connection) Close() error {
	if c.isOpen() {
		if c.handle != nil {
			_, err := c.client.Cancel(c.handle)
			if err != nil {
				return err
			}
			c.handle = nil
		}

		c.transport.Close()
        c.client = nil
	}
	return nil
}

func (c *Connection) Query(query string) (RowSet, error) {
	bquery := beeswax.Query{}

	bquery.Query = query
	bquery.Configuration = []string{}

	handle, err := c.client.Query(&bquery)

	if err != nil {
		return nil, err
	}

	return newRowSet(c.client, handle, c.options), nil
}
