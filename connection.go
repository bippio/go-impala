package impalathing

import (
	"errors"
	"fmt"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/bippio/go-impala/sasl"
	"github.com/bippio/go-impala/services/beeswax"
	impala "github.com/bippio/go-impala/services/impalaservice"
)

type Options struct {
	UseLDAP             bool
	Username            string
	Password            string
	PollIntervalSeconds float64
	BatchSize           int64
	BufferSize          int
}

var (
	DefaultOptions = Options{PollIntervalSeconds: 0.1, BatchSize: 10000, BufferSize: 4096}
)

type Connection struct {
	client    *impala.ImpalaServiceClient
	handle    *beeswax.QueryHandle
	transport thrift.TTransport
	options   *Options
}

func Connect(host string, port int, options *Options) (*Connection, error) {
	socket, err := thrift.NewTSocket(fmt.Sprintf("%s:%d", host, port))

	if err != nil {
		return nil, err
	}

	var transport thrift.TTransport
	if options.UseLDAP {

		if options.Username == "" {
			return nil, errors.New("Please provide username for LDAP auth")
		}

		if options.Password == "" {
			return nil, errors.New("Please provide password for LDAP auth")
		}

		transport, err = sasl.NewTSaslTransport(socket, &sasl.Options{
			Host:     host,
			Username: options.Username,
			Password: options.Password,
		})

		if err != nil {
			return nil, err
		}
	} else {
		transport = thrift.NewTBufferedTransport(socket, options.BufferSize)
	}

	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

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
