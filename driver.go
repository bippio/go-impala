package impala

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql/driver"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/bippio/go-impala/hive"
	"github.com/bippio/go-impala/sasl"
)

var (
	// ErrNotSupported means this operation is not supported by impala driver
	ErrNotSupported = errors.New("impala: not supported")
)

// Driver to impala
type Driver struct{}

// Open creates new connection to impala
func (d *Driver) Open(uri string) (driver.Conn, error) {
	opts, err := parseURI(uri)
	if err != nil {
		return nil, err
	}

	log.Printf("opts: %v", opts)

	conn, err := connect(opts)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func parseURI(uri string) (*Options, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	if u.Scheme != "impala" {
		return nil, fmt.Errorf("scheme %s not recognized", u.Scheme)
	}

	opts := DefaultOptions

	if u.User != nil {
		opts.Username = u.User.Username()
		password, ok := u.User.Password()
		if ok {
			opts.Password = password
		}
	}

	if !strings.Contains(u.Host, ":") {
		u.Host = fmt.Sprintf("%s:%s", u.Host, DefaultOptions.Port)
	}

	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		return nil, err
	}

	opts.Host = host
	opts.Port = port

	query := u.Query()
	auth := query.Get("auth")
	if auth == "ldap" {
		opts.UseLDAP = true
	}

	tls, ok := query["tls"]
	if ok {
		v, err := strconv.ParseBool(tls[0])
		if err != nil {
			return nil, err
		}
		opts.UseTLS = v
	}

	caCert, ok := query["ca-cert"]
	if ok {
		opts.CACertPath = caCert[0]
	}

	batchSize, ok := query["batch-size"]
	if ok {
		size, err := strconv.Atoi(batchSize[0])
		if err != nil {
			return nil, err
		}
		opts.BatchSize = size
	}

	bufferSize, ok := query["buffer-size"]
	if ok {
		size, err := strconv.Atoi(bufferSize[0])
		if err != nil {
			return nil, err
		}
		opts.BufferSize = size
	}

	memLimit, ok := query["mem-limit"]
	if ok {
		opts.MemoryLimit = memLimit[0]
	}

	queryTimeout, ok := query["query-timeout"]
	if ok {
		qTimeout, err := strconv.Atoi(queryTimeout[0])
		if err != nil {
			return nil, err
		}
		opts.QueryTimeout = qTimeout
	}

	return &opts, nil
}

// OpenConnector parses name and return connector with fixed options
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {

	opts, err := parseURI(name)
	if err != nil {
		return nil, err
	}

	return &connector{opts: opts}, nil
}

type connector struct {
	d    *Driver
	opts *Options
}

// NewConnector creates connector with specified options
func NewConnector(opts *Options) driver.Connector {
	return &connector{opts: opts}
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	return connect(c.opts)
}

func (c *connector) Driver() driver.Driver {
	return c.d
}

func connect(opts *Options) (*Conn, error) {

	addr := net.JoinHostPort(opts.Host, opts.Port)

	var socket thrift.TTransport
	var err error
	if opts.UseTLS {

		if opts.CACertPath == "" {
			return nil, errors.New("Please provide CA certificate path")
		}

		caCert, err := ioutil.ReadFile(opts.CACertPath)
		if err != nil {
			return nil, err
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		socket, err = thrift.NewTSSLSocket(addr, &tls.Config{
			RootCAs: caCertPool,
		})
	} else {
		socket, err = thrift.NewTSocket(addr)
	}

	if err != nil {
		return nil, err
	}

	var transport thrift.TTransport
	if opts.UseLDAP {

		if opts.Username == "" {
			return nil, errors.New("Please provide username for LDAP auth")
		}

		if opts.Password == "" {
			return nil, errors.New("Please provide password for LDAP auth")
		}

		transport, err = sasl.NewTSaslTransport(socket, &sasl.Options{
			Host:     opts.Host,
			Username: opts.Username,
			Password: opts.Password,
		})

		if err != nil {
			return nil, err
		}
	} else {
		transport = thrift.NewTBufferedTransport(socket, opts.BufferSize)
	}

	protocol := thrift.NewTBinaryProtocol(transport, false, true)

	if err := transport.Open(); err != nil {
		return nil, err
	}

	logger := log.New(opts.LogOut, "impala: ", log.LstdFlags)

	tclient := thrift.NewTStandardClient(protocol, protocol)
	client := hive.NewClient(tclient, logger, &hive.Options{
		MaxRows:  int64(opts.BatchSize),
		MemLimit: opts.MemoryLimit,
		QueryTimeout: opts.QueryTimeout,
	})

	return &Conn{client: client, t: transport, log: logger}, nil
}
