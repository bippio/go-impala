package impalathing

import (
	"crypto/tls"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/bippio/impalathing/services/beeswax"
	"github.com/bippio/impalathing/services/cli_service"
	impala "github.com/bippio/impalathing/services/impalaservice"
	"github.com/freddierice/go-sasl"
	"net"
)

type Options struct {
	PollIntervalSeconds float64
	BatchSize           int64
}

var (
	DefaultOptions = Options{PollIntervalSeconds: 0.1, BatchSize: 10000}
)

type Connection struct {
	client    *impala.ImpalaServiceClient
	handle    *beeswax.QueryHandle
	transport thrift.TTransport
	options   Options
}

func Connect(host string, port int, options Options) (*Connection, error) {
	socket, err := thrift.NewTSSLSocket(net.JoinHostPort(host, fmt.Sprintf("%d", port)), &tls.Config{InsecureSkipVerify: true})
	//socket, err := thrift.NewTSocket(fmt.Sprintf("%s:%d", host, port))

	if err != nil {
		return nil, err
	}

	transportFactory := thrift.NewTBufferedTransportFactory(24 * 1024 * 1024)
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	transport := transportFactory.GetTransport(socket)

	if err := transport.Open(); err != nil {
		return nil, err
	}

	s := cli_service.NewTCLIServiceClientFactory(transport, protocolFactory)
	rr := cli_service.NewTOpenSessionReq()
	p := "cloudera123"
	rr.Username = &p
	rr.Password = &p

	rrResp, err := s.OpenSession(rr)
	fmt.Println(err)


	gi := cli_service.NewTGetInfoReq()
	gi.SessionHandle = rrResp.GetSessionHandle()
	gi.InfoType = cli_service.TGetInfoType_CLI_SERVER_NAME
	r, _ := s.GetInfo(gi)
	fmt.Println("Server name:", r.GetInfoValue().GetStringValue())

	ex := cli_service.NewTExecuteStatementReq()

	ex.Statement = "SELECT true;"
	ex.ConfOverlay = map[string]string{}
	ex.SessionHandle = rrResp.GetSessionHandle()
	exResp, err := s.ExecuteStatement(ex)
	fmt.Println("exResp:", exResp)
	fmt.Println("errr", err)

	rrr := cli_service.NewTFetchResultsReq()
	rrr.OperationHandle = exResp.GetOperationHandle()
	resres, err := s.FetchResults(rrr)
	fmt.Println(resres)
	fmt.Println(resres.GetResults().Rows[0].GetColVals()[0].GetBoolVal())
	fmt.Println(err)

	client := impala.NewImpalaServiceClientFactory(transport, protocolFactory)

	return &Connection{client, nil, transport, options}, nil
}

func ConnectSasl(host string, port int, options Options) (*Connection, error) {
	socket, err := thrift.NewTSSLSocket(net.JoinHostPort(host, fmt.Sprintf("%d", port)), &tls.Config{InsecureSkipVerify: true})
	//socket, err := thrift.NewTSocket(net.JoinHostPort(host, fmt.Sprintf("%d", port)))

	if err != nil {
		return nil, err
	}

	if socket == nil {
		panic("Socket is nil")
	}

	transportFactory := thrift.NewTBufferedTransportFactory(100000)
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	transport := transportFactory.GetTransport(socket)
	if transport == nil {
		panic("Could not find transport!")
	}

	saslConfig := sasl.Config{
		Username: "test_user",
		Password: "password",
		Authname: "test_user",
		Realm:    "testimpala",
	}

	saslClient, err := sasl.NewClient("hive", fmt.Sprintf("%s:%d", host, port), &saslConfig)

	transport, err = NewTSaslTransport(transport, saslClient, []string{"PLAIN", "ANONYMOUS", "GS2-KRB5", "GSSAPI", "GSSAPI"})
	if err != nil {
		return nil, err
	}

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
