// Code generated by Thrift Compiler (0.19.0). DO NOT EDIT.

package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	thrift "github.com/apache/thrift/lib/go/thrift"
	"github.com/bippio/go-impala/services/hive_metastore"
	"github.com/bippio/go-impala/services/beeswax"
)

var _ = hive_metastore.GoUnusedProtection__
var _ = beeswax.GoUnusedProtection__

func Usage() {
  fmt.Fprintln(os.Stderr, "Usage of ", os.Args[0], " [-h host:port] [-u url] [-f[ramed]] function [arg1 [arg2...]]:")
  flag.PrintDefaults()
  fmt.Fprintln(os.Stderr, "\nFunctions:")
  fmt.Fprintln(os.Stderr, "  QueryHandle query(Query query)")
  fmt.Fprintln(os.Stderr, "  QueryHandle executeAndWait(Query query, LogContextId clientCtx)")
  fmt.Fprintln(os.Stderr, "  QueryExplanation explain(Query query)")
  fmt.Fprintln(os.Stderr, "  Results fetch(QueryHandle query_id, bool start_over, i32 fetch_size)")
  fmt.Fprintln(os.Stderr, "  QueryState get_state(QueryHandle handle)")
  fmt.Fprintln(os.Stderr, "  ResultsMetadata get_results_metadata(QueryHandle handle)")
  fmt.Fprintln(os.Stderr, "  string echo(string s)")
  fmt.Fprintln(os.Stderr, "  string dump_config()")
  fmt.Fprintln(os.Stderr, "  string get_log(LogContextId context)")
  fmt.Fprintln(os.Stderr, "   get_default_configuration(bool include_hadoop)")
  fmt.Fprintln(os.Stderr, "  void close(QueryHandle handle)")
  fmt.Fprintln(os.Stderr, "  void clean(LogContextId log_context)")
  fmt.Fprintln(os.Stderr)
  os.Exit(0)
}

type httpHeaders map[string]string

func (h httpHeaders) String() string {
  var m map[string]string = h
  return fmt.Sprintf("%s", m)
}

func (h httpHeaders) Set(value string) error {
  parts := strings.Split(value, ": ")
  if len(parts) != 2 {
    return fmt.Errorf("header should be of format 'Key: Value'")
  }
  h[parts[0]] = parts[1]
  return nil
}

func main() {
  flag.Usage = Usage
  var host string
  var port int
  var protocol string
  var urlString string
  var framed bool
  var useHttp bool
  headers := make(httpHeaders)
  var parsedUrl *url.URL
  var trans thrift.TTransport
  _ = strconv.Atoi
  _ = math.Abs
  flag.Usage = Usage
  flag.StringVar(&host, "h", "localhost", "Specify host and port")
  flag.IntVar(&port, "p", 9090, "Specify port")
  flag.StringVar(&protocol, "P", "binary", "Specify the protocol (binary, compact, simplejson, json)")
  flag.StringVar(&urlString, "u", "", "Specify the url")
  flag.BoolVar(&framed, "framed", false, "Use framed transport")
  flag.BoolVar(&useHttp, "http", false, "Use http")
  flag.Var(headers, "H", "Headers to set on the http(s) request (e.g. -H \"Key: Value\")")
  flag.Parse()
  
  if len(urlString) > 0 {
    var err error
    parsedUrl, err = url.Parse(urlString)
    if err != nil {
      fmt.Fprintln(os.Stderr, "Error parsing URL: ", err)
      flag.Usage()
    }
    host = parsedUrl.Host
    useHttp = len(parsedUrl.Scheme) <= 0 || parsedUrl.Scheme == "http" || parsedUrl.Scheme == "https"
  } else if useHttp {
    _, err := url.Parse(fmt.Sprint("http://", host, ":", port))
    if err != nil {
      fmt.Fprintln(os.Stderr, "Error parsing URL: ", err)
      flag.Usage()
    }
  }
  
  cmd := flag.Arg(0)
  var err error
  var cfg *thrift.TConfiguration = nil
  if useHttp {
    trans, err = thrift.NewTHttpClient(parsedUrl.String())
    if len(headers) > 0 {
      httptrans := trans.(*thrift.THttpClient)
      for key, value := range headers {
        httptrans.SetHeader(key, value)
      }
    }
  } else {
    portStr := fmt.Sprint(port)
    if strings.Contains(host, ":") {
           host, portStr, err = net.SplitHostPort(host)
           if err != nil {
                   fmt.Fprintln(os.Stderr, "error with host:", err)
                   os.Exit(1)
           }
    }
    trans = thrift.NewTSocketConf(net.JoinHostPort(host, portStr), cfg)
    if err != nil {
      fmt.Fprintln(os.Stderr, "error resolving address:", err)
      os.Exit(1)
    }
    if framed {
      trans = thrift.NewTFramedTransportConf(trans, cfg)
    }
  }
  if err != nil {
    fmt.Fprintln(os.Stderr, "Error creating transport", err)
    os.Exit(1)
  }
  defer trans.Close()
  var protocolFactory thrift.TProtocolFactory
  switch protocol {
  case "compact":
    protocolFactory = thrift.NewTCompactProtocolFactoryConf(cfg)
    break
  case "simplejson":
    protocolFactory = thrift.NewTSimpleJSONProtocolFactoryConf(cfg)
    break
  case "json":
    protocolFactory = thrift.NewTJSONProtocolFactory()
    break
  case "binary", "":
    protocolFactory = thrift.NewTBinaryProtocolFactoryConf(cfg)
    break
  default:
    fmt.Fprintln(os.Stderr, "Invalid protocol specified: ", protocol)
    Usage()
    os.Exit(1)
  }
  iprot := protocolFactory.GetProtocol(trans)
  oprot := protocolFactory.GetProtocol(trans)
  client := beeswax.NewBeeswaxServiceClient(thrift.NewTStandardClient(iprot, oprot))
  if err := trans.Open(); err != nil {
    fmt.Fprintln(os.Stderr, "Error opening socket to ", host, ":", port, " ", err)
    os.Exit(1)
  }
  
  switch cmd {
  case "query":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Query requires 1 args")
      flag.Usage()
    }
    arg74 := flag.Arg(1)
    mbTrans75 := thrift.NewTMemoryBufferLen(len(arg74))
    defer mbTrans75.Close()
    _, err76 := mbTrans75.WriteString(arg74)
    if err76 != nil {
      Usage()
      return
    }
    factory77 := thrift.NewTJSONProtocolFactory()
    jsProt78 := factory77.GetProtocol(mbTrans75)
    argvalue0 := beeswax.NewQuery()
    err79 := argvalue0.Read(context.Background(), jsProt78)
    if err79 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.Query(context.Background(), value0))
    fmt.Print("\n")
    break
  case "executeAndWait":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "ExecuteAndWait requires 2 args")
      flag.Usage()
    }
    arg80 := flag.Arg(1)
    mbTrans81 := thrift.NewTMemoryBufferLen(len(arg80))
    defer mbTrans81.Close()
    _, err82 := mbTrans81.WriteString(arg80)
    if err82 != nil {
      Usage()
      return
    }
    factory83 := thrift.NewTJSONProtocolFactory()
    jsProt84 := factory83.GetProtocol(mbTrans81)
    argvalue0 := beeswax.NewQuery()
    err85 := argvalue0.Read(context.Background(), jsProt84)
    if err85 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := beeswax.LogContextId(argvalue1)
    fmt.Print(client.ExecuteAndWait(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "explain":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Explain requires 1 args")
      flag.Usage()
    }
    arg87 := flag.Arg(1)
    mbTrans88 := thrift.NewTMemoryBufferLen(len(arg87))
    defer mbTrans88.Close()
    _, err89 := mbTrans88.WriteString(arg87)
    if err89 != nil {
      Usage()
      return
    }
    factory90 := thrift.NewTJSONProtocolFactory()
    jsProt91 := factory90.GetProtocol(mbTrans88)
    argvalue0 := beeswax.NewQuery()
    err92 := argvalue0.Read(context.Background(), jsProt91)
    if err92 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.Explain(context.Background(), value0))
    fmt.Print("\n")
    break
  case "fetch":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "Fetch requires 3 args")
      flag.Usage()
    }
    arg93 := flag.Arg(1)
    mbTrans94 := thrift.NewTMemoryBufferLen(len(arg93))
    defer mbTrans94.Close()
    _, err95 := mbTrans94.WriteString(arg93)
    if err95 != nil {
      Usage()
      return
    }
    factory96 := thrift.NewTJSONProtocolFactory()
    jsProt97 := factory96.GetProtocol(mbTrans94)
    argvalue0 := beeswax.NewQueryHandle()
    err98 := argvalue0.Read(context.Background(), jsProt97)
    if err98 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2) == "true"
    value1 := argvalue1
    tmp2, err100 := (strconv.Atoi(flag.Arg(3)))
    if err100 != nil {
      Usage()
      return
    }
    argvalue2 := int32(tmp2)
    value2 := argvalue2
    fmt.Print(client.Fetch(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "get_state":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "GetState requires 1 args")
      flag.Usage()
    }
    arg101 := flag.Arg(1)
    mbTrans102 := thrift.NewTMemoryBufferLen(len(arg101))
    defer mbTrans102.Close()
    _, err103 := mbTrans102.WriteString(arg101)
    if err103 != nil {
      Usage()
      return
    }
    factory104 := thrift.NewTJSONProtocolFactory()
    jsProt105 := factory104.GetProtocol(mbTrans102)
    argvalue0 := beeswax.NewQueryHandle()
    err106 := argvalue0.Read(context.Background(), jsProt105)
    if err106 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.GetState(context.Background(), value0))
    fmt.Print("\n")
    break
  case "get_results_metadata":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "GetResultsMetadata requires 1 args")
      flag.Usage()
    }
    arg107 := flag.Arg(1)
    mbTrans108 := thrift.NewTMemoryBufferLen(len(arg107))
    defer mbTrans108.Close()
    _, err109 := mbTrans108.WriteString(arg107)
    if err109 != nil {
      Usage()
      return
    }
    factory110 := thrift.NewTJSONProtocolFactory()
    jsProt111 := factory110.GetProtocol(mbTrans108)
    argvalue0 := beeswax.NewQueryHandle()
    err112 := argvalue0.Read(context.Background(), jsProt111)
    if err112 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.GetResultsMetadata(context.Background(), value0))
    fmt.Print("\n")
    break
  case "echo":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Echo requires 1 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    fmt.Print(client.Echo(context.Background(), value0))
    fmt.Print("\n")
    break
  case "dump_config":
    if flag.NArg() - 1 != 0 {
      fmt.Fprintln(os.Stderr, "DumpConfig requires 0 args")
      flag.Usage()
    }
    fmt.Print(client.DumpConfig(context.Background()))
    fmt.Print("\n")
    break
  case "get_log":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "GetLog requires 1 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := beeswax.LogContextId(argvalue0)
    fmt.Print(client.GetLog(context.Background(), value0))
    fmt.Print("\n")
    break
  case "get_default_configuration":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "GetDefaultConfiguration requires 1 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1) == "true"
    value0 := argvalue0
    fmt.Print(client.GetDefaultConfiguration(context.Background(), value0))
    fmt.Print("\n")
    break
  case "close":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Close requires 1 args")
      flag.Usage()
    }
    arg116 := flag.Arg(1)
    mbTrans117 := thrift.NewTMemoryBufferLen(len(arg116))
    defer mbTrans117.Close()
    _, err118 := mbTrans117.WriteString(arg116)
    if err118 != nil {
      Usage()
      return
    }
    factory119 := thrift.NewTJSONProtocolFactory()
    jsProt120 := factory119.GetProtocol(mbTrans117)
    argvalue0 := beeswax.NewQueryHandle()
    err121 := argvalue0.Read(context.Background(), jsProt120)
    if err121 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.Close(context.Background(), value0))
    fmt.Print("\n")
    break
  case "clean":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Clean requires 1 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := beeswax.LogContextId(argvalue0)
    fmt.Print(client.Clean(context.Background(), value0))
    fmt.Print("\n")
    break
  case "":
    Usage()
    break
  default:
    fmt.Fprintln(os.Stderr, "Invalid function ", cmd)
  }
}
