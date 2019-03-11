package main

import (
	"bufio"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"syscall"
	"time"

	impala "github.com/bippio/go-impala"
)

func main() {

	var timeout int
	var verbose bool
	opts := impala.DefaultOptions
	flag.StringVar(&opts.Host, "host", "", "impalad hostname")
	flag.StringVar(&opts.Port, "p", "21050", "impala daemon port")
	flag.BoolVar(&opts.UseLDAP, "l", false, "use ldap authentication")
	flag.StringVar(&opts.Username, "username", "", "ldap usename")
	flag.StringVar(&opts.Password, "password", "", "ldap password")
	flag.BoolVar(&opts.UseTLS, "tls", false, "use tls")
	flag.StringVar(&opts.CACertPath, "ca-cert", "", "ca certificate path")
	flag.IntVar(&opts.BatchSize, "batch-size", 1024, "fetch batch size")
	flag.StringVar(&opts.MemoryLimit, "mem-limit", "0", "memory limit")
	flag.IntVar(&timeout, "timeout", 0, "timeout in ms; set 0 to disable timeout")
	flag.BoolVar(&verbose, "v", false, "verbose")
	flag.Parse()

	if opts.UseLDAP {
		if opts.Username == "" {
			log.Fatalf("Please specify username with --username flag")
		}
		if opts.Password == "" {
			log.Fatalf("Please specify password with --password flag")
		}
	}

	if opts.UseTLS {
		if opts.CACertPath == "" {
			log.Fatalf("Please specify ca certificate path with --ca-cert flag")
		}
	}

	if verbose {
		opts.LogOut = os.Stderr
	}

	connector := impala.NewConnector(&opts)

	db := sql.OpenDB(connector)
	defer db.Close()

	appctx, cancel := context.WithCancel(context.Background())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	go func() {
		select {
		case <-sig:
			cancel()
		}
	}()

	if err := db.PingContext(appctx); err != nil {
		log.Fatal(err)
	}

	var q string

	stdinstat, err := os.Stdin.Stat()
	if err != nil {
		log.Fatal(err)
	}

	if stdinstat.Mode()&os.ModeNamedPipe != 0 {
		bytes, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			log.Fatal(err)
		}
		q = string(bytes)
	} else if len(flag.Args()) == 1 {
		q = flag.Arg(0)
	} else {
		fmt.Print("> ")
		reader := bufio.NewReader(os.Stdin)
		line, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}
		q = line
	}

	if timeout != 0 {
		ctx, release := context.WithTimeout(appctx, time.Duration(timeout*int(time.Millisecond)))
		defer release()

		appctx = ctx
	}

	query(appctx, db, q)
	//exec(appctx, db, q)
}

func query(ctx context.Context, db *sql.DB, query string) {
	startTime := time.Now()

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Fatal(err)
	}

	coltypes, err := rows.ColumnTypes()
	if err != nil {
		log.Fatal(err)
	}

	in := make([]reflect.Value, len(coltypes))
	for i, coltype := range coltypes {
		in[i] = reflect.New(coltype.ScanType())
	}

	once := new(sync.Once)
	var results int
	scanner := reflect.ValueOf(rows).MethodByName("Scan")
	for rows.Next() {
		errv := scanner.Call(in)
		if !errv[0].IsNil() {
			errtext := errv[0].MethodByName("Error").Call(nil)[0].String()
			log.Fatal(errtext)
		}

		once.Do(func() {
			for _, coltype := range coltypes {
				fmt.Printf("%s\t", coltype.Name())
			}
			fmt.Print("\n")
		})

		for _, col := range in {
			fmt.Printf("%v\t", col.Elem())
		}
		fmt.Print("\n")
		results++
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Fetch %d rows(s) in %.2fs\n", results, time.Duration(time.Since(startTime)).Seconds())
}

func exec(ctx context.Context, db *sql.DB, query string) {
	res, err := db.ExecContext(ctx, query)
	if err != nil {
		log.Fatal(err)
	}

	log.Print(res)
	fmt.Print("The operation has no results.\n")
}
