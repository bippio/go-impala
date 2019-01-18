package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	impalathing "github.com/bippio/go-impala"
)

func main() {

	var host string
	flag.StringVar(&host, "host", "", "impalad hostname")

	var port int
	flag.IntVar(&port, "p", 21000, "impala daemon port")

	opts := impalathing.DefaultOptions
	flag.BoolVar(&opts.UseLDAP, "l", false, "use ldap authentication")
	flag.StringVar(&opts.Username, "username", "", "ldap usename")
	flag.StringVar(&opts.Password, "password", "", "ldap password")
	flag.BoolVar(&opts.UseTLS, "tls", false, "use tls")
	flag.StringVar(&opts.CACertPath, "ca-cert", "", "ca certificate path")

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

	q := flag.Arg(0)

	con, err := impalathing.Connect(host, port, &opts)

	if err != nil {
		log.Fatalf("Error connecting: %v", err)
	}

	ctx := context.Background()

	query, err := con.Query(ctx, q)
	if err != nil {
		log.Fatal(err)
	}

	startTime := time.Now()
	results := query.FetchAll(ctx)
	fmt.Printf("\nFetch %d rows(s) in %.2fs\n", len(results), time.Duration(time.Since(startTime)).Seconds())

	var columns []string
	for _, col := range query.Schema(ctx) {
		columns = append(columns, col.Name)

		fmt.Printf("%25s |", fmt.Sprintf("%s (%s)", col.Name, col.Type))
	}
	fmt.Println()
	fmt.Println("-----------------------------------------------------")

	for _, row := range results {
		for _, col := range columns {
			fmt.Printf("%25v |", row[col])
		}
		fmt.Println()
	}

	log.Printf("Fetch %d rows(s) in %.2fs", len(results), time.Duration(time.Since(startTime)).Seconds())
}
