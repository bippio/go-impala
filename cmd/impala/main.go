package main

import (
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

	flag.Parse()

	if opts.UseLDAP {
		if opts.Username == "" {
			log.Fatalf("Please specify username with --username flag")
		}
		if opts.Password == "" {
			log.Fatalf("Please specify password with --password flag")
		}
	}

	q := flag.Arg(0)

	con, err := impalathing.Connect(host, port, &opts)

	if err != nil {
		log.Fatalf("Error connecting: %v", err)
	}

	query, err := con.Query(q)

	if err != nil {
		log.Fatal(err)
	}

	startTime := time.Now()
	results := query.FetchAll()
	fmt.Printf("\nFetch %d rows(s) in %.2fs\n", len(results), time.Duration(time.Since(startTime)).Seconds())

	var columns []string
	for _, col := range query.Schema() {
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
