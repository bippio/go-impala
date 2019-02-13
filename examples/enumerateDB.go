package main

// Simple program to list databases and the tables

import (
	"context"
	"fmt"
	"log"

	impala "github.com/bippio/go-impala"
)

func main() {
	host := "<impala host>"
	port := 21000

	opts := impala.DefaultOptions

	// enable LDAP authentication:
	opts.UseLDAP = true
	opts.Username = "<ldap username>"
	opts.Password = "<ldap password>"

	// enable TLS
	opts.UseTLS = true
	opts.CACertPath = "/path/to/cacert"

	con, err := impala.Connect(host, port, &opts)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	var rows impala.RowSet

	// get all databases for the connection object
	query := fmt.Sprintf("SHOW DATABASES")
	rows, err = con.Query(ctx, query)
	if err != nil {
		log.Fatal("error in retrieving databases: ", err)
	}

	databases := make([]string, 0) // databases will contain all the DBs to enumerate later
	for rows.Next(ctx) {
		row := make(map[string]interface{})
		err = rows.MapScan(row)
		if err != nil {
			log.Println(err)
			continue
		}
		if db, ok := row["name"].(string); ok {
			databases = append(databases, db)
		}
	}
	log.Println("List of Databases", databases)

	for _, d := range databases {
		q := "SHOW TABLES IN " + d

		rows, err = con.Query(ctx, q)
		if err != nil {
			log.Printf("error in querying database %s: %s", d, err.Error())
			continue
		}

		tables := make([]string, 0) // databases will contain all the DBs to enumerate later
		for rows.Next(ctx) {
			row := make(map[string]interface{})
			err = rows.MapScan(row)
			if err != nil {
				log.Println(err)
				continue
			}
			if tab, ok := row["name"].(string); ok {
				tables = append(tables, tab)
			}
		}
		log.Printf("List of Tables in Database %s: %v\n", d, tables)
	}
}
