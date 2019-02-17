go-impala is a Go driver for Apache Impala

As far as we know, this is the only pure golang driver for Apache Impala that has TLS and LDAP support.

We at Bipp, want to make large scale data analytics accesible to every business user.

As part of that, we are commited to making this is a production grade driver that be used in serious enterprise scenarios in lieu of the ODBC/JDBC drivers.

Issues and contributions are welcome. 

## Using
```go
package main

// Simple program to list databases and the tables

import (
	"context"
	"database/sql"
	"log"

	impala "github.com/bippio/go-impala"
)

func main() {

	opts := impala.DefaultOptions

	opts.Host = "<impala host>"
	opts.Port = "21050"

	// enable LDAP authentication:
	opts.UseLDAP = true
	opts.Username = "<ldap username>"
	opts.Password = "<ldap password>"

	// enable TLS
	opts.UseTLS = true
	opts.CACertPath = "/path/to/cacert"

	connector := impala.NewConnector(&opts)
	db := sql.OpenDB(connector)
	defer db.Close()

	ctx := context.Background()

	rows, err := db.QueryContext(ctx, "SHOW DATABASES")
	if err != nil {
		log.Fatal(err)
	}

	r := struct {
		name    string
		comment string
	}{}

	databases := make([]string, 0) // databases will contain all the DBs to enumerate later
	for rows.Next() {
		if err := rows.Scan(&r.name, &r.comment); err != nil {
			log.Fatal(err)
		}
		databases = append(databases, r.name)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	log.Println("List of Databases", databases)

	stmt, err := db.PrepareContext(ctx, "SHOW TABLES IN ?")
	if err != nil {
		log.Fatal(err)
	}

	tbl := struct {
		name string
	}{}

	for _, d := range databases {
		rows, err := stmt.QueryContext(ctx, d)
		if err != nil {
			log.Printf("error in querying database %s: %s", d, err.Error())
			continue
		}

		tables := make([]string, 0)
		for rows.Next() {
			if err := rows.Scan(&tbl.name); err != nil {
				log.Println(err)
				continue
			}
			tables = append(tables, tbl.name)
		}
		log.Printf("List of Tables in Database %s: %v\n", d, tables)
	}
}

```
Initial fork from [impalathing](https://github.com/koblas/impalathing)
