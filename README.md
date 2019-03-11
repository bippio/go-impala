# Golang Apache Impala Driver 

**Apache Impala driver for Go's [database/sql](https://golang.org/pkg/database/sql) package**

As far as we know, this is the only pure golang driver for Apache Impala that has TLS and LDAP support. 

The current implementation of the driver is based on the Hive Server 2 protocol. 

**The legacy Beeswax protocol based driver is available at [go-impala v1.0.0](https://github.com/bippio/go-impala/tree/v1.0.0), which is marked deprecated and will no longer be maintained.**

*If you are using Go 1.12 or later, you can get the v1.0.0 of the driver with ***go get github.com/bippio/go-impala@v1.0.0*** or use a dependency management tool such as [dep](https://golang.github.io/dep/])*

We at [Bipp](http://www.bipp.io), want to make large scale data analytics accesible to every business user.

As part of that, we are commited to making this is a production grade driver that be used in serious enterprise scenarios in lieu of the ODBC/JDBC drivers.

Issues and contributions are welcome. 


## Install

go get github.com/bippio/go-impala


## Connection Parameters and DSN

The connection string uses a URL format: impala://username:password@host:port?param1=value&param2=value

### Parameters:

* `auth` - string. Authentication mode. Supported values: "noauth", "ldap"
* `tls` - boolean. Enable TLS
* `ca-cert` - The file that contains the public key certificate of the CA that signed the impala certificate
* `batch-size` - integer value (default: 1024). Maximum number of rows fetched per request
* `buffer-size`- in bytes (default: 4096); Buffer size for the Thrift transport 

A string of this format can be constructed using the URL type in the net/url package.

```go
  query := url.Values{}
  query.Add("auth", "ldap")

  u := &url.URL{
      Scheme:   "impala",
      User:     url.UserPassword(username, password),
      Host:     net.JoinHostPort(hostname, port),
      RawQuery: query.Encode(),
  }
  db, err := sql.Open("impala", u.String())
```

Also, you can bypass string-base data source name by using sql.OpenDB:

```go
  opts := impala.DefaultOptions
  opts.Host = hostname
  opts.UseLDAP = true
  opts.Username = username
  opts.Password = password

  connector := impala.NewConnector(&opts)
  db := sql.OpenDB(connector)
```


## Example

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
