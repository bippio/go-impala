go-impala is a Go driver for Cloudera Impala

It's based on [impalathing](https://github.com/koblas/impalathing)

## Using
```go
package main

import (
    "log"
    "fmt"
    "time"
    impalathing "github.com/bippio/go-impala"
)

func main() {
    host := "<impala-host>"
    port := 21000

    opts := impalathing.DefaultOptions

    // enable LDAP authentication:
    opts.UseLDAP = true
    opts.Username = "<username>"
    opts.Password = "<password>"

    con, err := impalathing.Connect(host, port, opts)
    if err != nil {
        log.Fatal(err)
    }

    query, err := con.Query("<sql-query>")

    startTime := time.Now()
    results := query.query.FetchAll()
    log.Printf("Fetch %d rows(s) in %.2fs", len(results), time.Duration(time.Since(startTime)).Seconds())
}

```

## Building

To build this project you must have libsasl2 installed.
On Debian: 
```bash
sudo apt-get install libsasl2-dev
```

On Redhat:
```bash
sudo yum install cyrus-sasl-devel.x86_64
```