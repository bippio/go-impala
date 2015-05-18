Impalathing is a small Go wrapper library the thrift interface go Impala

It's based on [hivething](https://github.com/derekgr/hivething)

Working on this you quickly realize that having strings deliminated by tabs is a ugly API... (That's the thrift
side of things)

## Usage

```go
package main

import (
    "log"
    "fmt"
    "time"
    "github.com/koblas/impalathing"
)

func main() {
    host := "impala-host"
    port := 21000

    con, err := impalathing.Connect(host, port, impalathing.DefaultOptions)

    if err != nil {
        log.Fatal("Error connecting", err)
        return
    }

    query, err := con.Query("SELECT user_id, action, yyyymm FROM engagements LIMIT 10000")

    startTime := time.Now()
    total := 0
    for query.Next() {
        var (
            user_id     string
            action      string
            yyyymm      int
        )

        query.Scan(&user_id, &action, &yyyymm)
        total += 1

        fmt.Println(user_id, action)
    }

    log.Printf("Fetch %d rows(s) in %.2fs", total, time.Duration(time.Since(startTime)).Seconds())
}

```
