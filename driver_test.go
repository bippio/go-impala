package impala

import (
	"io/ioutil"
	"testing"
)

func TestParseURI(t *testing.T) {
	tests := []struct {
		in  string
		out Options
	}{
		{
			"impala://localhost",
			Options{Host: "localhost", Port: "21050", BatchSize: 1024, BufferSize: 4096, LogOut: ioutil.Discard},
		},
		{
			"impala://localhost:21000",
			Options{Host: "localhost", Port: "21000", BatchSize: 1024, BufferSize: 4096, LogOut: ioutil.Discard},
		},
		{
			"impala://admin@localhost",
			Options{Host: "localhost", Port: "21050", Username: "admin", BatchSize: 1024, BufferSize: 4096, LogOut: ioutil.Discard},
		},
		{
			"impala://admin:password@localhost",
			Options{Host: "localhost", Port: "21050", Username: "admin", Password: "password", BatchSize: 1024, BufferSize: 4096, LogOut: ioutil.Discard},
		},
		{
			"impala://admin:p%40ssw0rd@localhost",
			Options{Host: "localhost", Port: "21050", Username: "admin", Password: "p@ssw0rd", BatchSize: 1024, BufferSize: 4096, LogOut: ioutil.Discard},
		},
		{
			"impala://admin:p%40ssw0rd@localhost?auth=ldap",
			Options{Host: "localhost", Port: "21050", Username: "admin", Password: "p@ssw0rd", UseLDAP: true, BatchSize: 1024, BufferSize: 4096, LogOut: ioutil.Discard},
		},
		{
			"impala://localhost?tls=true&ca-cert=/etc/ca.crt",
			Options{Host: "localhost", Port: "21050", UseTLS: true, CACertPath: "/etc/ca.crt", BatchSize: 1024, BufferSize: 4096, LogOut: ioutil.Discard},
		},
		{
			"impala://localhost?batch-size=2048&buffer-size=2048",
			Options{Host: "localhost", Port: "21050", BatchSize: 2048, BufferSize: 2048, LogOut: ioutil.Discard},
		},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			opts, err := parseURI(tt.in)
			if err != nil {
				t.Error(err)
				return
			}
			if *opts != tt.out {
				t.Errorf("got: %v, want: %v", opts, tt.out)
			}
		})
	}
}
