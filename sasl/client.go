package sasl

import (
	"fmt"
)

var supported = map[string]func(*Options) mech{
	MechPlain: newPlain,
}

// NewClient created new sasl client
func NewClient(opts *Options) Client {
	return &client{
		opts: opts,
	}
}

func (c *client) Start(mechlist []string) (string, []byte, bool, error) {
	for _, mech := range mechlist {
		if newMech, ok := supported[mech]; ok {
			c.m = newMech(c.opts)
			return c.m.Start()
		}
	}
	return "", nil, false, fmt.Errorf("sasl: none of the provided mechs %v are supported", mechlist)
}

func (c *client) Step(challenge []byte) ([]byte, bool, error) {
	return c.m.Step(challenge)
}

func (c *client) Free() {}

type mech interface {
	Start() (mech string, initial []byte, done bool, err error)
	Step(challenge []byte) (response []byte, done bool, err error)
}

type client struct {
	m    mech
	opts *Options
}
