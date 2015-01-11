package cluster

import (
	"time"

	"gopkg.in/redis.v2"
)

type Options struct {
	// A seed-list of host:port addresses of known cluster nodes
	Addrs []string

	// An optional password
	Password string

	// The maximum number of open connections.
	// Default: 10
	//
	// ATTENTION:
	// This is the maximum of Redis connections,
	// not TCP connections. In theory the absolute maximuma
	// of TCP connections is limited by: MaxConns x PoolSize
	MaxConns int

	// The maximum number of TCP connections per
	// Redis connection. Default: 10
	PoolSize int

	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

func (o *Options) maxConns() int {
	if o.MaxConns < 1 {
		return 10
	}
	return o.MaxConns
}

func (o *Options) options(addr string) *redis.Options {
	return &redis.Options{
		Addr: addr,

		Password: o.Password,
		PoolSize: o.PoolSize,

		DialTimeout:  o.DialTimeout,
		ReadTimeout:  o.ReadTimeout,
		WriteTimeout: o.WriteTimeout,
		IdleTimeout:  o.IdleTimeout,
	}
}
