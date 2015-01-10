package cluster

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/redis.v2"
)

var _ = Describe("Options", func() {

	It("should create redis options", func() {
		opts := &Options{}
		Expect(opts.options("127.0.0.1:7001")).To(Equal(&redis.Options{
			Network: "tcp",
			Addr:    "127.0.0.1:7001",
		}))
	})

	It("should have a max-conn default", func() {
		opts := &Options{}
		Expect(opts.maxConns()).To(Equal(10))
		opts.MaxConns = 20
		Expect(opts.maxConns()).To(Equal(20))
	})

})
