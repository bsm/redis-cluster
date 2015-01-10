package cluster

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("CRC", func() {

	It("should calculate CRC-16 digests", func() {
		Expect(crc16sum("123456789")).To(Equal(uint16(12739)))
	})

})

var _ = Describe("HashSlot", func() {

	It("should calculate hash slots", func() {
		Expect(HashSlot("123456789")).To(Equal(12739))
		Expect(HashSlot("{}foo")).To(Equal(9500))
		Expect(HashSlot("foo{}")).To(Equal(5542))
		Expect(HashSlot("foo{}{bar}")).To(Equal(8363))
	})

	It("should extract keys from tags", func() {
		tests := []struct {
			one, two string
		}{
			{"foo{bar}", "bar"},
			{"{foo}bar", "foo"},
			{"{user1000}.following", "{user1000}.followers"},
			{"foo{{bar}}zap", "{bar"},
			{"foo{bar}{zap}", "bar"},
		}

		for _, test := range tests {
			Expect(HashSlot(test.one)).To(Equal(HashSlot(test.two)), "for %s <-> %s", test.one, test.two)
		}
	})

})
