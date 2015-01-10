package cluster

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("slotInfo", func() {

	It("should parse from result", func() {
		info, err := parseSlotInfo([]interface{}{
			[]interface{}{int64(0), int64(4095), []interface{}{"127.0.0.1", int64(7000)}, []interface{}{"127.0.0.1", int64(7004)}},
			[]interface{}{int64(12288), int64(16383), []interface{}{"127.0.0.1", int64(7003)}, []interface{}{"127.0.0.1", int64(7007)}},
			[]interface{}{int64(4096), int64(8191), []interface{}{"127.0.0.1", int64(7001)}, []interface{}{"127.0.0.1", int64(7005)}},
			[]interface{}{int64(8192), int64(12287), []interface{}{"127.0.0.1", int64(7002)}, []interface{}{"127.0.0.1", int64(7006)}},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(info).To(Equal([]slotInfo{
			{min: 0, max: 4095, addrs: []string{"127.0.0.1:7000", "127.0.0.1:7004"}},
			{min: 12288, max: 16383, addrs: []string{"127.0.0.1:7003", "127.0.0.1:7007"}},
			{min: 4096, max: 8191, addrs: []string{"127.0.0.1:7001", "127.0.0.1:7005"}},
			{min: 8192, max: 12287, addrs: []string{"127.0.0.1:7002", "127.0.0.1:7006"}},
		}))
	})

})
