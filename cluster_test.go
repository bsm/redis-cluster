package cluster

import (
	"sort"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Client", func() {
	var subject *Client
	var populate = func() {
		subject.reset()
		subject.cacheSlots([]slotInfo{
			{min: 0, max: 4095, addrs: []string{"127.0.0.1:7000", "127.0.0.1:7004"}},
			{min: 12288, max: 16383, addrs: []string{"127.0.0.1:7003", "127.0.0.1:7007"}},
			{min: 4096, max: 8191, addrs: []string{"127.0.0.1:7001", "127.0.0.1:7005"}},
			{min: 8192, max: 12287, addrs: []string{"127.0.0.1:7002", "127.0.0.1:7006"}},
		})
	}

	BeforeEach(func() {
		subject = newClient(&Options{
			Addrs: []string{"127.0.0.1:6379", "127.0.0.1:7003", "127.0.0.1:7006"},
		})
	})

	AfterEach(func() {
		subject.Close()
	})

	It("should reset slots cache an connections", func() {
		populate()
		subject.conns.Fetch("127.0.0.1:7003", subject.connectTo)
		Expect(subject.slots).To(HaveLen(HashSlots))
		Expect(subject.slots[0]).To(HaveLen(2))
		Expect(subject.conns.len()).To(Equal(1))

		subject.reset()
		Expect(subject.slots).To(HaveLen(HashSlots))
		Expect(subject.slots[0]).To(BeEmpty())
		Expect(subject.conns.len()).To(Equal(0))
	})

	It("should populate slots cache", func() {
		populate()
		Expect(subject.slots[0]).To(Equal([]string{"127.0.0.1:7000", "127.0.0.1:7004"}))
		Expect(subject.slots[4095]).To(Equal([]string{"127.0.0.1:7000", "127.0.0.1:7004"}))
		Expect(subject.slots[4096]).To(Equal([]string{"127.0.0.1:7001", "127.0.0.1:7005"}))
		Expect(subject.slots[8191]).To(Equal([]string{"127.0.0.1:7001", "127.0.0.1:7005"}))
		Expect(subject.slots[8192]).To(Equal([]string{"127.0.0.1:7002", "127.0.0.1:7006"}))
		Expect(subject.slots[12287]).To(Equal([]string{"127.0.0.1:7002", "127.0.0.1:7006"}))
		Expect(subject.slots[12288]).To(Equal([]string{"127.0.0.1:7003", "127.0.0.1:7007"}))
		Expect(subject.slots[16383]).To(Equal([]string{"127.0.0.1:7003", "127.0.0.1:7007"}))

		Expect(subject.conns.len()).To(Equal(0))

		Expect(subject.addrs).To(ConsistOf([]string{
			"127.0.0.1:6379",
			"127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003",
			"127.0.0.1:7004", "127.0.0.1:7005", "127.0.0.1:7006", "127.0.0.1:7007",
		}))
	})

	It("should find the current address of a slot", func() {
		Expect(subject.slotAddr(1000)).To(Equal(""))
		populate()
		Expect(subject.slotAddr(1000)).To(Equal("127.0.0.1:7000"))
	})

	It("should find next addresses", func() {
		populate()
		seen := map[string]struct{}{
			"127.0.0.1:7000": struct{}{},
			"127.0.0.1:7001": struct{}{},
			"127.0.0.1:7003": struct{}{},
		}
		sort.Strings(subject.addrs)

		Expect(subject.nextAddr(seen)).To(Equal("127.0.0.1:6379"))
		seen["127.0.0.1:6379"] = struct{}{}
		Expect(subject.nextAddr(seen)).To(Equal("127.0.0.1:7002"))
		seen["127.0.0.1:7002"] = struct{}{}
		Expect(subject.nextAddr(seen)).To(Equal("127.0.0.1:7004"))
		seen["127.0.0.1:7004"] = struct{}{}
		Expect(subject.nextAddr(seen)).To(Equal("127.0.0.1:7005"))
		seen["127.0.0.1:7005"] = struct{}{}
		Expect(subject.nextAddr(seen)).To(Equal("127.0.0.1:7006"))
		seen["127.0.0.1:7006"] = struct{}{}
		Expect(subject.nextAddr(seen)).To(Equal("127.0.0.1:7007"))
		seen["127.0.0.1:7007"] = struct{}{}
		Expect(subject.nextAddr(seen)).To(Equal(""))
	})

	It("should check if reload is due", func() {
		Expect(subject.reloadDue()).To(BeFalse())
		subject.forceReloadOnNextCommand()
		Expect(subject.reloadDue()).To(BeTrue())
		Expect(subject.reloadDue()).To(BeFalse())
	})

})

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "github.com/bsm/redis-cluster")
}
