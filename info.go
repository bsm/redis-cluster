package cluster

import (
	"errors"
	"net"
	"strconv"
)

type slotInfo struct {
	min, max int
	addrs    []string
}

var errInvalidSlotInfo = errors.New("redis-cluster: invalid slot info")

func parseSlotInfo(res []interface{}) ([]slotInfo, error) {
	infos := make([]slotInfo, len(res))
	for i, iitem := range res {
		item, ok := iitem.([]interface{})
		if !ok || len(item) < 3 {
			return nil, errInvalidSlotInfo
		}

		min, ok := item[0].(int64)
		if !ok || min < 0 || min > HashSlots {
			return nil, errInvalidSlotInfo
		}

		max, ok := item[1].(int64)
		if !ok || max < 0 || max > HashSlots {
			return nil, errInvalidSlotInfo
		}

		info := slotInfo{min: int(min), max: int(max), addrs: make([]string, len(item)-2)}
		for n, ipair := range item[2:] {
			pair, ok := ipair.([]interface{})
			if !ok || len(pair) != 2 {
				return nil, errInvalidSlotInfo
			}

			ip, ok := pair[0].(string)
			if !ok || len(ip) < 1 {
				return nil, errInvalidSlotInfo
			}

			port, ok := pair[1].(int64)
			if !ok || port < 1 {
				return nil, errInvalidSlotInfo
			}

			info.addrs[n] = net.JoinHostPort(ip, strconv.FormatInt(port, 10))
		}

		infos[i] = info
	}
	return infos, nil
}
