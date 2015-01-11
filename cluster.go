package cluster

import (
	"errors"
	"io"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"gopkg.in/redis.v2"
)

const (
	HashSlots    = 16384
	MaxRedirects = 16
)

var errNoAddresses = errors.New("redis cluster: missing addresses")

type Client struct {
	addrs []string
	opts  *Options

	slots [][]string
	conns *connLRU

	forceReload uint32

	lock sync.RWMutex
}

// Connect connects to a cluster, using a list of seeds
func Connect(opts *Options) (*Client, error) {
	client := newClient(opts)
	if err := client.reload(); err != nil {
		return nil, err
	} else if len(client.addrs) < 1 {
		return nil, errNoAddresses
	}
	return client, nil
}

func newClient(opts *Options) *Client {
	if opts == nil {
		opts = &Options{}
	}
	return &Client{
		addrs: opts.Addrs,
		opts:  opts,
		conns: newLRU(opts.maxConns()),
	}
}

// Close closes all cached connections
func (c *Client) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.reset()
	return nil
}

// Process applies a single command to a hashSlot
func (c *Client) Process(hashSlot int, cmd redis.Cmder) {
	if c.reloadDue() {
		c.reload()
	}

	ask := false

	c.lock.RLock()
	defer c.lock.RUnlock()

	tried := make(map[string]struct{}, len(c.addrs))
	addr := c.slotAddr(hashSlot)
	for attempt := 0; attempt < MaxRedirects; attempt++ {
		tried[addr] = struct{}{}

		// Pick the connection, process request
		conn := c.conns.Fetch(addr, c.connectTo)
		if ask {
			pipe := conn.Pipeline()
			pipe.Process(redis.NewCmd("ASKING"))
			pipe.Process(cmd)
			_, _ = pipe.Exec()
			ask = false
		} else {
			conn.Process(cmd)
		}

		// If there is no (real) error, we are done!
		err := cmd.Err()
		if err == nil || err == redis.Nil {
			return
		}

		// On connection errors, pick the next (not previosuly) tried connection
		// and try again
		if _, ok := err.(*net.OpError); ok || err == io.EOF {
			if addr = c.nextAddr(tried); addr == "" {
				return
			}
			cmd.Reset()
			continue
		}

		// Check the error message, return if unexpected
		parts := strings.SplitN(err.Error(), " ", 3)
		if len(parts) != 3 {
			return
		}

		// Handle MOVE and ASK redirections, return on any other error
		switch parts[0] {
		case "MOVED":
			c.forceReloadOnNextCommand()
			addr = parts[2]
		case "ASK":
			ask = true
			addr = parts[2]
		default:
			return
		}
		cmd.Reset()
	}
}

// Closes all connections and reloads slot cache
func (c *Client) reload() (err error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	for _, addr := range c.addrs {
		c.reset()

		var infos []slotInfo
		if infos, err = c.clusterSlots(addr); err == nil {
			c.cacheSlots(infos)
			break
		}
	}
	return
}

// Closes all connections and flushes slots cache
func (c *Client) reset() {
	c.conns.Clear()
	c.slots = make([][]string, HashSlots)
}

// Set slots cache
func (c *Client) cacheSlots(infos []slotInfo) {
	// Create a map of known nodes
	known := make(map[string]struct{}, len(c.addrs))
	for _, addr := range c.addrs {
		known[addr] = struct{}{}
	}

	// Populate slots, store unknown nodes
	for _, info := range infos {
		for i := info.min; i <= info.max; i++ {
			c.slots[i] = info.addrs
		}

		for _, addr := range info.addrs {
			if _, ok := known[addr]; !ok {
				c.addrs = append(c.addrs, addr)
				known[addr] = struct{}{}
			}
		}
	}

	// Shuffle addresses
	for i := range c.addrs {
		j := rand.Intn(i + 1)
		c.addrs[i], c.addrs[j] = c.addrs[j], c.addrs[i]
	}
}

func (c *Client) clusterSlots(addr string) ([]slotInfo, error) {
	conn := c.connectTo(addr)
	defer conn.Close()

	cmd := redis.NewSliceCmd("CLUSTER", "SLOTS")
	conn.Process(cmd)

	result, err := cmd.Result()
	if err != nil {
		return nil, err
	}
	return parseSlotInfo(result)
}

// Connect to an address
func (c *Client) connectTo(addr string) *redis.Client {
	return redis.NewTCPClient(c.opts.options(addr))
}

// Forces a cache reload on next request
func (c *Client) forceReloadOnNextCommand() {
	atomic.StoreUint32(&c.forceReload, 1)
}

// Is a cache reload due
func (c *Client) reloadDue() bool {
	return atomic.CompareAndSwapUint32(&c.forceReload, 1, 0)
}

// Find the current address for a hash slot
func (c *Client) slotAddr(hashSlot int) string {
	if len(c.slots) == HashSlots {
		if addrs := c.slots[hashSlot]; len(addrs) > 0 {
			return addrs[0]
		}
	}
	return ""
}

// Find the next untried address
func (c *Client) nextAddr(tried map[string]struct{}) string {
	for _, addr := range c.addrs {
		if _, ok := tried[addr]; !ok {
			return addr
		}
	}
	return ""
}
