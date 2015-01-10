package cluster

import (
	"strconv"
	"time"

	"gopkg.in/redis.v2"
)

func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func firstKey(args []string) string {
	if len(args) > 0 {
		return args[0]
	}
	return ""
}

func (c *Client) Del(keys ...string) *redis.IntCmd {
	cmd := redis.NewIntCmd(append([]string{"DEL"}, keys...)...)
	c.Process(HashSlot(firstKey(keys)), cmd)
	return cmd
}

func (c *Client) Dump(key string) *redis.StringCmd {
	cmd := redis.NewStringCmd("DUMP", key)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) Exists(key string) *redis.BoolCmd {
	cmd := redis.NewBoolCmd("EXISTS", key)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) Expire(key string, dur time.Duration) *redis.BoolCmd {
	cmd := redis.NewBoolCmd("EXPIRE", key, strconv.FormatInt(int64(dur/time.Second), 10))
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) ExpireAt(key string, tm time.Time) *redis.BoolCmd {
	cmd := redis.NewBoolCmd("EXPIREAT", key, strconv.FormatInt(tm.Unix(), 10))
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) PExpire(key string, dur time.Duration) *redis.BoolCmd {
	cmd := redis.NewBoolCmd("PEXPIRE", key, strconv.FormatInt(int64(dur/time.Millisecond), 10))
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) PExpireAt(key string, tm time.Time) *redis.BoolCmd {
	cmd := redis.NewBoolCmd(
		"PEXPIREAT",
		key,
		strconv.FormatInt(tm.UnixNano()/int64(time.Millisecond), 10),
	)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) PTTL(key string) *redis.DurationCmd {
	cmd := redis.NewDurationCmd(time.Millisecond, "PTTL", key)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) TTL(key string) *redis.DurationCmd {
	cmd := redis.NewDurationCmd(time.Second, "TTL", key)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) Type(key string) *redis.StatusCmd {
	cmd := redis.NewStatusCmd("TYPE", key)
	c.Process(HashSlot(key), cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *Client) Append(key, value string) *redis.IntCmd {
	cmd := redis.NewIntCmd("APPEND", key, value)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) BitCount(key string, bitCount *redis.BitCount) *redis.IntCmd {
	args := []string{"BITCOUNT", key}
	if bitCount != nil {
		args = append(
			args,
			strconv.FormatInt(bitCount.Start, 10),
			strconv.FormatInt(bitCount.End, 10),
		)
	}
	cmd := redis.NewIntCmd(args...)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) Decr(key string) *redis.IntCmd {
	cmd := redis.NewIntCmd("DECR", key)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) DecrBy(key string, decrement int64) *redis.IntCmd {
	cmd := redis.NewIntCmd("DECRBY", key, strconv.FormatInt(decrement, 10))
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) Get(key string) *redis.StringCmd {
	cmd := redis.NewStringCmd("GET", key)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) GetBit(key string, offset int64) *redis.IntCmd {
	cmd := redis.NewIntCmd("GETBIT", key, strconv.FormatInt(offset, 10))
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) GetRange(key string, start, end int64) *redis.StringCmd {
	cmd := redis.NewStringCmd(
		"GETRANGE",
		key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(end, 10),
	)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) GetSet(key, value string) *redis.StringCmd {
	cmd := redis.NewStringCmd("GETSET", key, value)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) Incr(key string) *redis.IntCmd {
	cmd := redis.NewIntCmd("INCR", key)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) IncrBy(key string, value int64) *redis.IntCmd {
	cmd := redis.NewIntCmd("INCRBY", key, strconv.FormatInt(value, 10))
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) IncrByFloat(key string, value float64) *redis.FloatCmd {
	cmd := redis.NewFloatCmd("INCRBYFLOAT", key, formatFloat(value))
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) MGet(keys ...string) *redis.SliceCmd {
	cmd := redis.NewSliceCmd(append([]string{"MGET"}, keys...)...)
	c.Process(HashSlot(firstKey(keys)), cmd)
	return cmd
}

func (c *Client) MSet(pairs ...string) *redis.StatusCmd {
	cmd := redis.NewStatusCmd(append([]string{"MSET"}, pairs...)...)
	c.Process(HashSlot(firstKey(pairs)), cmd)
	return cmd
}

func (c *Client) MSetNX(pairs ...string) *redis.BoolCmd {
	cmd := redis.NewBoolCmd(append([]string{"MSETNX"}, pairs...)...)
	c.Process(HashSlot(firstKey(pairs)), cmd)
	return cmd
}

func (c *Client) PSetEx(key string, dur time.Duration, value string) *redis.StatusCmd {
	cmd := redis.NewStatusCmd(
		"PSETEX",
		key,
		strconv.FormatInt(int64(dur/time.Millisecond), 10),
		value,
	)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) Set(key, value string) *redis.StatusCmd {
	cmd := redis.NewStatusCmd("SET", key, value)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) SetBit(key string, offset int64, value int) *redis.IntCmd {
	cmd := redis.NewIntCmd(
		"SETBIT",
		key,
		strconv.FormatInt(offset, 10),
		strconv.FormatInt(int64(value), 10),
	)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) SetEx(key string, dur time.Duration, value string) *redis.StatusCmd {
	cmd := redis.NewStatusCmd("SETEX", key, strconv.FormatInt(int64(dur/time.Second), 10), value)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) SetNX(key, value string) *redis.BoolCmd {
	cmd := redis.NewBoolCmd("SETNX", key, value)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) SetRange(key string, offset int64, value string) *redis.IntCmd {
	cmd := redis.NewIntCmd("SETRANGE", key, strconv.FormatInt(offset, 10), value)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) StrLen(key string) *redis.IntCmd {
	cmd := redis.NewIntCmd("STRLEN", key)
	c.Process(HashSlot(key), cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *Client) HDel(key string, fields ...string) *redis.IntCmd {
	args := append([]string{"HDEL", key}, fields...)
	cmd := redis.NewIntCmd(args...)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) HExists(key, field string) *redis.BoolCmd {
	cmd := redis.NewBoolCmd("HEXISTS", key, field)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) HGet(key, field string) *redis.StringCmd {
	cmd := redis.NewStringCmd("HGET", key, field)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) HGetAll(key string) *redis.StringSliceCmd {
	cmd := redis.NewStringSliceCmd("HGETALL", key)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) HGetAllMap(key string) *redis.StringStringMapCmd {
	cmd := redis.NewStringStringMapCmd("HGETALL", key)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) HIncrBy(key, field string, incr int64) *redis.IntCmd {
	cmd := redis.NewIntCmd("HINCRBY", key, field, strconv.FormatInt(incr, 10))
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) HIncrByFloat(key, field string, incr float64) *redis.FloatCmd {
	cmd := redis.NewFloatCmd("HINCRBYFLOAT", key, field, formatFloat(incr))
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) HKeys(key string) *redis.StringSliceCmd {
	cmd := redis.NewStringSliceCmd("HKEYS", key)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) HLen(key string) *redis.IntCmd {
	cmd := redis.NewIntCmd("HLEN", key)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) HMGet(key string, fields ...string) *redis.SliceCmd {
	args := append([]string{"HMGET", key}, fields...)
	cmd := redis.NewSliceCmd(args...)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) HMSet(key, field, value string, pairs ...string) *redis.StatusCmd {
	args := append([]string{"HMSET", key, field, value}, pairs...)
	cmd := redis.NewStatusCmd(args...)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) HSet(key, field, value string) *redis.BoolCmd {
	cmd := redis.NewBoolCmd("HSET", key, field, value)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) HSetNX(key, field, value string) *redis.BoolCmd {
	cmd := redis.NewBoolCmd("HSETNX", key, field, value)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) HVals(key string) *redis.StringSliceCmd {
	cmd := redis.NewStringSliceCmd("HVALS", key)
	c.Process(HashSlot(key), cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *Client) LIndex(key string, index int64) *redis.StringCmd {
	cmd := redis.NewStringCmd("LINDEX", key, strconv.FormatInt(index, 10))
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) LInsert(key, op, pivot, value string) *redis.IntCmd {
	cmd := redis.NewIntCmd("LINSERT", key, op, pivot, value)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) LLen(key string) *redis.IntCmd {
	cmd := redis.NewIntCmd("LLEN", key)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) LPop(key string) *redis.StringCmd {
	cmd := redis.NewStringCmd("LPOP", key)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) LPush(key string, values ...string) *redis.IntCmd {
	args := append([]string{"LPUSH", key}, values...)
	cmd := redis.NewIntCmd(args...)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) LPushX(key, value string) *redis.IntCmd {
	cmd := redis.NewIntCmd("LPUSHX", key, value)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) LRange(key string, start, stop int64) *redis.StringSliceCmd {
	cmd := redis.NewStringSliceCmd(
		"LRANGE",
		key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(stop, 10),
	)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) LRem(key string, count int64, value string) *redis.IntCmd {
	cmd := redis.NewIntCmd("LREM", key, strconv.FormatInt(count, 10), value)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) LSet(key string, index int64, value string) *redis.StatusCmd {
	cmd := redis.NewStatusCmd("LSET", key, strconv.FormatInt(index, 10), value)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) LTrim(key string, start, stop int64) *redis.StatusCmd {
	cmd := redis.NewStatusCmd(
		"LTRIM",
		key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(stop, 10),
	)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) RPop(key string) *redis.StringCmd {
	cmd := redis.NewStringCmd("RPOP", key)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) RPopLPush(source, destination string) *redis.StringCmd {
	cmd := redis.NewStringCmd("RPOPLPUSH", source, destination)
	c.Process(HashSlot(source), cmd)
	return cmd
}

func (c *Client) RPush(key string, values ...string) *redis.IntCmd {
	args := append([]string{"RPUSH", key}, values...)
	cmd := redis.NewIntCmd(args...)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) RPushX(key string, value string) *redis.IntCmd {
	cmd := redis.NewIntCmd("RPUSHX", key, value)
	c.Process(HashSlot(key), cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c *Client) SAdd(key string, members ...string) *redis.IntCmd {
	args := append([]string{"SADD", key}, members...)
	cmd := redis.NewIntCmd(args...)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) SCard(key string) *redis.IntCmd {
	cmd := redis.NewIntCmd("SCARD", key)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) SDiff(keys ...string) *redis.StringSliceCmd {
	args := append([]string{"SDIFF"}, keys...)
	cmd := redis.NewStringSliceCmd(args...)
	c.Process(HashSlot(firstKey(keys)), cmd)
	return cmd
}

func (c *Client) SDiffStore(destination string, keys ...string) *redis.IntCmd {
	args := append([]string{"SDIFFSTORE", destination}, keys...)
	cmd := redis.NewIntCmd(args...)
	c.Process(HashSlot(destination), cmd)
	return cmd
}

func (c *Client) SInter(keys ...string) *redis.StringSliceCmd {
	args := append([]string{"SINTER"}, keys...)
	cmd := redis.NewStringSliceCmd(args...)
	c.Process(HashSlot(firstKey(keys)), cmd)
	return cmd
}

func (c *Client) SInterStore(destination string, keys ...string) *redis.IntCmd {
	args := append([]string{"SINTERSTORE", destination}, keys...)
	cmd := redis.NewIntCmd(args...)
	c.Process(HashSlot(destination), cmd)
	return cmd
}

func (c *Client) SIsMember(key, member string) *redis.BoolCmd {
	cmd := redis.NewBoolCmd("SISMEMBER", key, member)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) SMembers(key string) *redis.StringSliceCmd {
	cmd := redis.NewStringSliceCmd("SMEMBERS", key)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) SMove(source, destination, member string) *redis.BoolCmd {
	cmd := redis.NewBoolCmd("SMOVE", source, destination, member)
	c.Process(HashSlot(source), cmd)
	return cmd
}

func (c *Client) SPop(key string) *redis.StringCmd {
	cmd := redis.NewStringCmd("SPOP", key)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) SRandMember(key string) *redis.StringCmd {
	cmd := redis.NewStringCmd("SRANDMEMBER", key)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) SRem(key string, members ...string) *redis.IntCmd {
	args := append([]string{"SREM", key}, members...)
	cmd := redis.NewIntCmd(args...)
	c.Process(HashSlot(key), cmd)
	return cmd
}

func (c *Client) SUnion(keys ...string) *redis.StringSliceCmd {
	args := append([]string{"SUNION"}, keys...)
	cmd := redis.NewStringSliceCmd(args...)
	c.Process(HashSlot(firstKey(keys)), cmd)
	return cmd
}

func (c *Client) SUnionStore(destination string, keys ...string) *redis.IntCmd {
	args := append([]string{"SUNIONSTORE", destination}, keys...)
	cmd := redis.NewIntCmd(args...)
	c.Process(HashSlot(destination), cmd)
	return cmd
}

//------------------------------------------------------------------------------
