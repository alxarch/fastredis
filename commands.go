package redis

import (
	"time"

	"github.com/alxarch/fastredis/resp"
)

// Cluster

// TODO: [commands] Cluster commands

// Connection

// Auth authenticates to the server
func (p *Pipeline) Auth(password string) {
	p.BulkStringArray("AUTH", password)
}

// Echo exchos the given string
func (p *Pipeline) Echo(message string) {
	p.BulkStringArray("ECHO", message)
}

// Ping pings the server
func (p *Pipeline) Ping(message string) {
	p.BulkStringArray("PING", message)
}

// Quit closes the connection
func (p *Pipeline) Quit() {
	p.BulkStringArray("QUIT")
}

// Select changes the selected database for the current connection
func (p *Pipeline) Select(db int64) {
	p.do("SELECT", resp.Int(db))
}

// SwapDB swaps two Redis databases
func (p *Pipeline) SwapDB(i, j int64) {
	p.do("SWAPDB", resp.Int(i), resp.Int(j))
}

// Hashes

// HDel deletes one or more hash fields
func (p *Pipeline) HDel(key string, fields ...string) {
	p.Command("HDEL", len(fields))
	p.Arg(resp.Key(key))
	for _, f := range fields {
		p.Arg(resp.String(f))
	}
}

// HExists determines if a hash field exists
func (p *Pipeline) HExists(key string, field string) {
	p.do("HEXISTS", resp.Key(key), resp.String(field))
}

// HGet gets the value of a hash field
func (p *Pipeline) HGet(key, field string) {
	p.do("HSET", resp.Key(key), resp.String(field))
}

// HGetAll gets all the fields and values in a hash
func (p *Pipeline) HGetAll(key string) {
	p.do("HGETALL", resp.Key(key))
}

// HIncrBy increments the integer value of a hash field by the given number
func (p *Pipeline) HIncrBy(key, field string, n int64) {
	p.do("HINCRBY", resp.Key(key), resp.String(field), resp.Int(n))
}

// HIncrByFloat increments the float value of a hash field by the given amount
func (p *Pipeline) HIncrByFloat(key, field string, f float64) {
	p.do("HINCRBYFLOAT", resp.Key(key), resp.String(field), resp.Float(f))
}

// HKeys gets all the fields in a hash
func (p *Pipeline) HKeys(key string) {
	p.do("HKEYS", resp.Key(key))
}

// HLen gets the number of fields in a hash
func (p *Pipeline) HLen(key string) {
	p.do("HLEN", resp.Key(key))
}

// HMGet gets the values of all the the given hash fields
func (p *Pipeline) HMGet(key string, fields ...string) {
	p.Command("HMGET", 1+len(fields))
	p.Arg(resp.Key(key))
	for _, f := range fields {
		p.Arg(resp.String(f))
	}
}

// HMSet sets multiple hash fields to multiple values
func (p *Pipeline) HMSet(key string, values ...resp.KV) {
	p.Command("HMSET", 1+2*len(values))
	p.Arg(resp.Key(key))
	for i := range values {
		kv := &values[i]
		p.Arg(resp.String(kv.Key))
		p.Arg(kv.Arg)
	}
}

// HSet sets the value of a hash field
func (p *Pipeline) HSet(key, field string, value resp.Arg) {
	p.do("HSET", resp.Key(key), resp.String(field), value)
}

// HSetNX sets the value of a hash field, only if the field does not exist
func (p *Pipeline) HSetNX(key, field string, value resp.Arg) {
	p.do("HSETNX", resp.Key(key), resp.String(field), value)
}

// HStrLen gets the length of the value of a hash field
func (p *Pipeline) HStrLen(key, field string) {
	p.do("HSTRLEN", resp.Key(key), resp.String(field))
}

// HVals get all the values in a hash
func (p *Pipeline) HVals(key string) {
	p.do("HVALS", resp.Key(key))
}

// HScan incrementally iterates hash fields and associated values
func (p *Pipeline) HScan(key string, cur int64, match string, count int64) {
	if count <= 0 {
		count = defaultScanCount
	}
	if match == "" {
		p.do("HSCAN", resp.Key(key), resp.Int(cur), resp.String("COUNT"), resp.Int(count))
	} else {
		p.do("HSCAN", resp.Key(key), resp.Int(cur), resp.String("MATCH"), resp.String(match), resp.String("COUNT"), resp.Int(count))
	}
}

// HyperLogLog

// PFAdd adds the specified elements to the specified HyperLogLog
func (p *Pipeline) PFAdd(key string, elements ...string) {
	p.Command("PFADD", 1+len(elements))
	p.Arg(resp.Key(key))
	for _, el := range elements {
		p.Arg(resp.String(el))
	}
}

// PFCount returns the approximate cardinality of the set
func (p *Pipeline) PFCount(keys ...string) {
	p.Command("PFCOUNT", len(keys))
	for _, k := range keys {
		p.Arg(resp.Key(k))
	}
}

// PFMerge merges different HyperLoglLogs into a single one
func (p *Pipeline) PFMerge(dest string, src ...string) {
	p.Command("PFMERGE", 1+len(src))
	p.Arg(resp.Key(dest))
	for _, k := range src {
		p.Arg(resp.Key(k))
	}
}

// Keys

// Del deletes a key
func (p *Pipeline) Del(keys ...string) {
	p.Command("DEL", len(keys))
	for _, key := range keys {
		p.Arg(resp.Key(key))
	}
}

// Dump returns a serialized version of the value stored at key
func (p *Pipeline) Dump(key string) {
	p.do("DUMP", resp.Key(key))
}

// Exists determines if a key exists
func (p *Pipeline) Exists(keys ...string) {
	p.Command("EXISTS", len(keys))
	for _, key := range keys {
		p.Arg(resp.Key(key))
	}
}

// Expire sets a key's time to live
func (p *Pipeline) Expire(key string, ttl time.Duration) {
	p.do("PEXPIRE", resp.Key(key), resp.Int(int64(ttl/time.Millisecond)))
}

// ExpireAt sets the expiration time for a key as a UNIX timestamp
func (p *Pipeline) ExpireAt(key string, tm time.Time) {
	ms := tm.UnixNano() / int64(time.Millisecond)
	p.do("PEXPIREAT", resp.Key(key), resp.Int(ms))
}

// Keys finds all keys matching the given pattern
func (p *Pipeline) Keys(pattern string) {
	if pattern == "" {
		pattern = "*"
	}
	p.do("KEYS", resp.String(pattern))
}

// Migrate options
type Migrate struct {
	Host    string
	Port    string
	DB      int64
	Timeout time.Duration
	Copy    bool
	Replace bool
}

// Migrate atomically transfers a key from a redis instance to another one
func (p *Pipeline) Migrate(m Migrate, keys ...string) {
	args := []resp.Arg{
		resp.String(m.Host),
		resp.String(m.Port),
		resp.String(""),
		resp.Int(m.DB),
		resp.Int(int64(m.Timeout / time.Second)),
	}

	if m.Copy {
		args = append(args, resp.String("COPY"))
	}
	if m.Replace {
		args = append(args, resp.String("REPLACE"))
	}
	args = append(args, resp.String("KEYS"))
	for _, key := range keys {
		args = append(args, resp.Key(key))
	}
	p.do("MIGRATE", args...)
}

// Move moves a key to another database
func (p *Pipeline) Move(key string, db int64) {
	p.do("MOVE", resp.Key(key), resp.Int(db))
}

// Persist removes the expiration from a key
func (p *Pipeline) Persist(key string) {
	p.do("PERSIST", resp.Key(key))
}

// PTTL gets the time to live for a key in milliseconds
func (p *Pipeline) PTTL(key string) {
	p.do("PTTL", resp.Key(key))
}

// RandomKey returns a random key from the keyspace
func (p *Pipeline) RandomKey() {
	p.do("RANDOMKEY")
}

// Rename renames a key
func (p *Pipeline) Rename(key, newkey string) {
	p.do("RENAME", resp.Key(key), resp.Key(newkey))
}

// RenameNX renames a key only if the new key does not exist
func (p *Pipeline) RenameNX(key, newkey string) {
	p.do("RENAMENX", resp.Key(key), resp.Key(newkey))
}

// Restore creates a key using the provided serialized value, previously obtained using DUMP
func (p *Pipeline) Restore(key string, ttl time.Duration, data []byte, replace bool, idletime int64, frequency int64) {
	args := []resp.Arg{
		resp.Key(key),
		resp.Int(int64(ttl / time.Second)),
		resp.Raw(data),
	}
	if replace {
		args = append(args, resp.String("REPLACE"))
	}
	if idletime > 0 {
		args = append(args, resp.String("IDLETIME"), resp.Int(idletime))
	}
	if frequency >= 0 {
		args = append(args, resp.String("FREQ"), resp.Int(frequency))
	}
	p.do("RESTORE", args...)
}

// Sort options for SORT command
type Sort struct {
	By            string
	Offset, Count int64
	Get           []string
	Alpha         bool
	Desc          bool
	Store         string
}

// Sort sorts the elements in a list, set or sorted set
func (p *Pipeline) Sort(key string, options Sort) {
	args := []resp.Arg{
		resp.Key(key),
	}
	if options.By != "" {
		args = append(args, resp.String("BY"), resp.String(options.By))
	}
	args = limit(args, options.Offset, options.Count)
	for _, pattern := range options.Get {
		args = append(args, resp.String("GET"), resp.String(pattern))
	}
	if options.Desc {
		args = append(args, resp.String("DESC"))
	}
	if options.Alpha {
		args = append(args, resp.String("ALPHA"))
	}
	if options.Store != "" {
		args = append(args, resp.String("STORE"), resp.Key(options.Store))
	}
	p.do("SORT", args...)

}

// Touch alters the last access time of a key
func (p *Pipeline) Touch(keys ...string) {
	p.Command("TOUCH", len(keys))
	for _, key := range keys {
		p.Arg(resp.Key(key))
	}
}

// TTL gets the time to live for a key in seconds
func (p *Pipeline) TTL(key string) {
	p.do("TTL", resp.Key(key))
}

// Type determines the type stored at key
func (p *Pipeline) Type(key string) {
	p.do("TYPE", resp.Key(key))
}

// Unlink deletes a key asyncronously in another thread.
func (p *Pipeline) Unlink(keys ...string) {
	p.Command("UNLINK", len(keys))
	for _, key := range keys {
		p.Arg(resp.Key(key))
	}
}

// Wait waits for the synchronous replication of all the write commands sent in the context of the current connection
func (p *Pipeline) Wait(replicas int64, timeout time.Duration) {
	p.do("WAIT", resp.Int(replicas), resp.Int(int64(timeout/time.Second)))
}

const defaultScanCount = 10

// Scan incrementally iterates the keyspace
func (p *Pipeline) Scan(cur int64, match string, count int64) {
	if count <= 0 {
		count = defaultScanCount
	}
	if match == "" {
		p.do("SCAN", resp.Int(cur), resp.String("COUNT"), resp.Int(count))
	} else {
		p.do("SCAN", resp.Int(cur), resp.String("MATCH"), resp.String(match), resp.String("COUNT"), resp.Int(count))
	}
}

// Lists

func (p *Pipeline) BLPop(timeout time.Duration, keys ...string) {
	p.Command("BLPOP", 1+len(keys))
	for _, key := range keys {
		p.Arg(resp.Key(key))
	}
	p.Arg(resp.Int(int64(timeout / time.Second)))
}

func (p *Pipeline) BRPop(timeout time.Duration, keys ...string) {
	p.Command("BRPOP", 1+len(keys))
	for _, key := range keys {
		p.Arg(resp.Key(key))
	}
	p.Arg(resp.Int(int64(timeout / time.Second)))
}
func (p *Pipeline) BRPopLPush(src, dest string, timeout time.Duration) {
	p.do("BRPOPLPUSH", resp.Key(src), resp.Key(dest), resp.Int(int64(timeout/time.Second)))
}
func (p *Pipeline) LIndex(key string, index int64) {
	p.do("LINDEX", resp.Key(key), resp.Int(index))
}
func (p *Pipeline) LInsertBefore(key string, pivot int64, value resp.Arg) {
	p.do("LINSERT", resp.Key(key), resp.String("BEFORE"), resp.Int(pivot), value)
}
func (p *Pipeline) LInsertAfter(key string, pivot int64, value resp.Arg) {
	p.do("LINSERT", resp.Key(key), resp.String("AFTER"), resp.Int(pivot), value)
}
func (p *Pipeline) LLen(key string) {
	p.do("LLEN", resp.Key(key))
}
func (p *Pipeline) LPop(key string) {
	p.do("LPOP", resp.Key(key))
}
func (p *Pipeline) LPush(key string) {
	p.do("LPUSH", resp.Key(key))
}
func (p *Pipeline) LPushX(key string) {
	p.do("LPUSHX", resp.Key(key))
}
func (p *Pipeline) LRange(key string, start, stop int64) {
	p.do("LRANGE", resp.Key(key), resp.Int(start), resp.Int(stop))
}
func (p *Pipeline) LRem(key string, count int64, value resp.Arg) {
	p.do("LREM", resp.Key(key), resp.Int(count), value)
}
func (p *Pipeline) LSet(key string, index int64, value resp.Arg) {
	p.do("LSET", resp.Key(key), resp.Int(index), value)
}
func (p *Pipeline) LTrim(key string, start, stop int64) {
	p.do("LTRIM", resp.Key(key), resp.Int(start), resp.Int(stop))
}
func (p *Pipeline) RPop(key string) {
	p.do("RPOP", resp.Key(key))
}
func (p *Pipeline) RPopLPush(src, dest string) {
	p.do("RPOPLPUSH", resp.Key(src), resp.Key(dest))
}
func (p *Pipeline) RPush(key string, values ...resp.Arg) {
	p.Command("RPUSH", len(values)+1)
	p.Arg(resp.Key(key))
	for _, v := range values {
		p.Arg(v)
	}
}
func (p *Pipeline) RPushX(key string, value resp.Arg) {
	p.do("RPUSHX", resp.Key(key), value)
}

// Pub/Sub
// TODO: [commands] Pub/Sub commands

// Scripting

// Eval executes a Lua script server side
func (p *Pipeline) Eval(script string, keysAndArgs ...resp.Arg) {
	p.Command("EVAL", len(keysAndArgs)+2) // Script + NumKeys
	p.Arg(resp.String(script))
	keys, _ := splitKeysArgs(keysAndArgs)
	p.Arg(resp.Int(int64(len(keys))))
	for _, a := range keysAndArgs {
		p.Arg(a)
	}
}

// EvalSHA executes a cached Lua script server side
func (p *Pipeline) EvalSHA(sha1 string, keysAndArgs ...resp.Arg) {
	p.Command("EVALSHA", len(keysAndArgs)+2)
	p.Arg(resp.String(sha1))
	keys, _ := splitKeysArgs(keysAndArgs)
	numKeys := int64(len(keys))
	p.Arg(resp.Int(numKeys))
	p.Arg(keysAndArgs...)
}

func splitKeysArgs(keysAndArgs []resp.Arg) (keys, args []resp.Arg) {
	for i := range keysAndArgs {
		a := &keysAndArgs[i]
		if !a.IsKey() {
			return keysAndArgs[:i], keysAndArgs[i:]
		}
	}
	return keysAndArgs, nil
}

// ScriptExists checks existence of scripts in the script cache
func (p *Pipeline) ScriptExists(sha1 ...string) {
	p.Command("SCRIPT", 1+len(sha1))
	p.BulkString("EXISTS")
	for _, s := range sha1 {
		p.BulkString(s)
	}
}

// ScriptDebugSync sets the debug mode for executed scripts to SYNC
func (p *Pipeline) ScriptDebugSync() {
	p.do("SCRIPT", resp.String("DEBUG"), resp.String("SYNC"))
}

// ScriptDebug sets the debug mode for executed scripts to YES/NO
func (p *Pipeline) ScriptDebug(debug bool) {
	if debug {
		p.do("SCRIPT", resp.String("DEBUG"), resp.String("YES"))
	} else {
		p.do("SCRIPT", resp.String("DEBUG"), resp.String("NO"))
	}
}

// ScriptFlush removes all the scripts from the script cache
func (p *Pipeline) ScriptFlush() {
	p.do("SCRIPT", resp.String("FLUSH"))
}

// ScriptKill kills the script currently in execution
func (p *Pipeline) ScriptKill() {
	p.do("SCRIPT", resp.String("KILL"))
}

// ScriptLoad loads the specified Lua script into the script cache
func (p *Pipeline) ScriptLoad(script string) {
	p.do("SCRIPT", resp.String("LOAD"), resp.String(script))
}

// Server
// TODO: [commands] Server

func (p *Pipeline) BGRewriteAOF() {
	p.do("BGREWRITEAOF")
}
func (p *Pipeline) BGSave() {
	p.do("BGSAVE")
}

func (p *Pipeline) FlushDB() {
	p.do("FLUSHDB")
}

// Sets

func (p *Pipeline) SAdd(key string, members ...resp.Arg) {
	p.Command("SADD", 1+len(members))
	p.Arg(resp.Key(key))
	p.Arg(members...)
}

func (p *Pipeline) SCard(key string) {
	p.do("SCARD", resp.Key(key))
}
func (p *Pipeline) SDiff(keys ...string) {
	p.Command("SDIFF", len(keys))
	for _, k := range keys {
		p.Arg(resp.Key(k))
	}
}
func (p *Pipeline) SDiffStore(dest string, keys ...string) {
	p.Command("SDIFFSTORE", 1+len(keys))
	p.Arg(resp.Key(dest))
	for _, k := range keys {
		p.Arg(resp.Key(k))
	}
}
func (p *Pipeline) SInter(keys ...string) {
	p.Command("SINTER", len(keys))
	for _, k := range keys {
		p.Arg(resp.Key(k))
	}
}
func (p *Pipeline) SInterStore(dest string, keys ...string) {
	p.Command("SINTERSTORE", 1+len(keys))
	p.Arg(resp.Key(dest))
	for _, k := range keys {
		p.Arg(resp.Key(k))
	}
}
func (p *Pipeline) SIsMember(key, member string) {
	p.do("SISMEMBER", resp.Key(key), resp.String(member))
}
func (p *Pipeline) SMembers(key string) {
	p.do("SMEMBERS", resp.Key(key))
}
func (p *Pipeline) SMove(src, dest, member string) {
	p.do("SMOVE", resp.Key(src), resp.Key(dest), resp.String(member))
}
func (p *Pipeline) SPop(key string, count int64) {
	if count > 0 {
		p.do("SPOP", resp.Key(key), resp.Int(count))
	} else {
		p.do("SPOP", resp.Key(key))
	}
}
func (p *Pipeline) SRandMember(key string, count int64) {
	if count > 0 {
		p.do("SRANDMEMBER", resp.Key(key), resp.Int(count))
	} else {
		p.do("SRANDMEMBER", resp.Key(key))
	}
}
func (p *Pipeline) SRem(key string, members ...resp.Arg) {
	p.Command("SREM", 1+len(members))
	p.Arg(resp.Key(key))
	p.Arg(members...)
}
func (p *Pipeline) SUnion(keys ...string) {
	p.Command("SUNION", len(keys))
	for _, k := range keys {
		p.Arg(resp.Key(k))
	}
}
func (p *Pipeline) SUnionStore(dest string, keys ...string) {
	p.Command("SUNIONSTORE", 1+len(keys))
	p.Arg(resp.Key(dest))
	for _, k := range keys {
		p.Arg(resp.Key(k))
	}
}

func (p *Pipeline) SScan(key string, cur int64, match string, count int64) {
	if count <= 0 {
		count = defaultScanCount
	}
	if match == "" {
		p.do("SSCAN", resp.Key(key), resp.Int(cur), resp.String("COUNT"), resp.Int(count))
	} else {
		p.do("SSCAN", resp.Key(key), resp.Int(cur), resp.String("MATCH"), resp.String(match), resp.String("COUNT"), resp.Int(count))
	}
}

// Sorted Sets

// ZMember is a score/value pair
type ZMember struct {
	Score  float64
	Member string
}

// Z creates a new ZMember
func Z(score float64, member string) ZMember {
	return ZMember{Score: score, Member: member}
}

// SetMode determines the update mode for SET command
type SetMode uint

// SetMode enum
const (
	_ SetMode = iota
	NX
	XX
)

// ZAdd adds score/value pairs to a sorted set
func (p *Pipeline) ZAdd(key string, add SetMode, changed bool, members ...ZMember) {
	numArgs := 1
	switch add {
	case NX, XX:
		numArgs++

	}
	if changed {
		numArgs++
	}
	numArgs += 2 * len(members)
	p.Command("ZADD", numArgs)
	p.Arg(resp.Key(key))
	switch add {
	case NX:
		p.BulkString("NX")
	case XX:
		p.BulkString("XX")
	}
	if changed {
		p.BulkString("CH")
	}
	for i := range members {
		m := &members[i]
		p.Arg(resp.Float(m.Score), resp.String(m.Member))
	}
}

// ZCard gets the number of members in a sorted set
func (p *Pipeline) ZCard(key string) {
	p.do("ZCARD", resp.Key(key))
}

// ZCount counts the number of members in a sorted set with scores within the given values
func (p *Pipeline) ZCount(key string, min, max float64) {
	p.do("ZCOUNT", resp.Key(key), resp.Float(min), resp.Float(max))
}

// ZIncrBy increments the score of a member in a sorted set
func (p *Pipeline) ZIncrBy(key string, inc float64, member string) {
	p.do("ZINCRBY", resp.Key(key), resp.Float(inc), resp.String(member))
}

// ZIncrByNX increments the score of a member in a sorted set, only if the member does not exist in the set
func (p *Pipeline) ZIncrByNX(key string, inc float64, member string) {
	p.do("ZADD", resp.Key(key), resp.String("NX"), resp.String("INCR"), resp.Float(inc), resp.String(member))
}

// ZIncrByXX increments the score of a member in a sorted set, only if the member already exists in the set
func (p *Pipeline) ZIncrByXX(key string, inc float64, member string) {
	p.do("ZADD", resp.Key(key), resp.String("XX"), resp.String("INCR"), resp.Float(inc), resp.String(member))
}

func (p *Pipeline) zstore(cmd string, dest string, keysAndWeights ...resp.Arg) {
	keys, weights := splitKeysArgs(keysAndWeights)
	if len(weights) > 0 {
		p.Command("ZINTERSTORE", 3+2*len(keys))
		p.Arg(resp.Key(dest))
		p.Arg(keys...)
		p.BulkString("WEIGHTS")
		p.Arg(weights...)
		return
	}
	p.Command(cmd, 2+2*len(keys))
	p.Arg(resp.Key(dest))
	p.Arg(keys...)

}

// ZInterStore intersects multiple sorted sets and stores the resulting sorted set in a new key
func (p *Pipeline) ZInterStore(dest string, keysAndWeights ...resp.Arg) {
	p.zstore("ZINTERSTORE", dest, keysAndWeights...)
}

// ZLexCount counts the number of members in a sorted set between a given lexicographical range
func (p *Pipeline) ZLexCount(key, min, max string) {
	p.do("ZLEXCOUNT", resp.Key(key), resp.String(min), resp.String(max))
}

// ZPopMax removes and returns members with the highest scores in a sorted set
func (p *Pipeline) ZPopMax(key string, count int64) {
	if count > 0 {
		p.do("ZPOPMAX", resp.Key(key), resp.Int(count))
	} else {
		p.do("ZPOPMAX", resp.Key(key))
	}
}

// ZPopMin removes and returns members with the lowest scores in a sorted set
func (p *Pipeline) ZPopMin(key string, count int64) {
	if count > 0 {
		p.do("ZPOPMIN", resp.Key(key), resp.Int(count))
	} else {
		p.do("ZPOPMIN", resp.Key(key))
	}
}

// ZRange returns a range of members in a sorted set, by index
func (p *Pipeline) ZRange(key string, start, stop int64, scores bool) {
	if scores {
		p.do("ZRANGE", resp.Key(key), resp.Int(start), resp.Int(stop), resp.String("WITHSCORES"))
	} else {
		p.do("ZRANGE", resp.Key(key), resp.Int(start), resp.Int(stop))

	}
}

// ZRangeByLex returns a range of members in a sorted set, by lexicographical range
func (p *Pipeline) ZRangeByLex(key, min, max string, offset, count int64) {
	args := []resp.Arg{
		resp.Key(key),
		resp.String(min),
		resp.String(max),
	}
	args = limit(args, offset, count)
	p.do("ZRANGEBYLEX", args...)
}
func limit(args []resp.Arg, offset, count int64) []resp.Arg {
	if count == 0 && offset == 0 {
		return args
	}
	return append(args,
		resp.String("LIMIT"),
		resp.Int(offset),
		resp.Int(count),
	)
}

// ZRangeByScore returns a range of members in a sorted set, by score
func (p *Pipeline) ZRangeByScore(key string, min, max float64, scores bool, offset, count int64) {
	args := []resp.Arg{
		resp.Key(key),
		resp.Float(min),
		resp.Float(max),
	}
	if scores {
		args = append(args, resp.String("WITHSCORES"))
	}
	args = limit(args, offset, count)
	p.do("ZRANGEBYSCORE", args...)

}

// ZRank determines the index of a member in a sorted set
func (p *Pipeline) ZRank(key, member string) {
	p.do("ZRANK", resp.Key(key), resp.String(member))
}

// ZRem removes one or more members from a sorted set
func (p *Pipeline) ZRem(key string, members ...resp.Arg) {
	p.Command("ZREM", 1+len(members))
	p.Arg(resp.Key(key))
	p.Arg(members...)
}

// ZRemRangeByLex removes a range of members in a sorted set, between the given lexicographical range
func (p *Pipeline) ZRemRangeByLex(key, min, max string) {
	p.do("ZREMRANGEBYLEX", resp.Key(key), resp.String(min), resp.String(max))
}

// ZRemRangeByRank removes a range of members in a sorted set, within the given indexes
func (p *Pipeline) ZRemRangeByRank(key string, start, stop int64) {
	p.do("ZREMRANGEBYRANK", resp.Key(key), resp.Int(start), resp.Int(stop))
}

// ZRemRangeByScore removes a range of members in a sorted set, within the given scores
func (p *Pipeline) ZRemRangeByScore(key string, min, max float64) {
	p.do("ZREMRANGEBYSCORE", resp.Key(key), resp.Float(min), resp.Float(max))
}

// ZRevRank determines the index of a member in a sorted set, with scores ordered from high to low
func (p *Pipeline) ZRevRank(key, member string) {
	p.do("ZREVRANK", resp.Key(key), resp.String(member))
}

// ZScore gets the score associated with the given member in a sorted set
func (p *Pipeline) ZScore(key, member string) {
	p.do("ZSCORE", resp.Key(key), resp.String(member))
}

// ZUnionStore adds multiple sorted sets and stores the resulting sorted set in a new key
func (p *Pipeline) ZUnionStore(dest string, keysAndWeights ...resp.Arg) {
	p.zstore("ZUNIONSTORE", dest, keysAndWeights...)
}

// ZScan incrementally iterates sorted set's elements and associated scores
func (p *Pipeline) ZScan(key string, cur int64, match string, count int64) {
	if count <= 0 {
		count = defaultScanCount
	}
	if match == "" {
		p.do("ZSCAN", resp.Key(key), resp.Int(cur), resp.String("COUNT"), resp.Int(count))
	} else {
		p.do("ZSCAN", resp.Key(key), resp.Int(cur), resp.String("MATCH"), resp.String(match), resp.String("COUNT"), resp.Int(count))
	}
}

// Strings

func (p *Pipeline) Append(key string, value resp.Arg) {
	p.do("APPEND", resp.Key(key), value)
}
func (p *Pipeline) BitCount(key string, start, end int64) {
	p.do("BITCOUNT", resp.Key(key), resp.Int(start), resp.Int(end))
}

// TODO: [commands] BITFIELD

func (p *Pipeline) BitAND(dest string, src ...string) {
	p.bitop("AND", dest, src...)
}
func (p *Pipeline) BitOR(dest string, src ...string) {
	p.bitop("OR", dest, src...)
}
func (p *Pipeline) BitXOR(dest string, src ...string) {
	p.bitop("XOR", dest, src...)
}
func (p *Pipeline) BitNOT(dest, src string) {
	p.bitop("NOT", dest, src)
}
func (p *Pipeline) bitop(op, key string, keys ...string) {
	p.Command("BITOP", 2+len(keys))
	p.Arg(resp.String(op))
	p.Arg(resp.Key(key))
	for _, k := range keys {
		p.Arg(resp.Key(k))
	}
}
func (p *Pipeline) BitPos(key string, bit uint, startEnd ...int64) {
	if bit != 0 {
		bit = 1
	}
	switch len(startEnd) {
	case 0:
		p.do("BITPOS", resp.Key(key), resp.Int(int64(bit)))
	case 1:
		p.do("BITPOS", resp.Key(key), resp.Int(int64(bit)), resp.Int(startEnd[0]))
	default:
		p.do("BITPOS", resp.Key(key), resp.Int(int64(bit)), resp.Int(startEnd[0]), resp.Int(startEnd[1]))
	}
}
func (p *Pipeline) Decr(key string) {
	p.do("DECR", resp.Key(key))
}
func (p *Pipeline) DecrBy(key string, d int64) {
	p.do("DECRBY", resp.Key(key), resp.Int(d))
}
func (p *Pipeline) Get(key string) {
	p.do("GET", resp.Key(key))
}
func (p *Pipeline) GetBit(key string, offset int64) {
	p.do("GETBIT", resp.Key(key), resp.Int(offset))
}
func (p *Pipeline) GetRange(key string, start, end int64) {
	p.do("GETRANGE", resp.Key(key), resp.Int(start), resp.Int(end))
}
func (p *Pipeline) GetSet(key string, value resp.Arg) {
	p.do("GETSET", resp.Key(key), value)
}
func (p *Pipeline) Incr(key string) {
	p.do("INCR", resp.Key(key))
}
func (p *Pipeline) IncrBy(key string, d int64) {
	p.do("INCRBY", resp.Key(key), resp.Int(d))
}
func (p *Pipeline) IncrByFloat(key string, f float64) {
	p.do("INCRBYFLOAT", resp.Key(key), resp.Float(f))
}
func (p *Pipeline) MGet(keys ...string) {
	p.Command("MGET", len(keys))
	for _, key := range keys {
		p.Arg(resp.Key(key))
	}
}
func (p *Pipeline) MSet(pairs ...resp.KV) {
	p.Command("MSET", len(pairs)*2+1)
	for _, kv := range pairs {
		p.Arg(resp.Key(kv.Key))
		p.Arg(kv.Arg)
	}
}
func (p *Pipeline) MSetNX(pairs ...resp.KV) {
	p.Command("MSETNX", len(pairs)*2+1)
	for _, kv := range pairs {
		p.Arg(resp.Key(kv.Key))
		p.Arg(kv.Arg)
	}
}

func (p *Pipeline) Set(key string, value resp.Arg, ttl time.Duration) {
	ttl /= time.Millisecond
	if ttl > 0 {
		p.do("SET", resp.Key(key), value, resp.String("PX"), resp.Int(int64(ttl)))
	} else {
		p.do("SET", resp.Key(key), value)
	}
}

func (p *Pipeline) SetNX(key string, value resp.Arg, ttl time.Duration) {
	ttl /= time.Millisecond
	if ttl > 0 {
		p.do("SET", resp.Key(key), value, resp.String("PX"), resp.Int(int64(ttl)), resp.String("NX"))
	} else {
		p.do("SET", resp.Key(key), value, resp.String("NX"))
	}
}

func (p *Pipeline) SetXX(key string, value resp.Arg, ttl time.Duration) {
	ttl /= time.Millisecond
	if ttl > 0 {
		p.do("SET", resp.Key(key), value, resp.String("PX"), resp.Int(int64(ttl)), resp.String("XX"))
	} else {
		p.do("SET", resp.Key(key), value, resp.String("XX"))
	}
}
func (p *Pipeline) SetRange(key string, offset int64, value resp.Arg) {
	p.do("SETRANGE", resp.Key(key), resp.Int(offset), value)
}

func (p *Pipeline) StrLen(key string) {
	p.do("STRLEN", resp.Key(key))
}

// Transactions

// Discard discards all commands issued after MULTI
func (p *Pipeline) Discard() {
	p.do("DISCARD")
}

// Exec executes all commands issued after MULTI
func (p *Pipeline) Exec() {
	p.do("EXEC")
}

// Multi marks the start of a transaction block
func (p *Pipeline) Multi() {
	p.do("MULTI")
}

// Unwatch forgets about all watched keys
func (p *Pipeline) Unwatch() {
	p.do("UNWATCH")
}

// Watch watches the given keys to determine execution of the MULTI/EXEC block
func (p *Pipeline) Watch(keys ...string) {
	p.Command("WATCH", len(keys))
	for _, k := range keys {
		p.Arg(resp.Key(k))
	}
}
