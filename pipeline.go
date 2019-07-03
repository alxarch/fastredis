package redis

import (
	"sync"
	"time"

	resp "github.com/alxarch/fastredis/resp"
)

// Pipeline is a command buffer
type Pipeline struct {
	resp.Buffer
	n int
}

// Reset resets a pipeline
func (p *Pipeline) Reset() {
	p.Buffer.Reset()
	p.n = 0
}

// Len returns the number of commands in a pipeline
func (p *Pipeline) Len() int {
	return p.n
}

// Size returns the size of the pipeline in bytes
func (p *Pipeline) Size() int {
	return len(p.Buffer.B)
}

func (p *Pipeline) Do(cmd string, args ...resp.Arg) {
	p.Command(cmd, len(args))
	p.WriteArgs(args...)
}

func (p *Pipeline) Command(cmd string, numArgs int) {
	p.WriteArray(numArgs + 1)
	p.WriteBulkString(cmd)
	p.n++
}

var pipelinePool sync.Pool

func BlankPipeline() *Pipeline {
	x := pipelinePool.Get()
	if x == nil {
		return new(Pipeline)
	}
	return x.(*Pipeline)
}

func (p *Pipeline) Close() {
	if p != nil {
		p.Reset()
		pipelinePool.Put(p)
	}
}

func (p *Pipeline) HIncrBy(key, field string, n int64) {
	p.Do("HINCRBY", resp.Key(key), resp.String(field), resp.Int(n))
}
func (p *Pipeline) HIncrByFloat(key, field string, f float64) {
	p.Do("HINCRBYFLOAT", resp.Key(key), resp.String(field), resp.Float(f))
}

func (p *Pipeline) HSet(key, field string, value resp.Arg) {
	p.Do("HSET", resp.Key(key), resp.String(field), value)
}
func (p *Pipeline) HGet(key, field string) {
	p.Do("HSET", resp.Key(key), resp.String(field))
}

func (p *Pipeline) Expire(key string, ttl time.Duration) {
	p.Do("PEXPIRE", resp.Key(key), resp.Int(int64(ttl/time.Millisecond)))
}

func (p *Pipeline) HSetNX(key, field string, value resp.Arg) {
	p.Do("HSETNX", resp.Key(key), resp.String(field), value)
}

func (p *Pipeline) FlushDB() {
	p.Do("FLUSHDB")
}
func (p *Pipeline) Select(db int64) {
	p.Do("SELECT", resp.Int(db))
}

func (p *Pipeline) Set(key string, value resp.Arg, ttl time.Duration) {
	ttl /= time.Millisecond
	if ttl > 0 {
		p.Do("SET", resp.Key(key), value, resp.String("PX"), resp.Int(int64(ttl)))
	} else {
		p.Do("SET", resp.Key(key), value)
	}
}

func (p *Pipeline) SetNX(key string, value resp.Arg, ttl time.Duration) {
	ttl /= time.Millisecond
	if ttl > 0 {
		p.Do("SET", resp.Key(key), value, resp.String("PX"), resp.Int(int64(ttl)), resp.String("NX"))
	} else {
		p.Do("SET", resp.Key(key), value, resp.String("NX"))
	}
}

func (p *Pipeline) SetXX(key string, value resp.Arg, ttl time.Duration) {
	ttl /= time.Millisecond
	if ttl > 0 {
		p.Do("SET", resp.Key(key), value, resp.String("PX"), resp.Int(int64(ttl)), resp.String("XX"))
	} else {
		p.Do("SET", resp.Key(key), value, resp.String("XX"))
	}
}

func (p *Pipeline) Get(key string) {
	p.Do("GET", resp.Key(key))
}
func (p *Pipeline) MSet(pairs ...resp.KV) {
	p.Command("MSET", len(pairs)*2+1)
	for _, kv := range pairs {
		p.WriteArg(resp.Key(kv.Key))
		p.WriteArg(kv.Arg)
	}
}
func (p *Pipeline) MGet(keys ...string) {
	p.Command("MGET", len(keys))
	for _, key := range keys {
		p.WriteArg(resp.Key(key))
	}
}
func (p *Pipeline) Del(keys ...string) {
	p.Command("DEL", len(keys))
	for _, key := range keys {
		p.WriteArg(resp.Key(key))
	}
}

func (p *Pipeline) Keys(match string) {
	p.Do("KEYS", resp.String(match))
}

const defaultScanCount = 10

func (p *Pipeline) Scan(cur int64, match string, count int64) {
	if count <= 0 {
		count = defaultScanCount
	}
	if match == "" {
		p.Do("SCAN", resp.Int(cur), resp.String("COUNT"), resp.Int(count))
	} else {
		p.Do("SCAN", resp.Int(cur), resp.String("MATCH"), resp.String(match), resp.String("COUNT"), resp.Int(count))
	}
}

func (p *Pipeline) SScan(key string, cur int64, match string, count int64) {
	if count <= 0 {
		count = defaultScanCount
	}
	if match == "" {
		p.Do("SSCAN", resp.Int(cur), resp.String(key), resp.String("COUNT"), resp.Int(count))
	} else {
		p.Do("SSCAN", resp.String(key), resp.Int(cur), resp.String("MATCH"), resp.String(match), resp.String("COUNT"), resp.Int(count))
	}
}

func (p *Pipeline) HScan(key string, cur int64, match string, count int64) {
	if count <= 0 {
		count = defaultScanCount
	}
	if match == "" {
		p.Do("HSCAN", resp.Int(cur), resp.String(key), resp.String("COUNT"), resp.Int(count))
	} else {
		p.Do("HSCAN", resp.String(key), resp.Int(cur), resp.String("MATCH"), resp.String(match), resp.String("COUNT"), resp.Int(count))
	}
}

func (p *Pipeline) ZScan(key string, cur int64, match string, count int64) {
	if count <= 0 {
		count = defaultScanCount
	}
	if match == "" {
		p.Do("ZSCAN", resp.Int(cur), resp.String(key), resp.String("COUNT"), resp.Int(count))
	} else {
		p.Do("ZSCAN", resp.String(key), resp.Int(cur), resp.String("MATCH"), resp.String(match), resp.String("COUNT"), resp.Int(count))
	}
}

func (p *Pipeline) Eval(script string, keysAndArgs ...resp.Arg) {
	p.Command("EVAL", len(keysAndArgs)+2) // Script + NumKeys
	p.WriteArg(resp.String(script))
	p.eval(keysAndArgs)
}

func (p *Pipeline) EvalSHA(s *Script, keysAndArgs ...resp.Arg) {
	p.Command("EVALSHA", len(keysAndArgs)+2)
	p.WriteArg(resp.Raw(s.sha1[:]))
	p.eval(keysAndArgs)
}

func (p *Pipeline) eval(keysAndArgs []resp.Arg) {
	keys := keysAndArgs
	for i, k := range keys {
		if !k.IsKey() {
			keys = keys[:i]
			break
		}
	}
	p.WriteArg(resp.Int(int64(len(keys))))
	for _, a := range keysAndArgs {
		p.WriteArg(a)
	}

}
