package redis

import (
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/alxarch/fastredis/resp"
)

// Pipeline is a command buffer.
type Pipeline struct {
	buf     []byte
	scratch []byte
	n       int64
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

// Size returns the size of the pipeline in bytes
func (p *Pipeline) Size() int {
	return len(p.buf)
}

// Len returns the number of commands in the pipeline
func (p *Pipeline) Len() int64 {
	return p.n
}

// Reset resets the pipeline buffer.
func (p *Pipeline) Reset() {
	p.buf = p.buf[:0]
	p.n = 0
}

func (p *Pipeline) appendArr(n int) {
	p.buf = resp.AppendArray(p.buf, n)
}
func (p *Pipeline) cmd(cmd string, args ...Arg) {
	p.buf = resp.AppendArray(p.buf, len(args)+1)
	p.appendArg(String(cmd))
	for _, a := range args {
		p.appendArg(a)
	}
	p.n++
}

func (p *Pipeline) appendArg(a Arg) {
	switch a.typ {
	case typString, typKey:
		p.buf = resp.AppendBulkString(p.buf, a.str)
	case typBuffer:
		p.buf = resp.AppendBulkStringRaw(p.buf, a.buf)
	case typInt:
		p.scratch = strconv.AppendInt(p.scratch[:0], int64(a.num), 10)
		p.buf = resp.AppendBulkStringRaw(p.buf, p.scratch)
	case typFloat:
		p.scratch = strconv.AppendFloat(p.scratch, math.Float64frombits(a.num), 'f', -1, 64)
		p.buf = resp.AppendBulkStringRaw(p.buf, p.scratch)
	case typUint:
		p.scratch = strconv.AppendUint(p.scratch, a.num, 10)
		p.buf = resp.AppendBulkStringRaw(p.buf, p.scratch)
	case typTrue:
		p.buf = resp.AppendBulkString(p.buf, "true")
	case typFalse:
		p.buf = resp.AppendBulkString(p.buf, "false")
	default:
		p.buf = resp.AppendNullBulkString(p.buf)
	}
}

func (p *Pipeline) HIncrBy(key, field string, n int64) {
	p.cmd("HINCRBY", Key(key), String(field), Int(n))
}
func (p *Pipeline) HIncrByFloat(key, field string, f float64) {
	p.cmd("HINCRBYFLOAT", Key(key), String(field), Float(f))
}

func (p *Pipeline) HSet(key, field string, value Arg) {
	p.cmd("HSET", Key(key), String(field), value)
}
func (p *Pipeline) HGet(key, field string) {
	p.cmd("HSET", Key(key), String(field))
}

func (p *Pipeline) Expire(key string, ttl time.Duration) {
	p.cmd("PEXPIRE", Key(key), Int(int64(ttl/time.Millisecond)))
}

func (p *Pipeline) HSetNX(key, field string, value Arg) {
	p.cmd("HSETNX", Key(key), String(field), value)
}

func (p *Pipeline) FlushDB() {
	p.cmd("FLUSHDB")
}
func (p *Pipeline) Select(db int64) {
	p.cmd("SELECT", Int(db))
}

func (p *Pipeline) Set(key string, value Arg, ttl time.Duration) {
	ttl /= time.Millisecond
	if ttl > 0 {
		p.cmd("SET", Key(key), value, String("PX"), Int(int64(ttl)))
	} else {
		p.cmd("SET", Key(key), value)
	}
}

func (p *Pipeline) SetNX(key string, value Arg, ttl time.Duration) {
	ttl /= time.Millisecond
	if ttl > 0 {
		p.cmd("SET", Key(key), value, String("PX"), Int(int64(ttl)), String("NX"))
	} else {
		p.cmd("SET", Key(key), value, String("NX"))
	}
}

func (p *Pipeline) SetXX(key string, value Arg, ttl time.Duration) {
	ttl /= time.Millisecond
	if ttl > 0 {
		p.cmd("SET", Key(key), value, String("PX"), Int(int64(ttl)), String("XX"))
	} else {
		p.cmd("SET", Key(key), value, String("XX"))
	}
}

func (p *Pipeline) Get(key string) {
	p.cmd("GET", Key(key))
}
func (p *Pipeline) MSet(pairs ...KV) {
	p.appendArr(len(pairs)*2 + 1)
	p.appendArg(String("MSET"))
	for _, pair := range pairs {
		p.appendArg(Key(pair.Key))
		p.appendArg(pair.Arg)
	}
	p.n++
}
func (p *Pipeline) MGet(keys ...string) {
	p.appendArr(len(keys) + 1)
	p.appendArg(String("MGET"))
	for _, key := range keys {
		p.appendArg(Key(key))
	}
	p.n++
}
func (p *Pipeline) Del(keys ...string) {
	p.appendArr(len(keys) + 1)
	p.appendArg(String("DEL"))
	for _, key := range keys {
		p.appendArg(Key(key))
	}
	p.n++
}

func (p *Pipeline) Keys(match string) {
	p.cmd("KEYS", String(match))
}

const defaultScanCount = 10

func (p *Pipeline) Scan(cur int64, match string, count int64) {
	if count <= 0 {
		count = defaultScanCount
	}
	if match == "" {
		p.cmd("SCAN", Int(cur), String("COUNT"), Int(count))
	} else {
		p.cmd("SCAN", Int(cur), String("MATCH"), String(match), String("COUNT"), Int(count))
	}
}

func (p *Pipeline) SScan(key string, cur int64, match string, count int64) {
	if count <= 0 {
		count = defaultScanCount
	}
	if match == "" {
		p.cmd("SSCAN", Int(cur), String(key), String("COUNT"), Int(count))
	} else {
		p.cmd("SSCAN", String(key), Int(cur), String("MATCH"), String(match), String("COUNT"), Int(count))
	}
}

func (p *Pipeline) HScan(key string, cur int64, match string, count int64) {
	if count <= 0 {
		count = defaultScanCount
	}
	if match == "" {
		p.cmd("HSCAN", Int(cur), String(key), String("COUNT"), Int(count))
	} else {
		p.cmd("HSCAN", String(key), Int(cur), String("MATCH"), String(match), String("COUNT"), Int(count))
	}
}

func (p *Pipeline) ZScan(key string, cur int64, match string, count int64) {
	if count <= 0 {
		count = defaultScanCount
	}
	if match == "" {
		p.cmd("ZSCAN", Int(cur), String(key), String("COUNT"), Int(count))
	} else {
		p.cmd("ZSCAN", String(key), Int(cur), String("MATCH"), String(match), String("COUNT"), Int(count))
	}
}

func (p *Pipeline) Eval(script string, keysAndArgs ...Arg) {
	p.appendArr(len(keysAndArgs) + 3)
	p.appendArg(String("EVAL"))
	p.appendArg(String(script))
	p.appendEval(keysAndArgs)
}

func (p *Pipeline) EvalSHA(s *Script, keysAndArgs ...Arg) {
	p.appendArr(len(keysAndArgs) + 3)
	p.appendArg(String("EVALSHA"))
	p.appendArg(Raw(s.sha1[:]))
	p.appendEval(keysAndArgs)
}

func (p *Pipeline) appendEval(keysAndArgs []Arg) {
	keys := keysAndArgs
	for i := range keys {
		if keys[i].typ != typKey {
			keys = keys[:i]
			break
		}
	}
	p.appendArg(Int(int64(len(keys))))
	for _, a := range keysAndArgs {
		p.appendArg(a)
	}
	p.n++

}
