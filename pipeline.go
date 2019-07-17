package redis

import (
	"sync"

	resp "github.com/alxarch/fastredis/resp"
)

// Pipeline is a command buffer
type Pipeline struct {
	resp.Buffer
	offset int
	n      int
}

// Reset resets a pipeline
func (p *Pipeline) Reset() {
	p.Buffer.Reset()
	p.n = 0
	p.offset = 0
}

// Len returns the number of commands in a pipeline
func (p *Pipeline) Len() int {
	return p.n - p.offset
}

// Size returns the size of the pipeline in bytes
func (p *Pipeline) Size() int {
	return len(p.Buffer.B)
}

func (p *Pipeline) do(cmd string, args ...resp.Arg) {
	p.Command(cmd, len(args))
	p.Buffer.Arg(args...)
}

// Command starts a new command with n number of args
func (p *Pipeline) Command(cmd string, numArgs int) {
	p.Buffer.Array(numArgs + 1)
	p.Buffer.BulkString(cmd)
	p.n++
}

var pipelinePool sync.Pool

// BlankPipeline gets a blank pipeline from the pool
func BlankPipeline(db int64) (p *Pipeline) {
	x := pipelinePool.Get()
	if x == nil {
		p = new(Pipeline)
	} else {
		p = x.(*Pipeline)
	}
	if db > 0 {
		p.Select(db)
		p.offset++
	}
	return p

}

// ReleasePipeline resets and returns the pipeline to the pool
func ReleasePipeline(p *Pipeline) {
	if p != nil {
		p.Reset()
		pipelinePool.Put(p)
	}

}
