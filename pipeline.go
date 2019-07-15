package redis

import (
	"sync"

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

func (p *Pipeline) do(cmd string, args ...resp.Arg) {
	p.Command(cmd, len(args))
	p.Buffer.Arg(args...)
}

func (p *Pipeline) Command(cmd string, numArgs int) {
	p.Buffer.Array(numArgs + 1)
	p.Buffer.BulkString(cmd)
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

// ReleasePipieline resets and returns the pipeline to the pool
func ReleasePipeline(p *Pipeline) {
	if p != nil {
		p.Reset()
		pipelinePool.Put(p)
	}

}
