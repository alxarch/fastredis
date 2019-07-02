package resp

import "sync"

type Pipeline struct {
	Buffer
	n int
}

func (p *Pipeline) Reset() {
	p.Buffer.Reset()
	p.n = 0
}

func (p *Pipeline) Len() int {
	return p.n
}

func (p *Pipeline) Do(cmd string, args ...Arg) {
	p.WriteArray(len(args) + 1)
	p.WriteBulkString(cmd)
	for i := range args {
		a := &args[i]
		p.writeArg(a)
	}
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
	p.Reset()
	pipelinePool.Put(p)
}
