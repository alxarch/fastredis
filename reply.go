package redis

import (
	"sync"

	"github.com/alxarch/fastredis/resp"
)

// replyPool is a pool of replies
var replyPool sync.Pool

// BlankReply returns a blank reply from the pool
func BlankReply() *resp.Reply {
	x := replyPool.Get()
	if x == nil {
		return new(resp.Reply)
	}
	return x.(*resp.Reply)
}

// ReleaseReply resets and returns a Reply to the pool.
func ReleaseReply(r *resp.Reply) {
	if r != nil {
		r.Reset()
		replyPool.Put(r)
	}
}
