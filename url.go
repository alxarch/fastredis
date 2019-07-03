package redis

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type addr string

func (addr) Network() string {
	return "tcp"
}
func (a addr) String() string {
	return string(a)
}

func ParseURL(rawurl string) (o PoolOptions, err error) {
	o = DefaultPoolOptions()
	if rawurl == "" {
		return
	}
	u, err := url.Parse(rawurl)
	if err != nil {
		return
	}
	if u.Scheme != "redis" {
		err = fmt.Errorf(`Invalid URL scheme %q`, u.Scheme)
		return
	}
	switch n := strings.Trim(u.Path, "/"); n {
	case "", "0":
	default:
		err = fmt.Errorf(`Invalid URL path %q`, u.Path)
		return
	}
	q := u.Query()
	host, port := u.Hostname(), u.Port()
	if port == "" {
		port = "6379"
	}
	o.Address = addr(host + ":" + port)

	if v, ok := q["read-timeout"]; ok && len(v) > 0 {
		if d, _ := time.ParseDuration(v[0]); d > 0 {
			o.ReadTimeout = d
		}
	}
	if v, ok := q["write-timeout"]; ok && len(v) > 0 {
		if d, _ := time.ParseDuration(v[0]); d > 0 {
			o.WriteTimeout = d
		}
	}
	if v, ok := q["read-buffer-size"]; ok && len(v) > 0 {
		if size, _ := strconv.Atoi(v[0]); size > 0 {
			o.ReadBufferSize = size
		}
	}

	if v, ok := q["max-conn-age"]; ok && len(v) > 0 {
		if d, _ := time.ParseDuration(v[0]); d > 0 {
			o.MaxConnectionAge = d
		}
	}
	if v, ok := q["max-idle-time"]; ok && len(v) > 0 {
		if d, _ := time.ParseDuration(v[0]); d > 0 {
			o.MaxIdleTime = d
		}
	}
	if v, ok := q["check-idle-interval"]; ok && len(v) > 0 {
		if d, _ := time.ParseDuration(v[0]); d > 0 {
			o.CheckIdleInterval = d
		}
	}
	if v, ok := q["clock-frequency"]; ok && len(v) > 0 {
		if d, _ := time.ParseDuration(v[0]); d > 0 {
			o.ClockFrequency = d
		}
	}

	return
}
