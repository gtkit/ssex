package ssex

import "time"

// defaultWriteTimeout 是单帧写入的默认截止时长。
const defaultWriteTimeout = 10 * time.Second

// Option 配置 Writer / Stream 的可选行为。
type Option func(*options)

type options struct {
	writeTimeout time.Duration
}

// WithWriteTimeout 设置单帧写入的截止时长，默认 10s；非正值被忽略。
//
// 该截止时间只作用于单帧：客户端读取过慢导致某一帧写入超时时，
// 这一次写入返回错误（不会被判定为 ErrClientGone，慢客户端不等于断开），
// 长连接整体不受此值限制。弱网移动端可适当放大，内网可收紧以更快发现死连接。
func WithWriteTimeout(d time.Duration) Option {
	return func(o *options) {
		if d <= 0 {
			return
		}
		o.writeTimeout = d
	}
}

// newOptions 应用 Option 并回落到默认值。
func newOptions(opts []Option) options {
	o := options{writeTimeout: defaultWriteTimeout}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
