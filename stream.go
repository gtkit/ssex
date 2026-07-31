package ssex

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Stream 是面向业务的 SSE 写入器。相比底层 Writer,Stream 额外:
//  1. 首个事件自动写入 SSE 响应头;
//  2. 跟踪响应是否已开始;
//  3. 提供统一的 ping / error 事件辅助方法;
//  4. 支持显式终止流(Close),终止后拒绝后续写入。
//
// 并发安全:Stream 用互斥锁串行化所有写方法,可从不同 goroutine
// (如心跳 goroutine + 业务 goroutine)并发调用。
type Stream struct {
	writer  *Writer
	mu      sync.Mutex
	started bool
	closed  bool
}

// NewStream 创建一个 SSE Stream；可用 Option 调整写入行为。
//
// gin 里这样调用：ssex.NewStream(c.Writer, c.Request)。
func NewStream(w http.ResponseWriter, r *http.Request, opts ...Option) *Stream {
	return &Stream{writer: New(w, r, opts...)}
}

// Start 显式启动 SSE 响应(写入响应头并立即刷给客户端)。
func (s *Stream) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startLocked()
}

// Event 发送一条命名 SSE 事件;响应尚未开始时自动先写 SSE 响应头。
func (s *Stream) Event(name string, payload any) error {
	return s.write(func(w *Writer) error { return w.Event(name, payload) })
}

// EventWithID 发送一条带 `id:` 字段的命名 SSE 事件(断线续传,见 Writer.EventWithID);
// 响应尚未开始时自动先写 SSE 响应头。
func (s *Stream) EventWithID(id, name string, payload any) error {
	return s.write(func(w *Writer) error { return w.EventWithID(id, name, payload) })
}

// Data 发送一条 data-only 帧(OpenAI 风格,见 Writer.Data);
// 响应尚未开始时自动先写 SSE 响应头。
func (s *Stream) Data(payload any) error {
	return s.write(func(w *Writer) error { return w.Data(payload) })
}

// Send 写出一条 Event，语义与逐字段调用一致：Name 为空时写 data-only 帧，
// ID 为空时省略 `id:` 行。供 Hub 的消费循环直接写出投递来的事件。
func (s *Stream) Send(e Event) error {
	return s.write(func(w *Writer) error { return w.writeEvent(e.ID, e.Name, e.Data) })
}

// Ping 发送一条标准保活注释帧。
func (s *Stream) Ping(at time.Time) error {
	return s.Comment("ping " + at.UTC().Format(time.RFC3339))
}

// Heartbeat 每隔 interval 发送一条保活注释帧，直到 ctx 取消或写入失败。
//
// 阻塞运行，由调用方决定在哪个 goroutine 启动（本包不隐式起 goroutine）：
//
//	go func() { _ = stream.Heartbeat(c.Request.Context(), 15*time.Second) }()
//
// 长时间无数据的流（等支付结果、等登录态）必须有心跳，否则代理层会按空闲连接断开。
// ctx 取消时返回 ctx 错误；客户端断开返回 ErrClientGone；流已终止返回 ErrStreamClosed。
// interval 非正时立即返回错误，不写出任何字节。
func (s *Stream) Heartbeat(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("ssex: heartbeat interval must be positive: %s", interval)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case at := <-ticker.C:
			if err := s.Ping(at); err != nil {
				return err
			}
		}
	}
}

// Error 发送一条标准业务 error 事件。
func (s *Stream) Error(payload any) error {
	return s.Event("error", payload)
}

// Comment 发送一条注释帧。
func (s *Stream) Comment(text string) error {
	return s.write(func(w *Writer) error { return w.Comment(text) })
}

// Retry 告知客户端建议的重连间隔(毫秒)。
func (s *Stream) Retry(milliseconds int) error {
	return s.write(func(w *Writer) error { return w.Retry(milliseconds) })
}

// Close 终止本流：发送一条名为 close 的事件，并拒绝后续所有写入（返回 ErrStreamClosed）。
//
// 为什么需要它：EventSource 在服务端正常结束流后会按重连间隔自动重连，
// 订单进入终态、LLM 输出结束后直接 return 会让前端反复重连。约定前端监听
// close 事件后调用 EventSource.close() 主动断开，这轮推送才真正结束。
//
// Close 只终结本流的写入许可，不关闭 HTTP 连接——连接在 handler 返回时结束。
// 即使终止事件写入失败（客户端已断开），流同样标记为已终止。重复调用返回 ErrStreamClosed。
func (s *Stream) Close(payload any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStreamClosed
	}
	s.startLocked()

	err := s.writer.Event("close", payload)
	s.closed = true

	return err
}

// Started 返回 SSE 响应是否已开始写入。
func (s *Stream) Started() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started
}

// Context 返回绑定到本 SSE 连接的请求上下文。
func (s *Stream) Context() context.Context {
	return s.writer.Context()
}

// write 在锁内完成"终止检查 → 自动起流 → 执行写入",是所有写方法的公共骨架。
func (s *Stream) write(fn func(*Writer) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStreamClosed
	}
	s.startLocked()

	return fn(s.writer)
}

func (s *Stream) startLocked() {
	if s.started {
		return
	}
	s.writer.WriteHeaders()
	s.started = true
}
