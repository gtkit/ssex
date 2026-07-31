package ssex

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// Stream 是面向业务的 SSE 写入器。相比底层 Writer,Stream 额外:
//  1. 首个事件自动提交 SSE 响应头,且帧构造失败时不提交(调用方仍可回普通 JSON);
//  2. 跟踪响应是否已开始;
//  3. 提供统一的 ping / error / heartbeat 辅助方法;
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

// Start 显式提交 SSE 响应头并立即刷给客户端。
//
// 纯推送型 handler（如从 Hub 消费事件）必须先调用它：否则在第一条事件到来前
// 连接上一个字节都没有，前端迟迟不触发 onopen，空闲连接还可能被代理层掐断。
// 返回错误表示响应头没能送达，客户端已断开时可判定为 ErrClientGone。
func (s *Stream) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.startLocked()
}

// Event 发送一条命名 SSE 事件;响应尚未开始时自动先提交 SSE 响应头。
func (s *Stream) Event(name string, payload any) error {
	return s.send(opEvent, func() (string, error) { return buildEventFrame("", name, payload) })
}

// EventWithID 发送一条带 `id:` 字段的命名 SSE 事件(断线续传,见 Writer.EventWithID);
// 响应尚未开始时自动先提交 SSE 响应头。
func (s *Stream) EventWithID(id, name string, payload any) error {
	return s.send(opEvent, func() (string, error) { return buildEventFrame(id, name, payload) })
}

// Data 发送一条 data-only 帧(OpenAI 风格,见 Writer.Data);
// 响应尚未开始时自动先提交 SSE 响应头。
func (s *Stream) Data(payload any) error {
	return s.send(opEvent, func() (string, error) { return buildEventFrame("", "", payload) })
}

// Send 写出一条 Event，语义与逐字段调用一致：Name 为空时写 data-only 帧，
// ID 为空时省略 `id:` 行。供 Hub 的消费循环直接写出投递来的事件。
func (s *Stream) Send(e Event) error {
	return s.send(opEvent, func() (string, error) { return buildEventFrame(e.ID, e.Name, e.Data) })
}

// Ping 发送一条标准保活注释帧。
func (s *Stream) Ping(at time.Time) error {
	return s.Comment("ping " + at.UTC().Format(time.RFC3339))
}

// Error 发送一条标准业务 error 事件。
func (s *Stream) Error(payload any) error {
	return s.Event("error", payload)
}

// Comment 发送一条注释帧。
func (s *Stream) Comment(text string) error {
	return s.send(opComment, func() (string, error) { return buildCommentFrame(text), nil })
}

// Retry 告知客户端建议的重连间隔(毫秒);负值返回 ErrInvalidArgument。
func (s *Stream) Retry(milliseconds int) error {
	return s.send(opRetry, func() (string, error) { return buildRetryFrame(milliseconds) })
}

// Heartbeat 每隔 interval 发送一条保活注释帧，直到 ctx 取消或写入失败。
//
// 阻塞运行，启动时机与所在 goroutine 由调用方控制：
//
//	go func() { _ = stream.Heartbeat(c.Request.Context(), 15*time.Second) }()
//
// 长时间无数据的流（等支付结果、等登录态）必须有心跳，否则代理层会按空闲连接断开。
// ctx 取消时返回 ctx 错误；客户端断开返回 ErrClientGone；流已终止返回 ErrStreamClosed。
// interval 非正时立即返回 ErrInvalidArgument，不写出任何字节。
func (s *Stream) Heartbeat(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return invalidArgf("ssex: heartbeat interval must be positive: %s", interval)
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

// Close 终止本流：发送一条名为 close 的事件，并拒绝后续所有写入（返回 ErrStreamClosed）。
//
// 为什么需要它：EventSource 在服务端正常结束流后会按重连间隔自动重连，
// 订单进入终态、LLM 输出结束后直接 return 会让前端反复重连。约定前端监听
// close 事件后调用 EventSource.close()，这轮推送才真正结束。
//
// Close 只终结本流的写入许可，不关闭 HTTP 连接——连接在 handler 返回时结束。
// 即使终止事件写入失败（客户端已断开），流同样标记为已终止。重复调用返回 ErrStreamClosed。
func (s *Stream) Close(payload any) error {
	frame, err := buildEventFrame("", "close", payload)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStreamClosed
	}

	if startErr := s.startLocked(); startErr != nil {
		s.closed = true

		return startErr
	}

	err = s.writer.emit(opEvent, frame)
	s.closed = true

	return err
}

// Started 返回 SSE 响应是否已开始写入。
//
// 首帧因参数非法或序列化失败而报错时它仍为 false——响应头尚未提交，
// 调用方可以改用普通 JSON 回错。
func (s *Stream) Started() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.started
}

// Context 返回绑定到本 SSE 连接的请求上下文。
func (s *Stream) Context() context.Context {
	return s.writer.Context()
}

// send 先构造帧,构造失败时不提交响应头(调用方仍可改回普通 JSON 响应);
// 构造成功后才在锁内起流并写出。
func (s *Stream) send(op string, build func() (string, error)) error {
	frame, err := build()
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStreamClosed
	}
	if err := s.startLocked(); err != nil {
		return err
	}

	return s.writer.emit(op, frame)
}

// startLocked 首次调用时提交响应头。
//
// 连接已断开时响应头未提交,保持未开始状态;响应头一旦提交(即使随后刷新失败)
// 就标记为已开始,避免重复提交触发标准库的 superfluous response.WriteHeader。
func (s *Stream) startLocked() error {
	if s.started {
		return nil
	}
	if err := s.writer.checkAlive(opStart); err != nil {
		return err
	}

	s.started = true

	return s.writer.writeHeaders()
}
