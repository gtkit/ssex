// Package ssex 提供 Server-Sent Events 写入器：解决 SSE 长连接被
// http.Server.WriteTimeout 杀死的问题，并为每帧写入设置 per-write deadline。
//
// 入口只用标准库的 http.ResponseWriter 与 *http.Request，net/http、gin、chi
// 等都可直接使用；gin 里传 c.Writer 与 c.Request 即可。
package ssex

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	gtkitjson "github.com/gtkit/json/v2"
)

// Writer 是低层 SSE 写入器，负责设置 SSE 响应头与逐帧写入。
//
// 只依赖标准库 HTTP 接口，因此 net/http、gin、chi 等都可直接使用；
// gin 里传 c.Writer 与 c.Request 即可。
//
// 并发安全：Writer **非并发安全**——它直接写底层 http.ResponseWriter，
// 不做任何串行化。若需从多个 goroutine（如心跳 + 业务推送）写入同一连接，
// 请改用 Stream（它用互斥锁串行化所有写方法）。
type Writer struct {
	w            http.ResponseWriter
	r            *http.Request
	writeTimeout time.Duration
}

type rawData string

// Raw 将 data 标记为 Writer.Data 或 Stream.Data 的已编码 SSE data payload。
//
// 仅在确实需要绕过 JSON 序列化的 data-only 帧中使用,例如 OpenAI 风格哨兵:
// Data(ssex.Raw("[DONE]"))。裸换行仍会被拆成多条 data 行,不能注入 event 或 id 字段。
func Raw(data string) any {
	return rawData(data)
}

// New 创建一个 SSE Writer；可用 Option 调整写入行为。
//
// gin 里这样调用：ssex.New(c.Writer, c.Request)。
func New(w http.ResponseWriter, r *http.Request, opts ...Option) *Writer {
	return &Writer{w: w, r: r, writeTimeout: newOptions(opts).writeTimeout}
}

// WriteHeaders 写入 SSE 响应头，并解除 http.Server.WriteTimeout 对本长连接的写截止。
func (w *Writer) WriteHeaders() {
	// SSE 是长连接：必须解除 http.Server.WriteTimeout 对本条连接的写截止时间，
	// 否则待支付订单、LLM 长响应会在全局 WriteTimeout 到期时被服务端 RST。
	// SetWriteDeadline(time.Time{}) 表示不设超时；失败（httptest 的
	// http.ErrNotSupported / 连接已半关）不影响响应头继续下发，让后续 Write 自然报错。
	rc := http.NewResponseController(w.w)
	_ = rc.SetWriteDeadline(time.Time{})

	header := w.w.Header()
	header.Set("Content-Type", "text/event-stream; charset=utf-8")
	header.Set("Cache-Control", "no-store, no-cache")
	// Connection 是连接级头部,HTTP/2(RFC 9113)禁止;仅 HTTP/1.x 设置。
	if w.r.ProtoMajor == 1 {
		header.Set("Connection", "keep-alive")
	}
	header.Set("X-Accel-Buffering", "no")
	header.Set("X-Content-Type-Options", "nosniff")
	w.w.WriteHeader(http.StatusOK)

	// 立即把响应头刷给客户端：响应头本身是惰性落地的（框架包装层尤其如此），
	// 不刷则连接上一个字节都没有，前端 EventSource 要等到首帧才触发 onopen——
	// 大模型首 token 可能几十秒，期间的空闲连接易被代理层掐断。
	// 刷失败无需在此处理：紧随的首帧写入会带分类报错。
	_ = w.flush()
}

// Event 写入一条命名 SSE 事件，payload 自动 JSON 序列化；写入带 per-write deadline。
// name 含 \r / \n / NUL 时返回错误（防 SSE 帧注入），不写出任何字节。
func (w *Writer) Event(name string, payload any) error {
	return w.writeEvent("", name, payload)
}

// EventWithID 写入一条带 `id:` 字段的命名 SSE 事件，用于断线续传：
// EventSource 自动重连时会把最后收到的 id 放进 `Last-Event-ID` 头回传
// （服务端用 LastEventID 读取，自行决定从哪续推）。
// id 为空串时不输出 `id:` 行，行为等同 Event。
// id / name 含 \r / \n / NUL 时返回错误（防 SSE 帧注入），不写出任何字节。
func (w *Writer) EventWithID(id, name string, payload any) error {
	return w.writeEvent(id, name, payload)
}

// Data 写入一条 data-only 帧（仅 `data:` 行，无事件名），即 OpenAI 风格的
// 流式块格式；payload 自动 JSON 序列化，Raw(...) 原样透传——
// 终止哨兵可写作 Data(ssex.Raw("[DONE]"))，输出字面 `data: [DONE]`。
// 前端经 EventSource 的 onmessage（默认事件）接收。
func (w *Writer) Data(payload any) error {
	return w.writeEvent("", "", payload)
}

// writeEvent 是 Event / EventWithID / Data 共用的帧写入:id / name 为空串的
// 字段行省略,payload JSON 序列化为 data 行。
func (w *Writer) writeEvent(id, name string, payload any) error {
	const op = "ssex: write event"
	if err := w.checkAlive(op); err != nil {
		return err
	}

	if err := validateFieldValue("id", id); err != nil {
		return err
	}
	if err := validateFieldValue("event name", name); err != nil {
		return err
	}

	// Raw 绕过序列化原样透传:Marshal 会把字符串编码成带引号的 JSON
	// (实测 Marshal("[DONE]") => `"[DONE]"`),而 OpenAI 风格的哨兵要求字面
	// `data: [DONE]`;其余 payload 正常序列化。
	var data []byte
	if raw, ok := payload.(rawData); ok {
		data = []byte(raw)
	} else {
		marshaled, err := gtkitjson.Marshal(payload)
		if err != nil {
			return fmt.Errorf("%s: marshal payload: %w", op, err)
		}
		data = marshaled
	}

	var frame strings.Builder
	if id != "" {
		frame.WriteString("id: ")
		frame.WriteString(id)
		frame.WriteByte('\n')
	}
	if name != "" {
		frame.WriteString("event: ")
		frame.WriteString(name)
		frame.WriteByte('\n')
	}
	// data 含换行时按 SSE 规范拆成多个 data: 行(客户端以 \n 重新拼接)。
	// Marshal 输出换行必转义、走快路径;只有 raw 透传可能含裸换行。
	// 拆行前必须把孤立 \r 一并归一:SSE 以 CRLF / CR / LF 三者任一分行,
	// 只归一 CRLF 会让 \r 在客户端另起一行,使 raw 内容逃出 data 字段伪造帧。
	if !bytes.ContainsAny(data, "\r\n") {
		frame.WriteString("data: ")
		frame.Write(data)
		frame.WriteByte('\n')
	} else {
		for line := range strings.SplitSeq(normalizeNewlines(string(data)), "\n") {
			frame.WriteString("data: ")
			frame.WriteString(line)
			frame.WriteByte('\n')
		}
	}
	frame.WriteByte('\n')

	return w.withWriteDeadline(func() error {
		if _, err := io.WriteString(w.w, frame.String()); err != nil {
			return classify(w.r.Context(), op, err)
		}
		return w.flush()
	})
}

// validateFieldValue 拒绝含换行 / NUL 的 SSE 字段值:换行可注入伪造帧,
// NUL 是 SSE 规范明确禁止的 id 字符。非法字段是调用方编程错误,直接报错不转义。
func validateFieldValue(field, v string) error {
	if strings.ContainsAny(v, "\r\n\x00") {
		return fmt.Errorf("ssex: %s must not contain newline or NUL: %q", field, v)
	}
	return nil
}

// normalizeNewlines 把 CRLF 与孤立 CR 统一成 LF。
// SSE 规范以 CRLF / CR / LF 三者任一分行,只归一 CRLF 会让孤立 \r 在客户端
// 另起一行,使字段内容逃出所在字段伪造帧——注释帧与 raw data 帧都走这条路。
func normalizeNewlines(s string) string {
	if !strings.ContainsRune(s, '\r') {
		return s
	}
	return strings.ReplaceAll(strings.ReplaceAll(s, "\r\n", "\n"), "\r", "\n")
}

// checkAlive 在写入前确认连接仍可用,已断开则返回可判定的 ErrClientGone。
func (w *Writer) checkAlive(op string) error {
	if err := w.r.Context().Err(); err != nil {
		return clientGone(op, err)
	}
	return nil
}

// LastEventID 返回 EventSource 自动重连时携带的 `Last-Event-ID` 请求头
// （即客户端最后收到的 EventWithID 的 id），无则返回空串。
// 服务端据此决定断线续推的起点,由业务选择从哪一条开始重放。
func LastEventID(r *http.Request) string {
	return r.Header.Get("Last-Event-ID")
}

// Comment 写入一条 SSE 注释帧。
// 注释帧不会触发前端的业务事件回调，常用于链路保活、调试标记或代理层防空闲断开。
// 多行文本按行拆成多条注释行，任何换行形式都无法逃出注释语义。
func (w *Writer) Comment(text string) error {
	const op = "ssex: write comment"
	if err := w.checkAlive(op); err != nil {
		return err
	}

	// 逐行加 ": " 前缀后整帧一次写出;归一化孤立 \r,否则注释内容能另起一行伪造字段。
	var frame strings.Builder
	for line := range strings.SplitSeq(normalizeNewlines(text), "\n") {
		frame.WriteString(": ")
		frame.WriteString(line)
		frame.WriteByte('\n')
	}
	frame.WriteByte('\n')

	return w.withWriteDeadline(func() error {
		if _, err := io.WriteString(w.w, frame.String()); err != nil {
			return classify(w.r.Context(), op, err)
		}

		return w.flush()
	})
}

// Retry 写入 SSE 的 retry 指令，提示客户端后续重连间隔（毫秒）。
// 这是 SSE 协议的一部分，浏览器/EventSource 客户端会把它作为建议重连时间使用。
// milliseconds 为负时返回错误且不写出任何字节：SSE 规范只接受 ASCII 数字，
// 客户端会静默忽略非法值，静默失败比显式报错更难排查。
func (w *Writer) Retry(milliseconds int) error {
	const op = "ssex: write retry"
	if err := w.checkAlive(op); err != nil {
		return err
	}
	if milliseconds < 0 {
		return fmt.Errorf("%s: milliseconds must not be negative: %d", op, milliseconds)
	}

	return w.withWriteDeadline(func() error {
		if _, err := fmt.Fprintf(w.w, "retry: %d\n\n", milliseconds); err != nil {
			return classify(w.r.Context(), op, err)
		}

		return w.flush()
	})
}

// Context 返回绑定到本 SSE 连接的请求上下文。
func (w *Writer) Context() context.Context {
	return w.r.Context()
}

// flush 把帧冲刷到客户端并暴露错误:http.ResponseController.Flush 返回 error,
// 客户端断开能在当帧发现,
// 而非等 TCP 缓冲塞满后才从下一次 Write 报错(LLM 流场景避免对死连接白推)。
// writer 不支持 Flush(http.ErrNotSupported,如 httptest 包装层)时静默降级,
// 与 SetWriteDeadline 的降级策略一致。
func (w *Writer) flush() error {
	if err := http.NewResponseController(w.w).Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return classify(w.r.Context(), "ssex: flush", err)
	}
	return nil
}

func (w *Writer) withWriteDeadline(fn func() error) error {
	rc := http.NewResponseController(w.w)
	if err := rc.SetWriteDeadline(time.Now().Add(w.writeTimeout)); err != nil {
		if !errors.Is(err, http.ErrNotSupported) {
			return fmt.Errorf("ssex: set write deadline: %w", err)
		}
		return fn()
	}
	defer func() { _ = rc.SetWriteDeadline(time.Time{}) }()

	return fn()
}
