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

// 各写操作的错误前缀，出现在返回错误的最前面，便于定位来源。
const (
	opStart   = "ssex: start stream"
	opEvent   = "ssex: write event"
	opComment = "ssex: write comment"
	opRetry   = "ssex: write retry"
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

// WriteHeaders 提交 SSE 响应头并立即刷给客户端，同时解除 http.Server.WriteTimeout
// 对本长连接的写截止时间。
//
// 返回错误表示响应头没能送达：客户端已断开时可判定为 ErrClientGone。
// 底层不支持刷新时返回 nil（静默降级）。
// 响应头一旦提交就不应重复调用，否则标准库会报 superfluous response.WriteHeader。
func (w *Writer) WriteHeaders() error {
	if err := w.checkAlive(opStart); err != nil {
		return err
	}
	if err := w.clearWriteDeadline(); err != nil {
		return err
	}

	return w.commitHeaders()
}

// clearWriteDeadline 解除 http.Server.WriteTimeout 对本条长连接的写截止时间。
//
// SSE 是长连接：不解除的话，待支付订单、大模型长响应会在全局 WriteTimeout
// 到期时被服务端 RST。因此这里的失败不能吞掉——吞掉就等于对调用方谎称
// "长连接已保活"，而连接其实仍会在几十秒后被掐断。
// 底层不支持（http.ErrNotSupported，如某些包装层）时静默降级。
//
// 调用它时尚未触碰响应头，因此失败可以安全地当作"响应头未提交"。
func (w *Writer) clearWriteDeadline() error {
	err := http.NewResponseController(w.w).SetWriteDeadline(time.Time{})
	if err == nil || errors.Is(err, http.ErrNotSupported) {
		return nil
	}

	return classify(w.r.Context(), opStart, err)
}

// commitHeaders 设置 SSE 响应头、提交状态码并刷出。
//
// 与 clearWriteDeadline 分开是为了让 Stream 能区分"响应头未提交"与
// "响应头已提交但刷新失败"——后者不能再提交第二次，否则标准库会报
// superfluous response.WriteHeader。
// 一旦进入本方法，响应头即视为已提交。
func (w *Writer) commitHeaders() error {
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
	//
	// 这次刷新与后续帧共用 per-write deadline：刚才清零的是连接级截止时间，
	// 若这里不设上界，异常或恶意连接能让 handler 无限期卡在起流上。
	return w.withWriteDeadline(func() error {
		return w.flush(opStart)
	})
}

// Event 写入一条命名 SSE 事件，payload 自动 JSON 序列化；写入带 per-write deadline。
// name 含 \r / \n / NUL 时返回 ErrInvalidArgument，不写出任何字节。
func (w *Writer) Event(name string, payload any) error {
	frame, err := buildEventFrame("", name, payload)
	if err != nil {
		return err
	}

	return w.emit(opEvent, frame)
}

// EventWithID 写入一条带 `id:` 字段的命名 SSE 事件，用于断线续传：
// EventSource 自动重连时会把最后收到的 id 放进 `Last-Event-ID` 头回传
// （服务端用 LastEventID 读取，自行决定从哪续推）。
// id 为空串时不输出 `id:` 行，行为等同 Event。
// id / name 含 \r / \n / NUL 时返回 ErrInvalidArgument，不写出任何字节。
func (w *Writer) EventWithID(id, name string, payload any) error {
	frame, err := buildEventFrame(id, name, payload)
	if err != nil {
		return err
	}

	return w.emit(opEvent, frame)
}

// Data 写入一条 data-only 帧（仅 `data:` 行，无事件名），即 OpenAI 风格的
// 流式块格式；payload 自动 JSON 序列化，Raw(...) 原样透传——
// 终止哨兵可写作 Data(ssex.Raw("[DONE]"))，输出字面 `data: [DONE]`。
// 前端经 EventSource 的 onmessage（默认事件）接收。
func (w *Writer) Data(payload any) error {
	frame, err := buildEventFrame("", "", payload)
	if err != nil {
		return err
	}

	return w.emit(opEvent, frame)
}

// Comment 写入一条 SSE 注释帧。
// 注释帧不会触发前端的业务事件回调，常用于链路保活、调试标记或代理层防空闲断开。
// 多行文本按行拆成多条注释行，任何换行形式都无法逃出注释语义。
func (w *Writer) Comment(text string) error {
	return w.emit(opComment, buildCommentFrame(text))
}

// Retry 写入 SSE 的 retry 指令，提示客户端后续重连间隔（毫秒）。
// 这是 SSE 协议的一部分，浏览器/EventSource 客户端会把它作为建议重连时间使用。
// milliseconds 为负时返回 ErrInvalidArgument 且不写出任何字节：SSE 规范只接受
// ASCII 数字，客户端会静默忽略非法值，静默失败比显式报错更难排查。
func (w *Writer) Retry(milliseconds int) error {
	frame, err := buildRetryFrame(milliseconds)
	if err != nil {
		return err
	}

	return w.emit(opRetry, frame)
}

// Context 返回绑定到本 SSE 连接的请求上下文。
func (w *Writer) Context() context.Context {
	return w.r.Context()
}

// buildEventFrame 构造一条完整的事件帧:校验字段值、序列化载荷、按规范拆 data 行。
//
// 它是纯函数,完全不接触响应——校验或序列化失败时响应头尚未提交,
// 调用方仍可改用普通 JSON 回错(见 Stream.Started)。
func buildEventFrame(id, name string, payload any) (string, error) {
	if err := validateFieldValue("id", id); err != nil {
		return "", err
	}
	if err := validateFieldValue("event name", name); err != nil {
		return "", err
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
			return "", fmt.Errorf("%s: marshal payload: %w", opEvent, err)
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

	return frame.String(), nil
}

// buildCommentFrame 构造注释帧:逐行加 ": " 前缀,整帧一次写出。
// 归一化孤立 \r,否则注释内容能在客户端另起一行伪造字段。
func buildCommentFrame(text string) string {
	var frame strings.Builder
	for line := range strings.SplitSeq(normalizeNewlines(text), "\n") {
		frame.WriteString(": ")
		frame.WriteString(line)
		frame.WriteByte('\n')
	}
	frame.WriteByte('\n')

	return frame.String()
}

// buildRetryFrame 构造 retry 指令帧。
func buildRetryFrame(milliseconds int) (string, error) {
	if milliseconds < 0 {
		return "", invalidArgf("%s: milliseconds must not be negative: %d", opRetry, milliseconds)
	}

	return fmt.Sprintf("retry: %d\n\n", milliseconds), nil
}

// emit 把已构造好的帧写出:确认连接可用、设单帧写截止时间、写入并刷新。
func (w *Writer) emit(op, frame string) error {
	if err := w.checkAlive(op); err != nil {
		return err
	}

	return w.withWriteDeadline(func() error {
		if _, err := io.WriteString(w.w, frame); err != nil {
			return classify(w.r.Context(), op, err)
		}

		return w.flush(op)
	})
}

// validateFieldValue 拒绝含换行 / NUL 的 SSE 字段值:换行可注入伪造帧,
// NUL 是 SSE 规范明确禁止的 id 字符。非法字段是调用方编程错误,直接报错不转义。
func validateFieldValue(field, v string) error {
	if strings.ContainsAny(v, "\r\n\x00") {
		return invalidArgf("ssex: %s must not contain newline or NUL: %q", field, v)
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

// flush 把帧冲刷到客户端并暴露错误:http.ResponseController.Flush 返回 error,
// 客户端断开能在当帧发现,而非等 TCP 缓冲塞满后才从下一次 Write 报错
// (LLM 流场景避免对死连接白推)。
// writer 不支持 Flush(http.ErrNotSupported,如某些包装层)时静默降级,
// 与 SetWriteDeadline 的降级策略一致。
func (w *Writer) flush(op string) error {
	if err := http.NewResponseController(w.w).Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return classify(w.r.Context(), op, err)
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
	// 恢复失败不额外上报:那意味着后续帧仍带这次已过期的截止时间,
	// 下一次写入会立即失败并返回 ErrWriteTimeout——是延后一帧暴露,而非静默丢失。
	defer func() { _ = rc.SetWriteDeadline(time.Time{}) }()

	return fn()
}
