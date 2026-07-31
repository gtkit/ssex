package ssex

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// ============================================================================
// 提交时机:帧构造失败时不得提交响应头,调用方仍能改回普通 JSON
// ============================================================================

func TestStreamDoesNotCommitOnBuildFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		write func(*Stream) error
	}{
		{"事件名含换行", func(s *Stream) error { return s.Event("x\ndata: evil", nil) }},
		{"事件名含 NUL", func(s *Stream) error { return s.Event("x\x00y", nil) }},
		{"id 含换行", func(s *Stream) error { return s.EventWithID("1\n2", "chunk", nil) }},
		{"载荷无法序列化", func(s *Stream) error { return s.Event("chunk", make(chan int)) }},
		{"data 载荷无法序列化", func(s *Stream) error { return s.Data(make(chan int)) }},
		{"retry 为负", func(s *Stream) error { return s.Retry(-1) }},
		{"Send 的事件名非法", func(s *Stream) error { return s.Send(Event{Name: "a\nb"}) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			recorder := httptest.NewRecorder()
			stream := newTestStream(recorder)

			if err := tt.write(stream); err == nil {
				t.Fatal("error = nil, want build failure")
			}
			if stream.Started() {
				t.Fatal("Started() = true, want false（响应头不应在帧构造失败时提交）")
			}
			if recorder.Body.Len() != 0 {
				t.Fatalf("body = %q, want empty", recorder.Body.String())
			}
			if recorder.Flushed {
				t.Fatal("响应已被刷出，说明响应头已提交")
			}
			if len(recorder.Header()) != 0 {
				t.Fatalf("headers = %v, want none committed", recorder.Header())
			}

			// 关键:此时仍可改回普通 JSON 响应
			recorder.Header().Set("Content-Type", "application/json")
			recorder.WriteHeader(http.StatusBadRequest)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", recorder.Code)
			}
		})
	}
}

func TestStreamCommitsAfterSuccessfulBuild(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	stream := newTestStream(recorder)

	if err := stream.Event("status", map[string]string{"status": "pending"}); err != nil {
		t.Fatalf("Event() error = %v", err)
	}
	if !stream.Started() {
		t.Fatal("Started() = false after successful frame, want true")
	}
	if got := recorder.Header().Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
		t.Fatalf("content type = %q", got)
	}
}

// ============================================================================
// 起流错误可观察
// ============================================================================

func TestStartReturnsClientGoneWhenDisconnected(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	recorder := httptest.NewRecorder()
	stream := NewStream(recorder, newTestRequest().WithContext(ctx))

	err := stream.Start()
	if !errors.Is(err, ErrClientGone) {
		t.Fatalf("Start() error = %v, want ErrClientGone", err)
	}
	if stream.Started() {
		t.Fatal("Started() = true, want false（连接已断开时响应头不应提交）")
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("body = %q, want empty", recorder.Body.String())
	}
}

func TestStartSucceedsOnPlainRecorder(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	stream := newTestStream(recorder)

	if err := stream.Start(); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}
	if !stream.Started() {
		t.Fatal("Started() = false after Start(), want true")
	}
	// 重复起流是空操作，不重复提交响应头
	if err := stream.Start(); err != nil {
		t.Fatalf("second Start() error = %v, want nil", err)
	}
}

func TestWriteHeadersOnUnflushableWriter(t *testing.T) {
	t.Parallel()

	// deadlineResponseWriter 的 Flush 不返回错误，起流应成功
	writer, _ := newDeadlineWriter(t)
	if err := writer.WriteHeaders(); err != nil {
		t.Fatalf("WriteHeaders() error = %v, want nil", err)
	}
}

// ============================================================================
// 错误哨兵
// ============================================================================

func TestErrInvalidArgument(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		write func(*Stream) error
	}{
		{"事件名含换行", func(s *Stream) error { return s.Event("a\nb", nil) }},
		{"id 含 NUL", func(s *Stream) error { return s.EventWithID("a\x00b", "n", nil) }},
		{"retry 为负", func(s *Stream) error { return s.Retry(-1) }},
		{"心跳间隔为零", func(s *Stream) error { return s.Heartbeat(context.Background(), 0) }},
		{"心跳间隔为负", func(s *Stream) error { return s.Heartbeat(context.Background(), -time.Second) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.write(newTestStream(httptest.NewRecorder()))
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("error = %v, want errors.Is(err, ErrInvalidArgument)", err)
			}
			if errors.Is(err, ErrClientGone) {
				t.Fatalf("error = %v, must not be judged as ErrClientGone", err)
			}
		})
	}
}

// TestErrWriteTimeout 验证写截止时间超时被判定为 ErrWriteTimeout 而非客户端断开:
// 慢客户端的连接仍然活着,归错类会让调用方误判为"前端已关页面"而丢掉告警。
func TestErrWriteTimeout(t *testing.T) {
	t.Parallel()

	writer := New(&timeoutResponseWriter{headers: make(http.Header)}, newTestRequest())

	err := writer.Event("chunk", map[string]string{"x": "y"})
	if !errors.Is(err, ErrWriteTimeout) {
		t.Fatalf("error = %v, want errors.Is(err, ErrWriteTimeout)", err)
	}
	if errors.Is(err, ErrClientGone) {
		t.Fatalf("error = %v, must not be judged as ErrClientGone", err)
	}
}

// timeoutResponseWriter 的 Write 总是返回 os.ErrDeadlineExceeded,
// 模拟慢客户端导致的单帧写超时。
type timeoutResponseWriter struct {
	headers http.Header
}

func (w *timeoutResponseWriter) Header() http.Header { return w.headers }

func (w *timeoutResponseWriter) Write([]byte) (int, error) {
	return 0, os.ErrDeadlineExceeded
}

func (w *timeoutResponseWriter) WriteHeader(int) {}

func (w *timeoutResponseWriter) Flush() {}

// ============================================================================
// 解码边界
// ============================================================================

// TestDecodeFrameTooLargeAcrossLines 验证整帧上限:单行上限拦不住
// "大量短 data 行 + 迟迟不发空行"这种累积增长。
func TestDecodeFrameTooLargeAcrossLines(t *testing.T) {
	t.Parallel()

	// 每行只贡献 len(payload)+1 字节到帧计数，凑够行数让整帧超过上限且始终不发空行
	const payload = "xxxx"

	var sb strings.Builder
	line := "data: " + payload + "\n"
	for range maxFrameSize/(len(payload)+1) + 10 {
		sb.WriteString(line)
	}

	_, err := collectMessages(t, sb.String())
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("error = %v, want errors.Is(err, ErrFrameTooLarge)", err)
	}
}

func TestDecodeSingleLineTooLargeIsFrameTooLarge(t *testing.T) {
	t.Parallel()

	_, err := collectMessages(t, "data: "+strings.Repeat("x", maxFrameSize+1)+"\n\n")
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("error = %v, want errors.Is(err, ErrFrameTooLarge)", err)
	}
}

// TestDecodeFrameSizeResetsPerFrame 验证计数按帧重置:多个各自合规的帧
// 累计总量超过上限也不应报错。
func TestDecodeFrameSizeResetsPerFrame(t *testing.T) {
	t.Parallel()

	frame := "data: " + strings.Repeat("y", 1024) + "\n\n"
	var sb strings.Builder
	for range (maxFrameSize / 1024) + 8 {
		sb.WriteString(frame)
	}

	msgs, err := collectMessages(t, sb.String())
	if err != nil {
		t.Fatalf("Decode() error = %v, want nil", err)
	}
	if len(msgs) == 0 {
		t.Fatal("frames = 0, want many")
	}
	for i, msg := range msgs {
		if len(msg.Data) != 1024 {
			t.Fatalf("frame[%d] data len = %d, want 1024", i, len(msg.Data))
		}
	}
}

// TestDecodeRetryOverflow 验证极大 retry 值被忽略而非溢出成负 Duration。
func TestDecodeRetryOverflow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
	}{
		{"最大 int64", "retry: 9223372036854775807\ndata: x\n\n"},
		{"超出 uint64", "retry: 99999999999999999999999\ndata: x\n\n"},
		{"刚好超过毫秒上限", "retry: 9223372036855\ndata: x\n\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			msgs, err := collectMessages(t, tt.input)
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			if len(msgs) != 1 {
				t.Fatalf("frames = %d, want 1", len(msgs))
			}
			if msgs[0].Retry != 0 {
				t.Fatalf("retry = %s, want 0（超范围值应被忽略）", msgs[0].Retry)
			}
			if msgs[0].Retry < 0 {
				t.Fatalf("retry = %s, must never be negative", msgs[0].Retry)
			}
		})
	}
}

func TestDecodeRetryAtUpperBound(t *testing.T) {
	t.Parallel()

	// 恰好等于上限的值必须被接受
	msgs, err := collectMessages(t, "retry: 9223372036854\ndata: x\n\n")
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if len(msgs) != 1 || msgs[0].Retry <= 0 {
		t.Fatalf("retry = %v, want positive duration", msgs)
	}
}

// TestDecodePreservesInvalidUTF8 固定住"字节原样保留"这一与规范的差异:
// 转发链路上改写字节会让服务端交给前端的内容与上游不一致。
func TestDecodePreservesInvalidUTF8(t *testing.T) {
	t.Parallel()

	raw := "\xff\xfe invalid \xc3"
	msgs, err := collectMessages(t, "data: "+raw+"\n\n")
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if len(msgs) != 1 || string(msgs[0].Data) != raw {
		t.Fatalf("data = %q, want %q", msgs[0].Data, raw)
	}
}
