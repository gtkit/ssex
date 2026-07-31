package ssex

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWriterWriteHeadersAndEvent(t *testing.T) {
	t.Parallel()

	writer, recorder := newTestWriter(t)
	_ = writer.WriteHeaders()
	if err := writer.Event("chunk", map[string]any{
		"session_id": "s1",
		"delta":      "hello",
	}); err != nil {
		t.Fatalf("Event() error = %v", err)
	}

	if got := recorder.Header().Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
		t.Fatalf("content type = %q", got)
	}
	if got := recorder.Header().Get("Cache-Control"); got != "no-store, no-cache" {
		t.Fatalf("cache control = %q", got)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "event: chunk") {
		t.Fatalf("body missing event name: %s", body)
	}
	if !strings.Contains(body, `"session_id":"s1"`) || !strings.Contains(body, `"delta":"hello"`) {
		t.Fatalf("body missing payload: %s", body)
	}
	if !strings.HasSuffix(body, "\n\n") {
		t.Fatalf("body should end with event separator, got %q", body)
	}
}

func TestWriterEventReturnsContextError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	writer := New(httptest.NewRecorder(), newTestRequest().WithContext(ctx))

	err := writer.Event("chunk", map[string]string{"x": "y"})
	if err == nil {
		t.Fatal("Event() error = nil, want context error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Event() error = %v, want errors.Is(err, context.Canceled)", err)
	}
	if !errors.Is(err, ErrClientGone) {
		t.Fatalf("Event() error = %v, want errors.Is(err, ErrClientGone)", err)
	}
}

// TestWriterSurvivesServerWriteTimeout 验证 SSE 连接不会被 http.Server.WriteTimeout 截断。
// 回归场景：全局 WriteTimeout 作用到每次请求，心跳 Write 不会重置它；
// WriteHeaders 内部通过 http.ResponseController.SetWriteDeadline(time.Time{}) 清零。
func TestWriterSurvivesServerWriteTimeout(t *testing.T) {
	t.Parallel()

	const writeTimeout = 200 * time.Millisecond
	const totalDuration = writeTimeout * 5 // 明显超过 WriteTimeout，足以暴露问题

	mux := http.NewServeMux()
	mux.HandleFunc("/sse", func(w http.ResponseWriter, r *http.Request) {
		writer := New(w, r)
		_ = writer.WriteHeaders()

		ticker := time.NewTicker(writeTimeout / 2)
		defer ticker.Stop()
		deadline := time.NewTimer(totalDuration)
		defer deadline.Stop()

		for {
			select {
			case <-deadline.C:
				_ = writer.Event("done", map[string]string{"ok": "1"})

				return
			case <-ticker.C:
				if err := writer.Event("tick", map[string]string{"t": time.Now().Format(time.RFC3339Nano)}); err != nil {
					t.Errorf("Event() during long-lived SSE failed: %v", err)

					return
				}
			case <-r.Context().Done():
				return
			}
		}
	})

	server := httptest.NewUnstartedServer(mux)
	server.Config.WriteTimeout = writeTimeout
	server.Start()
	defer server.Close()

	resp, err := http.Get(server.URL + "/sse")
	if err != nil {
		t.Fatalf("GET sse failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// 读取整个响应；若 WriteTimeout 仍生效，连接会被服务端截断，读取不到 done 事件。
	body, err := io.ReadAll(bufio.NewReader(resp.Body))
	if err != nil {
		t.Fatalf("read sse body failed: %v", err)
	}

	if !strings.Contains(string(body), "event: done") {
		t.Fatalf("expected SSE stream to survive past WriteTimeout and deliver done event, got:\n%s", body)
	}
}

// TestContextReturnsRequestContext 验证 Context() 暴露的就是本连接的请求上下文——
// 调用方靠它感知客户端断开并取消上游请求。
func TestContextReturnsRequestContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := newTestRequest().WithContext(ctx)

	if got := New(httptest.NewRecorder(), req).Context(); got != ctx {
		t.Fatal("Writer.Context() 未返回请求上下文")
	}
	if got := NewStream(httptest.NewRecorder(), req).Context(); got != ctx {
		t.Fatal("Stream.Context() 未返回请求上下文")
	}
}

// TestWriteHeadersFlushesImmediately 验证起流即把响应头刷给客户端。
// 回归场景：响应头惰性写出时连接上零字节——大模型首 token 可能几十秒，
// 期间 EventSource 不触发 onopen，空闲连接易被代理层掐断。
func TestWriteHeadersFlushesImmediately(t *testing.T) {
	t.Parallel()

	const firstFrameDelay = 400 * time.Millisecond

	mux := http.NewServeMux()
	mux.HandleFunc("/sse", func(w http.ResponseWriter, r *http.Request) {
		stream := NewStream(w, r)
		_ = stream.Start()
		time.Sleep(firstFrameDelay)
		_ = stream.Event("chunk", map[string]string{"delta": "hi"})
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	begin := time.Now()
	resp, err := http.Get(server.URL + "/sse")
	if err != nil {
		t.Fatalf("GET sse failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if elapsed := time.Since(begin); elapsed >= firstFrameDelay {
		t.Fatalf("响应头耗时 %s，说明起流未刷出响应头（首帧延迟 %s）", elapsed, firstFrameDelay)
	}
	if got := resp.Header.Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
		t.Fatalf("content type = %q", got)
	}
}

func TestWriterCommentAndRetry(t *testing.T) {
	t.Parallel()

	writer, recorder := newTestWriter(t)
	_ = writer.WriteHeaders()
	if err := writer.Comment("keepalive"); err != nil {
		t.Fatalf("Comment() error = %v", err)
	}
	if err := writer.Retry(3000); err != nil {
		t.Fatalf("Retry() error = %v", err)
	}

	body := recorder.Body.String()
	if !strings.Contains(body, ": keepalive") {
		t.Fatalf("body missing comment frame: %s", body)
	}
	if !strings.Contains(body, "retry: 3000") {
		t.Fatalf("body missing retry frame: %s", body)
	}
}

func TestWriterOperationsSetPerWriteDeadline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		write    func(*Writer) error
		wantBody string
	}{
		{
			name:     "event",
			write:    func(writer *Writer) error { return writer.Event("chunk", map[string]string{"ok": "1"}) },
			wantBody: "event: chunk",
		},
		{
			name:     "comment",
			write:    func(writer *Writer) error { return writer.Comment("keepalive") },
			wantBody: ": keepalive",
		},
		{
			name:     "retry",
			write:    func(writer *Writer) error { return writer.Retry(3000) },
			wantBody: "retry: 3000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			writer, recorder := newDeadlineWriter(t)
			if err := tt.write(writer); err != nil {
				t.Fatalf("write error = %v", err)
			}

			if got := recorder.body.String(); !strings.Contains(got, tt.wantBody) {
				t.Fatalf("body missing %q: %s", tt.wantBody, got)
			}
			if len(recorder.deadlines) != 2 {
				t.Fatalf("deadline calls = %d, want 2", len(recorder.deadlines))
			}
			if recorder.deadlines[0].IsZero() {
				t.Fatal("first deadline is zero, want per-write timeout")
			}
			if time.Until(recorder.deadlines[0]) < defaultWriteTimeout/2 {
				t.Fatalf("first deadline = %s, want roughly %s in future", recorder.deadlines[0], defaultWriteTimeout)
			}
			if !recorder.deadlines[1].IsZero() {
				t.Fatalf("second deadline = %s, want cleared deadline", recorder.deadlines[1])
			}
		})
	}
}

// deadlineResponseWriter 记录每次设置的写截止时间,用于验证 per-write deadline。
type deadlineResponseWriter struct {
	headers   http.Header
	body      strings.Builder
	deadlines []time.Time
}

func newDeadlineResponseWriter() *deadlineResponseWriter {
	return &deadlineResponseWriter{headers: make(http.Header)}
}

func (w *deadlineResponseWriter) Header() http.Header { return w.headers }

func (w *deadlineResponseWriter) Write(data []byte) (int, error) { return w.body.Write(data) }

func (w *deadlineResponseWriter) WriteHeader(int) {}

func (w *deadlineResponseWriter) Flush() {}

func (w *deadlineResponseWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadlines = append(w.deadlines, deadline)

	return nil
}
