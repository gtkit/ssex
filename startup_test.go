package ssex

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"
)

// deadlineErrWriter 让 SetWriteDeadline 按预设返回错误，用于验证
// "清除连接级写截止时间失败"与"起流刷新超时"两条路径。
type deadlineErrWriter struct {
	headers http.Header
	body    []byte
	flushed bool

	// clearErr 在清零（零值 deadline）时返回；perWriteErr 在设置具体 deadline 时返回。
	clearErr    error
	perWriteErr error
	// writeErr 在实际写入时返回。
	writeErr error

	deadlines []time.Time
}

func newDeadlineErrWriter() *deadlineErrWriter {
	return &deadlineErrWriter{headers: make(http.Header)}
}

func (w *deadlineErrWriter) Header() http.Header { return w.headers }

func (w *deadlineErrWriter) Write(b []byte) (int, error) {
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	w.body = append(w.body, b...)

	return len(b), nil
}

func (w *deadlineErrWriter) WriteHeader(int) {}

func (w *deadlineErrWriter) Flush() { w.flushed = true }

func (w *deadlineErrWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadlines = append(w.deadlines, deadline)
	if deadline.IsZero() {
		return w.clearErr
	}

	return w.perWriteErr
}

// TestStartSetsPerWriteDeadlineForFlush 验证起流的刷新也受 per-write deadline 约束:
// 起流会先把连接级截止时间清零,若这次刷新不设上界,异常或恶意连接能让 handler
// 无限期卡在起流上。
func TestStartSetsPerWriteDeadlineForFlush(t *testing.T) {
	t.Parallel()

	recorder := newDeadlineErrWriter()
	stream := NewStream(recorder, newTestRequest(), WithWriteTimeout(2*time.Second))

	if err := stream.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if !recorder.flushed {
		t.Fatal("起流未刷出响应头")
	}

	// 期望三次：清零连接级截止时间、为刷新设置 per-write deadline、刷新后清零
	if len(recorder.deadlines) != 3 {
		t.Fatalf("deadline calls = %d (%v), want 3", len(recorder.deadlines), recorder.deadlines)
	}
	if !recorder.deadlines[0].IsZero() {
		t.Fatalf("first call = %s, want zero（解除 http.Server.WriteTimeout）", recorder.deadlines[0])
	}
	if recorder.deadlines[1].IsZero() {
		t.Fatal("second call is zero, want per-write deadline for the flush")
	}
	if got := time.Until(recorder.deadlines[1]); got > 2*time.Second || got <= time.Second {
		t.Fatalf("per-write deadline ≈ %s, want ≈ 2s", got)
	}
	if !recorder.deadlines[2].IsZero() {
		t.Fatalf("third call = %s, want cleared", recorder.deadlines[2])
	}
}

// TestStartFlushTimeoutIsWriteTimeout 验证起流刷新超时归入 ErrWriteTimeout。
func TestStartFlushTimeoutIsWriteTimeout(t *testing.T) {
	t.Parallel()

	recorder := newDeadlineErrWriter()
	recorder.writeErr = os.ErrDeadlineExceeded
	stream := NewStream(recorder, newTestRequest())

	// 起流本身只刷新（不写字节），因此用一个首帧触发写入路径
	if err := stream.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	err := stream.Event("status", map[string]string{"s": "1"})
	if !errors.Is(err, ErrWriteTimeout) {
		t.Fatalf("error = %v, want ErrWriteTimeout", err)
	}
}

// TestStartFailsWhenClearDeadlineFails 验证解除连接级写截止时间失败时:
// 起流报错、响应头未提交、Started() 仍为 false,调用方可改回普通 JSON。
//
// 吞掉这个错误等于对调用方谎称"长连接已保活",而连接其实仍会在全局
// WriteTimeout 到期时被服务端掐断。
func TestStartFailsWhenClearDeadlineFails(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("boom")
	recorder := newDeadlineErrWriter()
	recorder.clearErr = wantErr
	stream := NewStream(recorder, newTestRequest())

	err := stream.Start()
	if !errors.Is(err, wantErr) {
		t.Fatalf("Start() error = %v, want %v", err, wantErr)
	}
	if stream.Started() {
		t.Fatal("Started() = true, want false（响应头尚未提交）")
	}
	if len(recorder.headers) != 0 {
		t.Fatalf("headers = %v, want none", recorder.headers)
	}
	if recorder.flushed {
		t.Fatal("响应已刷出，说明响应头被提交了")
	}
	if len(recorder.body) != 0 {
		t.Fatalf("body = %q, want empty", recorder.body)
	}
}

// TestStartSucceedsWhenDeadlineUnsupported 验证底层不支持设置截止时间时静默降级。
func TestStartSucceedsWhenDeadlineUnsupported(t *testing.T) {
	t.Parallel()

	recorder := newDeadlineErrWriter()
	recorder.clearErr = http.ErrNotSupported
	recorder.perWriteErr = http.ErrNotSupported
	stream := NewStream(recorder, newTestRequest())

	if err := stream.Start(); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}
	if !stream.Started() {
		t.Fatal("Started() = false, want true")
	}
	if got := recorder.headers.Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
		t.Fatalf("content type = %q", got)
	}
}

// TestWriterWriteHeadersPropagatesClearFailure 验证低层 Writer 同样传播该错误。
func TestWriterWriteHeadersPropagatesClearFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("boom")
	recorder := newDeadlineErrWriter()
	recorder.clearErr = wantErr

	if err := New(recorder, newTestRequest()).WriteHeaders(); !errors.Is(err, wantErr) {
		t.Fatalf("WriteHeaders() error = %v, want %v", err, wantErr)
	}
	if len(recorder.headers) != 0 {
		t.Fatalf("headers = %v, want none", recorder.headers)
	}
}

// ============================================================================
// Option 容忍 nil
// ============================================================================

func TestNilOptionIsSkipped(t *testing.T) {
	t.Parallel()

	writer, recorder := newDeadlineWriter(t, nil, WithWriteTimeout(2*time.Second), nil)
	if err := writer.Event("chunk", map[string]string{"ok": "1"}); err != nil {
		t.Fatalf("Event() error = %v", err)
	}
	if got := time.Until(recorder.deadlines[0]); got > 2*time.Second || got <= time.Second {
		t.Fatalf("write deadline ≈ %s, want ≈ 2s（nil Option 应被跳过而非 panic）", got)
	}
}

func TestNilHubOptionIsSkipped(t *testing.T) {
	t.Parallel()

	hub := NewHub(nil, WithQueueSize(1), nil)

	events, release := hub.Subscribe("u1")
	defer release()

	hub.Push("u1", Event{Data: "s1"})
	hub.Push("u1", Event{Data: "s2"})
	if got := <-events; got.Data != "s2" {
		t.Fatalf("event = %+v, want s2（容量 1 生效）", got)
	}
}

// ============================================================================
// Hub 顺序契约与载荷所有权
// ============================================================================

// TestHubPreservesOrderWithinConnection 验证单连接内保持入队顺序。
func TestHubPreservesOrderWithinConnection(t *testing.T) {
	t.Parallel()

	hub := NewHub(WithQueueSize(8))

	events, release := hub.Subscribe("u1")
	defer release()

	want := []string{"s1", "s2", "s3", "s4"}
	for _, status := range want {
		hub.Push("u1", Event{Data: status})
	}
	for i, expected := range want {
		if got := <-events; got.Data != expected {
			t.Fatalf("event[%d] = %+v, want %s", i, got, expected)
		}
	}
}

// TestHubSharedPayloadAcrossConnections 验证同一份载荷被多个连接并发写出时无竞争
// （前提是调用方投递后不再修改它，这是 Event.Data 的所有权约束）。
// 必须在 -race 下运行才有意义。
func TestHubSharedPayloadAcrossConnections(t *testing.T) {
	t.Parallel()

	hub := NewHub()
	payload := map[string]string{"status": "paid"}

	const conns = 4

	var (
		wg      sync.WaitGroup
		streams = make([]*Stream, conns)
	)
	wg.Add(conns)

	for i := range conns {
		events, release := hub.Subscribe("u1")
		defer release()

		streams[i] = newTestStream(httptest.NewRecorder())
		go func(s *Stream, in <-chan Event) {
			defer wg.Done()
			if err := s.Send(<-in); err != nil {
				t.Errorf("Send() error = %v", err)
			}
		}(streams[i], events)
	}

	if delivered, dropped := hub.Push("u1", Event{Name: "status", Data: payload}); delivered != conns || dropped != 0 {
		t.Fatalf("Push() = (%d, %d), want (%d, 0)", delivered, dropped, conns)
	}
	wg.Wait()
}
