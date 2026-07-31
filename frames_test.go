package ssex

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// newTestRequest 构造一条 SSE 测试请求。
func newTestRequest() *http.Request {
	return httptest.NewRequest(http.MethodGet, "/sse", nil)
}

// newTestWriter 构造 Writer + recorder 测试对。
func newTestWriter(t *testing.T) (*Writer, *httptest.ResponseRecorder) {
	t.Helper()
	recorder := httptest.NewRecorder()

	return New(recorder, newTestRequest()), recorder
}

// ============================================================================
// EventWithID:断线续传 id 字段
// ============================================================================

func TestWriterEventWithID(t *testing.T) {
	t.Parallel()
	w, rec := newTestWriter(t)

	if err := w.EventWithID("42", "chunk", map[string]string{"delta": "hi"}); err != nil {
		t.Fatalf("EventWithID() error = %v", err)
	}

	body := rec.Body.String()
	if !strings.HasPrefix(body, "id: 42\nevent: chunk\ndata: ") {
		t.Fatalf("frame = %q, want id/event/data 三行序", body)
	}
	if !strings.HasSuffix(body, "\n\n") {
		t.Fatalf("frame should end with separator, got %q", body)
	}
}

func TestWriterEventWithIDEmptyIDEqualsEvent(t *testing.T) {
	t.Parallel()
	w, rec := newTestWriter(t)

	if err := w.EventWithID("", "chunk", map[string]string{"x": "y"}); err != nil {
		t.Fatalf("EventWithID() error = %v", err)
	}
	if body := rec.Body.String(); strings.Contains(body, "id:") {
		t.Fatalf("empty id should omit id line, got %q", body)
	}
}

func TestLastEventID(t *testing.T) {
	t.Parallel()
	req := newTestRequest()
	req.Header.Set("Last-Event-ID", "42")

	if got := LastEventID(req); got != "42" {
		t.Fatalf("LastEventID() = %q, want 42", got)
	}

	req.Header.Del("Last-Event-ID")
	if got := LastEventID(req); got != "" {
		t.Fatalf("LastEventID() = %q, want empty", got)
	}
}

// ============================================================================
// Data:data-only 帧(OpenAI 风格)
// ============================================================================

func TestWriterData(t *testing.T) {
	t.Parallel()
	w, rec := newTestWriter(t)

	if err := w.Data(map[string]string{"delta": "hi"}); err != nil {
		t.Fatalf("Data() error = %v", err)
	}

	body := rec.Body.String()
	if !strings.HasPrefix(body, "data: ") {
		t.Fatalf("frame = %q, want data-only", body)
	}
	if strings.Contains(body, "event:") || strings.Contains(body, "id:") {
		t.Fatalf("data-only frame must not contain event/id line, got %q", body)
	}
}

func TestWriterDataRawSentinel(t *testing.T) {
	t.Parallel()
	w, rec := newTestWriter(t)

	if err := w.Data(Raw("[DONE]")); err != nil {
		t.Fatalf("Data() error = %v", err)
	}
	if body := rec.Body.String(); body != "data: [DONE]\n\n" {
		t.Fatalf("frame = %q, want literal data: [DONE]", body)
	}
}

func TestWriterDataRawMultilineSplitsDataLines(t *testing.T) {
	t.Parallel()
	w, rec := newTestWriter(t)

	// raw 透传含裸换行:按 SSE 规范拆成多个 data: 行,换行无法逃出 data 字段
	if err := w.Data(Raw("line1\nline2")); err != nil {
		t.Fatalf("Data() error = %v", err)
	}
	if body := rec.Body.String(); body != "data: line1\ndata: line2\n\n" {
		t.Fatalf("frame = %q, want multiline data lines", body)
	}
}

// ============================================================================
// 字段注入防护
// ============================================================================

func TestWriterRejectsFieldInjection(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		call func(w *Writer) error
	}{
		{"event name 换行", func(w *Writer) error { return w.Event("x\ndata: evil", nil) }},
		{"event name 回车", func(w *Writer) error { return w.Event("x\rdata: evil", nil) }},
		{"id 换行", func(w *Writer) error { return w.EventWithID("1\n2", "n", nil) }},
		{"id NUL", func(w *Writer) error { return w.EventWithID("1\x002", "n", nil) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w, rec := newTestWriter(t)
			if err := tc.call(w); err == nil {
				t.Fatal("expected injection rejection error, got nil")
			}
			if rec.Body.Len() != 0 {
				t.Fatalf("rejected frame must not write bytes, got %q", rec.Body.String())
			}
		})
	}
}

// ============================================================================
// 换行归一:孤立 CR 不得逃出所在字段
// SSE 以 CRLF / CR / LF 三者任一分行,只归一 CRLF 时孤立 \r 会在客户端另起一行
// ============================================================================

func TestWriterCommentNormalizesLoneCR(t *testing.T) {
	t.Parallel()
	w, rec := newTestWriter(t)

	// 回归:孤立 \r 曾原样透传,客户端按 CR 分行后会派发伪造的 evil 事件
	if err := w.Comment("keepalive\revent: evil\rdata: pwned"); err != nil {
		t.Fatalf("Comment() error = %v", err)
	}
	want := ": keepalive\n: event: evil\n: data: pwned\n\n"
	if got := rec.Body.String(); got != want {
		t.Fatalf("frame = %q, want %q", got, want)
	}
}

func TestWriterCommentNormalizesCRLF(t *testing.T) {
	t.Parallel()
	w, rec := newTestWriter(t)

	if err := w.Comment("a\r\nb"); err != nil {
		t.Fatalf("Comment() error = %v", err)
	}
	want := ": a\n: b\n\n"
	if got := rec.Body.String(); got != want {
		t.Fatalf("frame = %q, want %q", got, want)
	}
}

func TestWriterDataRawNormalizesLoneCR(t *testing.T) {
	t.Parallel()
	w, rec := newTestWriter(t)

	// 回归:同一逃逸在 raw data 透传路径上同样存在
	if err := w.Data(Raw("hello\revent: evil\rdata: pwned")); err != nil {
		t.Fatalf("Data() error = %v", err)
	}
	want := "data: hello\ndata: event: evil\ndata: data: pwned\n\n"
	if got := rec.Body.String(); got != want {
		t.Fatalf("frame = %q, want %q", got, want)
	}
}

func TestWriterDataRawNormalizesCRLF(t *testing.T) {
	t.Parallel()
	w, rec := newTestWriter(t)

	if err := w.Data(Raw("line1\r\nline2")); err != nil {
		t.Fatalf("Data() error = %v", err)
	}
	want := "data: line1\ndata: line2\n\n"
	if got := rec.Body.String(); got != want {
		t.Fatalf("frame = %q, want %q", got, want)
	}
}

// ============================================================================
// retry 值域:规范只接受 ASCII 数字,非法值客户端静默忽略
// ============================================================================

func TestWriterRetryRejectsNegative(t *testing.T) {
	t.Parallel()
	w, rec := newTestWriter(t)

	if err := w.Retry(-1); err == nil {
		t.Fatal("Retry(-1) error = nil, want error")
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("rejected retry must not write bytes, got %q", rec.Body.String())
	}
}

func TestWriterRetryAllowsZero(t *testing.T) {
	t.Parallel()
	w, rec := newTestWriter(t)

	if err := w.Retry(0); err != nil {
		t.Fatalf("Retry(0) error = %v", err)
	}
	if got, want := rec.Body.String(), "retry: 0\n\n"; got != want {
		t.Fatalf("frame = %q, want %q", got, want)
	}
}

// ============================================================================
// 响应头合规:h2 无 Connection,nosniff 恒有
// ============================================================================

func TestWriteHeadersProtocolAware(t *testing.T) {
	t.Parallel()

	// HTTP/1.1:有 Connection
	rec1 := httptest.NewRecorder()
	_ = New(rec1, newTestRequest()).WriteHeaders()
	if got := rec1.Header().Get("Connection"); got != "keep-alive" {
		t.Fatalf("HTTP/1 Connection = %q, want keep-alive", got)
	}

	// HTTP/2:无 Connection(连接级头部,RFC 9113 禁止)
	rec2 := httptest.NewRecorder()
	req2 := newTestRequest()
	req2.Proto, req2.ProtoMajor, req2.ProtoMinor = "HTTP/2.0", 2, 0
	_ = New(rec2, req2).WriteHeaders()
	if got := rec2.Header().Get("Connection"); got != "" {
		t.Fatalf("HTTP/2 Connection = %q, want empty", got)
	}

	for _, rec := range []*httptest.ResponseRecorder{rec1, rec2} {
		if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
			t.Fatalf("X-Content-Type-Options = %q, want nosniff", got)
		}
	}
}

// ============================================================================
// Stream 封装:EventWithID / Data 自动写头
// ============================================================================

func TestStreamEventWithIDAndDataAutoStart(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	s := NewStream(rec, newTestRequest())
	if err := s.EventWithID("7", "chunk", map[string]int{"n": 1}); err != nil {
		t.Fatalf("EventWithID() error = %v", err)
	}
	if err := s.Data(Raw("[DONE]")); err != nil {
		t.Fatalf("Data() error = %v", err)
	}

	if !s.Started() {
		t.Fatal("stream should be started after first frame")
	}
	if got := rec.Header().Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
		t.Fatalf("content type = %q", got)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "id: 7\nevent: chunk\n") || !strings.HasSuffix(body, "data: [DONE]\n\n") {
		t.Fatalf("body = %q", body)
	}
}
