package ssex

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
)

// newCanceledWriter 构造一个请求上下文已取消(等价于客户端已断开)的 Writer。
func newCanceledWriter(t *testing.T) *Writer {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	return New(httptest.NewRecorder(), newTestRequest().WithContext(ctx))
}

// TestClientGoneOnCanceledContext 验证所有写方法在客户端断开后都返回可判定的 ErrClientGone,
// 且错误链保留 context.Canceled——调用方据此静默结束流并取消上游请求。
func TestClientGoneOnCanceledContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		write func(*Writer) error
	}{
		{"Event", func(w *Writer) error { return w.Event("chunk", nil) }},
		{"EventWithID", func(w *Writer) error { return w.EventWithID("1", "chunk", nil) }},
		{"Data", func(w *Writer) error { return w.Data(nil) }},
		{"Comment", func(w *Writer) error { return w.Comment("keepalive") }},
		{"Retry", func(w *Writer) error { return w.Retry(3000) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.write(newCanceledWriter(t))
			if !errors.Is(err, ErrClientGone) {
				t.Fatalf("error = %v, want errors.Is(err, ErrClientGone)", err)
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v, want errors.Is(err, context.Canceled)", err)
			}
		})
	}
}

// TestMarshalFailureIsNotClientGone 验证序列化失败不被误判为客户端断开:
// 这类错误是调用方的编程错误,必须能与"前端关页面"区分开。
func TestMarshalFailureIsNotClientGone(t *testing.T) {
	t.Parallel()
	w, rec := newTestWriter(t)

	err := w.Event("chunk", make(chan int)) // channel 无法序列化
	if err == nil {
		t.Fatal("Event() error = nil, want marshal error")
	}
	if errors.Is(err, ErrClientGone) {
		t.Fatalf("error = %v, must not be judged as ErrClientGone", err)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("failed marshal must not write bytes, got %q", rec.Body.String())
	}
}
