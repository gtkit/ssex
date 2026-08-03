package ssex

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestClosedStateBeatsBuildError 验证流终止后的错误契约:无论载荷能否序列化,
// 一律返回 ErrStreamClosed。
//
// 回归场景:send / Close 曾先构造帧、后检查 closed,于是"已终止 + 不可序列化载荷"
// 返回的是 JSON 序列化错误。文档承诺的是"终止后任何写入都返回 ErrStreamClosed",
// 调用方按 errors.Is 判断就会漏掉"流已经关了"这个事实。
func TestClosedStateBeatsBuildError(t *testing.T) {
	t.Parallel()

	badPayload := make(chan int) // 无法序列化

	tests := []struct {
		name  string
		write func(*Stream) error
	}{
		{"Event", func(s *Stream) error { return s.Event("chunk", badPayload) }},
		{"EventWithID", func(s *Stream) error { return s.EventWithID("1", "chunk", badPayload) }},
		{"Data", func(s *Stream) error { return s.Data(badPayload) }},
		{"Send", func(s *Stream) error { return s.Send(Event{Name: "chunk", Data: badPayload}) }},
		{"Error", func(s *Stream) error { return s.Error(badPayload) }},
		{"重复 Close", func(s *Stream) error { return s.Close(badPayload) }},
		// 非法事件名同样是构造阶段的错误
		{"非法事件名", func(s *Stream) error { return s.Event("a\nb", nil) }},
		{"负值 Retry", func(s *Stream) error { return s.Retry(-1) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stream := newTestStream(httptest.NewRecorder())
			if err := stream.Close(nil); err != nil {
				t.Fatalf("Close() error = %v", err)
			}

			err := tt.write(stream)
			if !errors.Is(err, ErrStreamClosed) {
				t.Fatalf("error = %v, want ErrStreamClosed（关闭状态必须优先于构造错误）", err)
			}
			if errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("error = %v, 不应暴露为参数错误", err)
			}
		})
	}
}

// TestBuildErrorStillWinsBeforeClose 确认上面的调整没有削弱既有契约:
// 流未终止时,构造失败仍然返回构造错误,且响应头未提交。
func TestBuildErrorStillWinsBeforeClose(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	stream := newTestStream(recorder)

	err := stream.Event("chunk", make(chan int))
	if err == nil {
		t.Fatal("error = nil, want marshal error")
	}
	if errors.Is(err, ErrStreamClosed) {
		t.Fatalf("error = %v, 流未终止时不该返回 ErrStreamClosed", err)
	}
	if stream.Started() {
		t.Fatal("Started() = true, want false（构造失败不应提交响应头）")
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("body = %q, want empty", recorder.Body.String())
	}
}

// startFailWriter 让第一次清零写截止时间失败,用来构造"Close 起流失败"。
type startFailWriter struct {
	headers http.Header
	calls   int
}

func (w *startFailWriter) Header() http.Header { return w.headers }

func (w *startFailWriter) Write(b []byte) (int, error) { return len(b), nil }

func (w *startFailWriter) WriteHeader(int) {}

func (w *startFailWriter) Flush() {}

func (w *startFailWriter) SetWriteDeadline(time.Time) error {
	w.calls++

	return errors.New("deadline unsupported by this transport")
}

// TestStartAfterFailedCloseIsRejected 验证 Close 起流失败后不能再起流。
//
// 回归场景:Close 在 startLocked 失败时会把流标记为已终止,而 startLocked 当时
// 不检查 closed——随后调用 Start 会成功提交响应头,形成 started 与 closed
// 同时为真的矛盾状态。
func TestStartAfterFailedCloseIsRejected(t *testing.T) {
	t.Parallel()

	writer := &startFailWriter{headers: make(http.Header)}
	stream := NewStream(writer, newTestRequest())

	// Close 会因为清零写截止时间失败而起流失败,但仍把流标记为已终止
	closeErr := stream.Close(nil)
	if closeErr == nil {
		t.Fatal("Close() error = nil, want start failure")
	}
	if stream.Started() {
		t.Fatal("Started() = true, want false（起流失败时响应头未提交）")
	}

	// 此后任何起流都必须被拒绝
	if err := stream.Start(); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("Start() error = %v, want ErrStreamClosed", err)
	}
	if stream.Started() {
		t.Fatal("Started() = true：流已终止后仍然提交了响应头，状态自相矛盾")
	}
	if len(writer.headers) != 0 {
		t.Fatalf("headers = %v, want none committed", writer.headers)
	}

	// 写方法同样被拒绝
	if err := stream.Event("chunk", nil); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("Event() error = %v, want ErrStreamClosed", err)
	}
}
