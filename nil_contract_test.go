package ssex

import (
	"net/http/httptest"
	"testing"
	"time"
)

// TestNilArgumentsPanic 钉住 nil 参数的契约：它们是明确的编程错误，
// 库选择 fail fast 而不是防御性降级。
//
// 这些函数如果对 nil 做静默降级，故障会被伪装成别的样子——一个永远读不到
// 内容的 Decode 看起来像"上游没有输出"，一个写不出字节的 Writer 看起来像
// "客户端没收到"，都比 panic 难定位得多。因此 GoDoc 明确要求非 nil，
// 这个测试固定住该行为，防止将来有人加进半吊子的防御（部分函数防御、
// 部分不防御）造成契约不一致。
func TestNilArgumentsPanic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		call func()
	}{
		{"Decode(nil)", func() {
			for range Decode(nil) { //nolint:revive // 故意迭代以触发读取
				break
			}
		}},
		{"LastEventID(nil)", func() { _ = LastEventID(nil) }},
		{"New(nil, nil).Context()", func() { _ = New(nil, nil).Context() }},
		{"New(w, nil).WriteHeaders()", func() { _ = New(httptest.NewRecorder(), nil).WriteHeaders() }},
		{"NewStream(nil, nil).Start()", func() { _ = NewStream(nil, nil).Start() }},
		{"Heartbeat(nil, interval)", func() {
			//nolint:staticcheck // 故意传 nil ctx
			_ = newTestStream(httptest.NewRecorder()).Heartbeat(nil, time.Millisecond)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			defer func() {
				if recover() == nil {
					t.Fatal("want panic on nil argument（契约是 fail fast，不做静默降级）")
				}
			}()

			tt.call()
		})
	}
}
