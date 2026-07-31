package ssex

import (
	"testing"
	"time"
)

// newDeadlineWriter 构造挂在 deadlineResponseWriter 上的 Writer,用于观察每帧的写截止时间。
func newDeadlineWriter(t *testing.T, opts ...Option) (*Writer, *deadlineResponseWriter) {
	t.Helper()
	rec := newDeadlineResponseWriter()

	return New(rec, newTestRequest(), opts...), rec
}

func TestWithWriteTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts []Option
		want time.Duration
	}{
		{"未配置时用默认值", nil, defaultWriteTimeout},
		{"显式配置生效", []Option{WithWriteTimeout(2 * time.Second)}, 2 * time.Second},
		{"零值被忽略", []Option{WithWriteTimeout(0)}, defaultWriteTimeout},
		{"负值被忽略", []Option{WithWriteTimeout(-time.Second)}, defaultWriteTimeout},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w, rec := newDeadlineWriter(t, tt.opts...)

			if err := w.Event("chunk", map[string]string{"ok": "1"}); err != nil {
				t.Fatalf("Event() error = %v", err)
			}
			if len(rec.deadlines) != 2 {
				t.Fatalf("deadline calls = %d, want 2 (设置 + 清零)", len(rec.deadlines))
			}

			// 允许调度抖动:落在 (want-1s, want] 区间即认为生效
			got := time.Until(rec.deadlines[0])
			if got > tt.want || got <= tt.want-time.Second {
				t.Fatalf("write deadline ≈ %s, want ≈ %s", got, tt.want)
			}
			if !rec.deadlines[1].IsZero() {
				t.Fatalf("second deadline = %s, want cleared", rec.deadlines[1])
			}
		})
	}
}
