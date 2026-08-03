package gincompat

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gtkit/ssex"
)

// timeoutWriter 的写入总是超时，用来让终止帧发送失败。
type timeoutWriter struct {
	headers http.Header
}

func (w *timeoutWriter) Header() http.Header { return w.headers }

func (w *timeoutWriter) Write([]byte) (int, error) { return 0, os.ErrDeadlineExceeded }

func (w *timeoutWriter) WriteHeader(int) {}

func (w *timeoutWriter) Flush() {}

// TestCloseStreamErrorReporting 覆盖 closeStream 的两条契约。
//
// 忽略终止帧的失败会让前端收不到 close 而继续重连，且服务端毫无信号；
// 反过来，把"客户端已经断开"也报成错误又会制造无意义的告警噪音。
// 这两条分支此前只有实现、没有断言。
func TestCloseStreamErrorReporting(t *testing.T) {
	t.Parallel()

	t.Run("客户端已断开不上报", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // 连接已断开
		req := httptest.NewRequest(http.MethodGet, "/sse", nil).WithContext(ctx)
		stream := ssex.NewStream(httptest.NewRecorder(), req)

		reported := 0
		closeStream(stream, gin.H{"reason": "final"}, func(error) { reported++ })

		if reported != 0 {
			t.Fatalf("上报次数 = %d, want 0（ErrClientGone 属正常收尾，不该产生告警）", reported)
		}
	})

	t.Run("写超时要上报", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/sse", nil)
		stream := ssex.NewStream(&timeoutWriter{headers: make(http.Header)}, req)

		var got error
		closeStream(stream, gin.H{"reason": "final"}, func(err error) { got = err })

		if got == nil {
			t.Fatal("终止帧写超时未上报：前端会收不到 close 而继续重连")
		}
		if !errors.Is(got, ssex.ErrWriteTimeout) {
			t.Fatalf("上报的错误 = %v, want ErrWriteTimeout", got)
		}
		if errors.Is(got, ssex.ErrClientGone) {
			t.Fatalf("上报的错误 = %v, 不应被判定为 ErrClientGone（连接还在，只是写不出去）", got)
		}
	})

	t.Run("序列化失败要上报", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/sse", nil)
		stream := ssex.NewStream(httptest.NewRecorder(), req)

		var got error
		// gin.H 里放一个无法序列化的值
		closeStream(stream, gin.H{"ch": make(chan int)}, func(err error) { got = err })

		if got == nil {
			t.Fatal("终止帧序列化失败未上报")
		}
		if errors.Is(got, ssex.ErrClientGone) {
			t.Fatalf("上报的错误 = %v, 不应被判定为 ErrClientGone", got)
		}
	})
}
