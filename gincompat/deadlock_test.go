package gincompat

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gtkit/ssex"
)

// failingWriter 在前 after 次写入后开始返回 os.ErrDeadlineExceeded，
// 用来确定性地制造"心跳因写错误返回、而请求上下文仍然活跃"这一场景。
//
// 它只嵌入 gin.ResponseWriter（不暴露 Unwrap），因此 http.ResponseController
// 拿不到 SetWriteDeadline，走静默降级——这不影响本测试要验证的路径。
type failingWriter struct {
	gin.ResponseWriter

	mu    sync.Mutex
	count int
	after int
}

func (w *failingWriter) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.count++
	if w.count > w.after {
		return 0, os.ErrDeadlineExceeded
	}

	return w.ResponseWriter.Write(b)
}

func (w *failingWriter) WriteString(s string) (int, error) {
	return w.Write([]byte(s))
}

// TestHeartbeatWriteErrorDoesNotBlockHandler 验证心跳因写错误返回时 handler 能正常退出。
//
// 回归场景：模板曾用同一个 channel 既传心跳错误、又当"goroutine 已退出"信号。
// 心跳 goroutine 只发送一次结果，主循环的 case 一旦消费掉它，defer 里的第二次接收
// 就永远等不到发送者——handler 永久阻塞。后果不止泄漏一个 goroutine：
// defer release() 按 LIFO 排在阻塞的 defer 之后，于是 Hub 注册表不清理、Online
// 长期不准、http.Server.Shutdown 一直等、gin Context 无法归还对象池。
//
// 请求上下文取消的路径掩盖了这个问题（主循环的 Done 分支几乎总是先就绪），
// 因此这里用一个到点必然写失败的 writer，让心跳以 ErrWriteTimeout 返回、
// 而 ctx 仍然活跃——主循环此时只有 hbErr 可选，死锁是确定性的。
func TestHeartbeatWriteErrorDoesNotBlockHandler(t *testing.T) {
	t.Parallel()

	hub := ssex.NewHub()
	handlerReturned := make(chan struct{})
	observed := make(chan error, 4)

	deps := orderEventsDeps{
		hub:           hub,
		appShutdown:   context.Background(),
		onStreamError: func(_ bool, err error) { observed <- err },
	}

	engine := gin.New()
	group := engine.Group("/events")
	group.Use(func(c *gin.Context) {
		c.Set("uid", "u1")
		// 放过起流与首帧快照，随后的心跳写入必失败
		c.Writer = &failingWriter{ResponseWriter: c.Writer, after: 2}
		c.Next()
	})
	group.GET("/orders/:id", func(c *gin.Context) {
		defer close(handlerReturned)
		orderEvents(deps)(c)
	})

	server := httptest.NewServer(engine)
	defer server.Close()

	resp, err := http.Get(server.URL + "/events/orders/o1")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// handler 必须在合理时间内返回：阻塞版本会一直卡到测试超时
	select {
	case <-handlerReturned:
	case <-time.After(5 * time.Second):
		t.Fatal("handler 未返回：心跳错误分支阻塞了 handler")
	}

	// defer release() 必须执行到
	waitFor(t, "连接注销", func() bool { return hub.Online("o1") == 0 })

	select {
	case err := <-observed:
		if err == nil {
			t.Fatal("未观察到心跳错误")
		}
	default:
		t.Fatal("handler 未上报心跳错误")
	}
}
