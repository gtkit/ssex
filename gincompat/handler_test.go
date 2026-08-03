// Package gincompat 验证 ssex 在 gin 下的集成行为。
//
// 这些测试同时是 README 第 9 节 handler 模板的可执行版本：模板改了，这里必须跟着改。
package gincompat

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gtkit/ssex"
)

func init() {
	gin.SetMode(gin.TestMode)
}

// orderEventsDeps 是模板 handler 需要的依赖，测试按用例注入。
type orderEventsDeps struct {
	hub         *ssex.Hub
	appShutdown context.Context
	// load 从持久化事实源读快照，返回单调递增的 revision 与载荷。
	// 为 nil 时用一个固定的 pending 快照。
	load func(ctx context.Context, orderID string) (int64, gin.H, error)
	// onStreamError 记录 handler 观察到的错误，供断言。
	onStreamError func(started bool, err error)
	// terminal 判断某条事件是否终态。
	terminal func(ssex.Event) bool
}

// revisionOf 取事件携带的 revision（模板约定放在 Event.ID 里）。
func revisionOf(e ssex.Event) int64 {
	rev, err := strconv.ParseInt(e.ID, 10, 64)
	if err != nil {
		return 0
	}

	return rev
}

// orderEvents 是 README 9.2 的模板 handler。
func orderEvents(deps orderEventsDeps) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 1. 鉴权与参数校验都在 Start() 之前
		uid := c.GetString("uid")
		if uid == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "未登录"})

			return
		}
		orderID := c.Param("id")

		// 2. 先订阅，再读快照。顺序反过来会永久漏事件：Load 与 Subscribe 之间
		//    发生的状态变更此时无人订阅，推送直接丢弃，客户端会永远停在旧快照上。
		events, release := deps.hub.Subscribe(orderID)
		defer release()

		// 3. 从事实源读快照。失败时还没起流，可以回普通 JSON。
		load := deps.load
		if load == nil {
			load = func(context.Context, string) (int64, gin.H, error) {
				return 1, gin.H{"status": "pending"}, nil
			}
		}
		snapRev, snapshot, err := load(c.Request.Context(), orderID)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"error": "订单不存在"})

			return
		}

		stream := ssex.NewStream(c.Writer, c.Request, ssex.WithWriteTimeout(10*time.Second))

		// 4. 起流失败时响应头尚未提交，仍可回 JSON
		if err := stream.Start(); err != nil {
			handleStreamError(c, stream, deps, err)

			return
		}

		// 5. 发快照，revision 放进事件 id
		if err := stream.EventWithID(strconv.FormatInt(snapRev, 10), "status", snapshot); err != nil {
			handleStreamError(c, stream, deps, err)

			return
		}

		// 6. 心跳：独立 ctx；错误走 hbErr，"已退出"走 hbDone。
		//    两者必须分开：若用同一个 channel 兼任，主循环的 case 消费掉唯一那次
		//    发送后，defer 里的第二次接收就永远等不到发送者——handler 永久阻塞，
		//    连排在后面的 release() 都不会执行。hbDone 用 close，读多少次都不会阻塞。
		hbCtx, stopHeartbeat := context.WithCancel(c.Request.Context())
		hbErr := make(chan error, 1)
		hbDone := make(chan struct{})
		go func() {
			defer close(hbDone)
			if err := stream.Heartbeat(hbCtx, 20*time.Millisecond); err != nil {
				hbErr <- err
			}
		}()
		defer func() {
			stopHeartbeat()
			<-hbDone
		}()

		// 7. 四个退出口
		for {
			select {
			case <-deps.appShutdown.Done():
				_ = stream.Close(gin.H{"reason": "server shutting down"})

				return

			case <-stream.Context().Done():
				return

			case err := <-hbErr:
				handleStreamError(c, stream, deps, err)

				return

			case e := <-events:
				// 跳过不比快照新的事件：订阅到读快照之间积压的那些，快照里已经含了
				if revisionOf(e) <= snapRev {
					continue
				}
				if err := stream.Send(e); err != nil {
					handleStreamError(c, stream, deps, err)

					return
				}
				if deps.terminal != nil && deps.terminal(e) {
					_ = stream.Close(gin.H{"reason": "final"})

					return
				}
			}
		}
	}
}

// handleStreamError 是 README 9.3 的错误处理规范。
func handleStreamError(c *gin.Context, stream *ssex.Stream, deps orderEventsDeps, err error) {
	started := stream.Started()
	if deps.onStreamError != nil {
		deps.onStreamError(started, err)
	}

	if errors.Is(err, ssex.ErrClientGone) || errors.Is(err, context.Canceled) {
		c.Abort() // 正常收尾

		return
	}

	_ = c.Error(err)

	if !started {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "建立事件流失败"})

		return
	}

	c.Abort() // 已开始 SSE：只能记录并结束
}

// newEngine 组装带鉴权中间件的 SSE 路由组（README 9.4）。
func newEngine(deps orderEventsDeps, uid string) *gin.Engine {
	engine := gin.New()
	group := engine.Group("/events")
	group.Use(gin.Recovery(), func(c *gin.Context) {
		if uid != "" {
			c.Set("uid", uid)
		}
		c.Next()
	})
	group.GET("/orders/:id", orderEvents(deps))

	return engine
}

func TestAuthRejectedBeforeStream(t *testing.T) {
	t.Parallel()

	deps := orderEventsDeps{hub: ssex.NewHub(), appShutdown: context.Background()}
	server := httptest.NewServer(newEngine(deps, "")) // 不注入 uid
	defer server.Close()

	resp, err := http.Get(server.URL + "/events/orders/o1")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); got == "text/event-stream; charset=utf-8" {
		t.Fatal("鉴权失败时不应进入 SSE 响应")
	}
}

// TestPushAndTerminalClose 验证快照 + 带外推送 + 终态 Close 的完整链路，
// 并用解码器解析客户端实际收到的字节。
func TestPushAndTerminalClose(t *testing.T) {
	t.Parallel()

	hub := ssex.NewHub()
	deps := orderEventsDeps{
		hub:         hub,
		appShutdown: context.Background(),
		terminal:    func(e ssex.Event) bool { return e.Name == "status" && e.ID == "2" },
	}
	server := httptest.NewServer(newEngine(deps, "u1"))
	defer server.Close()

	resp, err := http.Get(server.URL + "/events/orders/o1")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if got := resp.Header.Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
		t.Fatalf("content type = %q", got)
	}
	if got := resp.Header.Get("X-Accel-Buffering"); got != "no" {
		t.Fatalf("X-Accel-Buffering = %q, want no", got)
	}

	waitFor(t, "handler 完成 Subscribe", func() bool { return hub.Online("o1") == 1 })
	hub.Push("o1", ssex.Event{ID: "2", Name: "status", Data: gin.H{"status": "paid"}})

	var names []string
	for msg, err := range ssex.Decode(resp.Body) {
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		names = append(names, msg.Name)
		if msg.Name == "close" {
			break
		}
	}

	// 快照 status → 推送 status → 终态 close；中间可能夹着心跳注释帧（不产出消息）
	if len(names) < 3 || names[0] != "status" || names[len(names)-1] != "close" {
		t.Fatalf("events = %v, want status...close", names)
	}
}

// TestHeartbeatGoroutineExitsBeforeHandlerReturns 验证模板在 gin 的 Context
// 池化复用下是安全的。
//
// gin 的 c.Writer 是 Context 的内部字段，handler 返回后 Context 归还对象池，
// 下个请求会把它重置到另一个响应上。若心跳 goroutine 没在 handler 返回前退出，
// 它就会写到别人的响应里——那是 -race 能抓到的数据竞争。
// 本测试跑正确写法：并发多轮请求 + 立即断开，-race 下应当干净。
func TestHeartbeatGoroutineExitsBeforeHandlerReturns(t *testing.T) {
	t.Parallel()

	deps := orderEventsDeps{hub: ssex.NewHub(), appShutdown: context.Background()}
	server := httptest.NewServer(newEngine(deps, "u1"))
	defer server.Close()

	const (
		workers = 6
		rounds  = 12
	)

	var wg sync.WaitGroup
	wg.Add(workers)

	for range workers {
		go func() {
			defer wg.Done()
			for range rounds {
				resp, err := http.Get(server.URL + "/events/orders/o1")
				if err != nil {
					t.Errorf("GET failed: %v", err)

					return
				}
				// 读到首帧确认流已建立，随后立刻断开，制造 handler 与心跳的竞态窗口
				if _, err := resp.Body.Read(make([]byte, 32)); err != nil {
					t.Errorf("read: %v", err)
				}
				_ = resp.Body.Close()
			}
		}()
	}
	wg.Wait()

	// 所有 handler 退出后注册表应清空，说明 defer release() 都执行了
	waitFor(t, "全部连接注销", func() bool { return deps.hub.Online("o1") == 0 })
}

// TestSurvivesServerWriteTimeout 验证 gin 下长连接不被 http.Server.WriteTimeout 截断,
// 即 http.ResponseController 能经 gin ResponseWriter 的 Unwrap 拿到底层 SetWriteDeadline。
func TestSurvivesServerWriteTimeout(t *testing.T) {
	t.Parallel()

	const writeTimeout = 200 * time.Millisecond

	engine := gin.New()
	engine.GET("/sse", func(c *gin.Context) {
		stream := ssex.NewStream(c.Writer, c.Request)
		if err := stream.Start(); err != nil {
			return
		}

		ticker := time.NewTicker(writeTimeout / 2)
		defer ticker.Stop()
		deadline := time.NewTimer(writeTimeout * 5)
		defer deadline.Stop()

		for {
			select {
			case <-deadline.C:
				_ = stream.Close(gin.H{"reason": "done"})

				return
			case <-ticker.C:
				if err := stream.Event("tick", gin.H{"t": time.Now().UnixNano()}); err != nil {
					t.Errorf("Event() failed: %v", err)

					return
				}
			case <-stream.Context().Done():
				return
			}
		}
	})

	server := httptest.NewUnstartedServer(engine)
	server.Config.WriteTimeout = writeTimeout
	server.Start()
	defer server.Close()

	resp, err := http.Get(server.URL + "/sse")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	sawClose := false
	for msg, err := range ssex.Decode(resp.Body) {
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if msg.Name == "close" {
			sawClose = true

			break
		}
	}
	if !sawClose {
		t.Fatal("SSE 被 http.Server.WriteTimeout 截断，未收到 close 事件")
	}
}

// TestClientGoneOnDisconnect 验证 gin 下客户端断开被判定为 ErrClientGone。
func TestClientGoneOnDisconnect(t *testing.T) {
	t.Parallel()

	result := make(chan error, 1)
	engine := gin.New()
	engine.GET("/sse", func(c *gin.Context) {
		stream := ssex.NewStream(c.Writer, c.Request)
		if err := stream.Event("hello", nil); err != nil {
			result <- err

			return
		}

		deadline := time.After(5 * time.Second)
		for {
			select {
			case <-deadline:
				result <- errors.New("超时：客户端已断开但写入始终没有失败")

				return
			default:
			}
			if err := stream.Event("tick", nil); err != nil {
				result <- err

				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	})

	server := httptest.NewServer(engine)
	defer server.Close()

	resp, err := http.Get(server.URL + "/sse")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if _, err := resp.Body.Read(make([]byte, 64)); err != nil {
		t.Fatalf("read: %v", err)
	}
	_ = resp.Body.Close()

	if err := <-result; !errors.Is(err, ssex.ErrClientGone) {
		t.Fatalf("error = %v, want ErrClientGone", err)
	}
}

// TestStartedBoundary 验证错误处理的分界：起流前可回 JSON，起流后只能结束 handler。
func TestStartedBoundary(t *testing.T) {
	t.Parallel()

	t.Run("起流前失败可回 JSON", func(t *testing.T) {
		t.Parallel()

		engine := gin.New()
		engine.GET("/sse", func(c *gin.Context) {
			stream := ssex.NewStream(c.Writer, c.Request)
			// 首帧事件名非法：帧构造阶段就失败，响应头尚未提交
			err := stream.Event("bad\nname", nil)
			if err == nil {
				t.Error("want invalid argument error")
			}
			if stream.Started() {
				t.Error("Started() = true, want false")
			}
			if !errors.Is(err, ssex.ErrInvalidArgument) {
				t.Errorf("error = %v, want ErrInvalidArgument", err)
			}
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "建立事件流失败"})
		})

		server := httptest.NewServer(engine)
		defer server.Close()

		resp, err := http.Get(server.URL + "/sse")
		if err != nil {
			t.Fatalf("GET failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500", resp.StatusCode)
		}
		if got := resp.Header.Get("Content-Type"); got != "application/json; charset=utf-8" {
			t.Fatalf("content type = %q, want JSON", got)
		}
	})

	t.Run("起流后 Started 为真", func(t *testing.T) {
		t.Parallel()

		engine := gin.New()
		engine.GET("/sse", func(c *gin.Context) {
			stream := ssex.NewStream(c.Writer, c.Request)
			if err := stream.Event("ok", gin.H{"n": 1}); err != nil {
				t.Errorf("Event() error = %v", err)
			}
			if !stream.Started() {
				t.Error("Started() = false, want true")
			}
			// 此处禁止再调 c.JSON —— 响应头已提交
			c.Abort()
		})

		server := httptest.NewServer(engine)
		defer server.Close()

		resp, err := http.Get(server.URL + "/sse")
		if err != nil {
			t.Fatalf("GET failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		if got := resp.Header.Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
			t.Fatalf("content type = %q", got)
		}
	})
}

// TestAppShutdownClosesStream 验证应用级停机信号能让 handler 主动收尾。
func TestAppShutdownClosesStream(t *testing.T) {
	t.Parallel()

	shutdown, triggerShutdown := context.WithCancel(context.Background())
	deps := orderEventsDeps{hub: ssex.NewHub(), appShutdown: shutdown}
	server := httptest.NewServer(newEngine(deps, "u1"))
	defer server.Close()

	resp, err := http.Get(server.URL + "/events/orders/o1")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	waitFor(t, "handler 完成 Subscribe", func() bool { return deps.hub.Online("o1") == 1 })
	triggerShutdown()

	sawClose := false
	for msg, err := range ssex.Decode(resp.Body) {
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if msg.Name == "close" {
			sawClose = true

			break
		}
	}
	if !sawClose {
		t.Fatal("停机时未收到 close 事件")
	}
}

// waitFor 轮询等待 cond 成立。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("等待超时: %s", what)
}
