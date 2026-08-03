package gincompat

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gtkit/ssex"
)

// ============================================================================
// README 5.1「转发大模型输出」模板的可执行版本。
//
// 这个模板此前只存在于 Markdown 里，而它的每一条约束都是被真实缺陷推着加进去的：
// 结束哨兵判定、finish 的两条分支、Transport 克隆、显式起流、心跳保活。
// 纯文档形态已经漂移过多次，因此在这里跑起来。
// ============================================================================

// newUpstreamTransport 是 README 里的上游 Transport 构造。
//
// 必须从 DefaultTransport 克隆再覆盖，不要 new(http.Transport)——后者会丢掉
// ProxyFromEnvironment、ForceAttemptHTTP2、MaxIdleConns / IdleConnTimeout /
// ExpectContinueTimeout 等标准库调好的默认值。
func newUpstreamTransport() *http.Transport {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSHandshakeTimeout = 5 * time.Second
	tr.ResponseHeaderTimeout = 30 * time.Second
	tr.DialContext = (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext

	return tr
}

// relayDeps 是转发 handler 的可注入依赖。
type relayDeps struct {
	upstreamURL string
	client      *http.Client
	// heartbeat 为 0 时用一个足够长的间隔（测试里不希望心跳干扰断言）。
	heartbeat time.Duration
	// onResult 记录 handler 的最终返回值，供断言。
	onResult func(error)
}

// finish 是 README 5.1 的收尾函数。
//
// 先看心跳，有两个原因：
//  1. 心跳失败会 cancel 上游，因此上游返回的 context canceled 只是连带结果，
//     真正的原因是下游写不出去。不优先取心跳错误，ErrWriteTimeout 就会被上游
//     读错误遮蔽。
//  2. 心跳失败说明连接已经写不动了，此时再写终止帧只会白等一个 WithWriteTimeout
//     才失败，拖慢失败连接的释放，还会产生一次重复告警。
func finish(stream *ssex.Stream, hbErr <-chan error, reason string, cause error) error {
	select {
	case hbFail := <-hbErr:
		if errors.Is(hbFail, ssex.ErrClientGone) || errors.Is(hbFail, context.Canceled) {
			return nil
		}

		return hbFail
	default:
	}

	closeStream(stream, gin.H{"reason": reason}, nil)

	return cause
}

// relayChat 是 README 5.1 的模板 handler。
func relayChat(deps relayDeps) gin.HandlerFunc {
	return func(c *gin.Context) {
		err := doRelay(c, deps)
		if deps.onResult != nil {
			deps.onResult(err)
		}
	}
}

func doRelay(c *gin.Context, deps relayDeps) error {
	stream := ssex.NewStream(c.Writer, c.Request, ssex.WithWriteTimeout(2*time.Second))

	// 上游请求绑定下游 ctx，并叠加业务级的最长生成时长
	upCtx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(upCtx, http.MethodPost, deps.upstreamURL, strings.NewReader(`{}`))
	if err != nil {
		return err
	}
	resp, err := deps.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }() // 始终关闭，否则连接与 goroutine 泄漏

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("上游状态码 %d", resp.StatusCode)
	}

	// 用 mime.ParseMediaType 精确比较，不要 strings.HasPrefix
	rawCT := resp.Header.Get("Content-Type")
	mediaType, _, err := mime.ParseMediaType(rawCT)
	if err != nil || mediaType != "text/event-stream" {
		return fmt.Errorf("上游 Content-Type 非 SSE: %q", rawCT)
	}

	// 校验通过后显式起流，否则响应头要等第一个 token 或第一次心跳才提交
	if err := stream.Start(); err != nil {
		return err
	}

	interval := deps.heartbeat
	if interval == 0 {
		interval = time.Hour // 测试默认不让心跳干扰断言
	}

	hbCtx, stopHeartbeat := context.WithCancel(c.Request.Context())
	hbErr := make(chan error, 1)
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		if hbFail := stream.Heartbeat(hbCtx, interval); hbFail != nil {
			hbErr <- hbFail
			cancel() // 下游写不出去了，别再让上游继续生成
		}
	}()
	defer func() {
		stopHeartbeat()
		<-hbDone
	}()

	var completed bool
	for msg, decErr := range ssex.Decode(resp.Body) {
		if decErr != nil {
			return finish(stream, hbErr, "incomplete", decErr)
		}
		if string(msg.Data) == "[DONE]" {
			completed = true

			break
		}
		if writeErr := stream.Data(ssex.Raw(string(msg.Data))); writeErr != nil {
			cancel()

			if errors.Is(writeErr, ssex.ErrClientGone) || errors.Is(writeErr, context.Canceled) {
				return nil
			}

			return writeErr
		}
	}

	if !completed {
		return finish(stream, hbErr, "incomplete", errors.New("上游未返回结束哨兵，输出可能被截断"))
	}
	closeStream(stream, gin.H{"reason": "done"}, nil)

	return nil
}

// newUpstream 起一个假的上游 SSE 服务：按 frames 逐帧输出。
// contentType 为空时用正确的 text/event-stream。
func newUpstream(t *testing.T, status int, contentType string, frames ...string) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if contentType == "" {
			contentType = "text/event-stream; charset=utf-8"
		}
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(status)

		flusher, _ := w.(http.Flusher)
		for _, frame := range frames {
			if _, err := w.Write([]byte(frame)); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
}

// newRelayServer 组装转发端。
func newRelayServer(t *testing.T, deps relayDeps) *httptest.Server {
	t.Helper()

	if deps.client == nil {
		deps.client = &http.Client{Transport: newUpstreamTransport()}
	}

	engine := gin.New()
	engine.POST("/chat", relayChat(deps))

	return httptest.NewServer(engine)
}

func TestRelayForwardsUntilDoneSentinel(t *testing.T) {
	t.Parallel()

	upstream := newUpstream(t, http.StatusOK, "",
		"data: {\"delta\":\"Hel\"}\n\n",
		"data: {\"delta\":\"lo\"}\n\n",
		"data: [DONE]\n\n",
	)
	defer upstream.Close()

	results := make(chan error, 1)
	relay := newRelayServer(t, relayDeps{
		upstreamURL: upstream.URL,
		onResult:    func(err error) { results <- err },
	})
	defer relay.Close()

	resp, err := http.Post(relay.URL+"/chat", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var got []string
	for msg, decErr := range ssex.Decode(resp.Body) {
		if decErr != nil {
			t.Fatalf("decode: %v", decErr)
		}
		got = append(got, string(msg.Data))
		if msg.Name == "close" {
			break
		}
	}

	if handlerErr := <-results; handlerErr != nil {
		t.Fatalf("handler error = %v, want nil", handlerErr)
	}
	// 两个增量 + 终止帧；[DONE] 本身不转发
	if len(got) != 3 || got[0] != `{"delta":"Hel"}` || got[1] != `{"delta":"lo"}` {
		t.Fatalf("forwarded = %v", got)
	}
	if !strings.Contains(got[2], `"done"`) {
		t.Fatalf("终止帧 = %q, want reason=done", got[2])
	}
}

// TestRelayReportsTruncationWithoutSentinel 验证上游没发 [DONE] 就 EOF 时，
// 前端收到的是 incomplete 而不是 done——否则半截回答会被当成最终答案。
func TestRelayReportsTruncationWithoutSentinel(t *testing.T) {
	t.Parallel()

	// 上游输出两帧后直接结束，没有 [DONE]
	upstream := newUpstream(t, http.StatusOK, "",
		"data: {\"delta\":\"Hel\"}\n\n",
		"data: {\"delta\":\"lo\"}\n\n",
	)
	defer upstream.Close()

	results := make(chan error, 1)
	relay := newRelayServer(t, relayDeps{
		upstreamURL: upstream.URL,
		onResult:    func(err error) { results <- err },
	})
	defer relay.Close()

	resp, err := http.Post(relay.URL+"/chat", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var last string
	for msg, decErr := range ssex.Decode(resp.Body) {
		if decErr != nil {
			t.Fatalf("decode: %v", decErr)
		}
		last = string(msg.Data)
		if msg.Name == "close" {
			break
		}
	}

	if !strings.Contains(last, "incomplete") {
		t.Fatalf("终止帧 = %q, want reason=incomplete（缺结束哨兵必须可区分）", last)
	}

	handlerErr := <-results
	if handlerErr == nil {
		t.Fatal("handler error = nil，缺结束哨兵时必须返回错误以便告警")
	}
	if !strings.Contains(handlerErr.Error(), "结束哨兵") {
		t.Fatalf("handler error = %v, want 提到结束哨兵", handlerErr)
	}
}

func TestRelayRejectsBadUpstream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		status      int
		contentType string
		wantIn      string
	}{
		{"非 200 状态码", http.StatusBadGateway, "", "上游状态码"},
		{"上游返回 JSON", http.StatusOK, "application/json", "Content-Type 非 SSE"},
		// HasPrefix 会误接受这两个，ParseMediaType 精确比较才能挡住
		{"类似但非 SSE 的类型", http.StatusOK, "text/event-streaming", "Content-Type 非 SSE"},
		{"带后缀的伪类型", http.StatusOK, "text/event-stream-error", "Content-Type 非 SSE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			upstream := newUpstream(t, tt.status, tt.contentType, "data: x\n\n")
			defer upstream.Close()

			results := make(chan error, 1)
			relay := newRelayServer(t, relayDeps{
				upstreamURL: upstream.URL,
				onResult:    func(err error) { results <- err },
			})
			defer relay.Close()

			resp, err := http.Post(relay.URL+"/chat", "application/json", strings.NewReader(`{}`))
			if err != nil {
				t.Fatalf("POST failed: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()

			handlerErr := <-results
			if handlerErr == nil {
				t.Fatal("handler error = nil, want upstream rejection")
			}
			if !strings.Contains(handlerErr.Error(), tt.wantIn) {
				t.Fatalf("handler error = %v, want 包含 %q", handlerErr, tt.wantIn)
			}
			// 上游校验失败发生在起流之前，因此不应产出 SSE 响应
			if got := resp.Header.Get("Content-Type"); got == "text/event-stream; charset=utf-8" {
				t.Fatal("上游校验失败时不应已经起流")
			}
		})
	}
}

// TestRelayAcceptsCaseInsensitiveContentType 验证合法的大小写变体不被拒——
// media type 本身大小写不敏感，HasPrefix 会误拒。
func TestRelayAcceptsCaseInsensitiveContentType(t *testing.T) {
	t.Parallel()

	upstream := newUpstream(t, http.StatusOK, "Text/Event-Stream; charset=UTF-8",
		"data: [DONE]\n\n",
	)
	defer upstream.Close()

	results := make(chan error, 1)
	relay := newRelayServer(t, relayDeps{
		upstreamURL: upstream.URL,
		onResult:    func(err error) { results <- err },
	})
	defer relay.Close()

	resp, err := http.Post(relay.URL+"/chat", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if handlerErr := <-results; handlerErr != nil {
		t.Fatalf("handler error = %v, want nil（大小写变体是合法的）", handlerErr)
	}
}

// TestRelayStartsStreamBeforeFirstToken 验证校验通过后立即起流：
// 响应头不该等到第一个 token 才提交。
func TestRelayStartsStreamBeforeFirstToken(t *testing.T) {
	t.Parallel()

	const firstTokenDelay = 400 * time.Millisecond

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(firstTokenDelay) // 模型思考
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer upstream.Close()

	relay := newRelayServer(t, relayDeps{upstreamURL: upstream.URL})
	defer relay.Close()

	begin := time.Now()
	resp, err := http.Post(relay.URL+"/chat", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if elapsed := time.Since(begin); elapsed >= firstTokenDelay {
		t.Fatalf("响应头耗时 %s，说明没有显式起流（首 token 延迟 %s）", elapsed, firstTokenDelay)
	}
	if got := resp.Header.Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
		t.Fatalf("content type = %q", got)
	}
}

// TestRelayHeartbeatKeepsConnectionAlive 验证首 token 迟迟不来时心跳仍在发注释帧——
// 否则中间代理会按空闲超时掐掉连接，前端看到的是"生成中忽然断线"。
//
// 注释帧被 Decode 按规范忽略，因此这里直接读原始字节。
func TestRelayHeartbeatKeepsConnectionAlive(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		select { // 模型长时间思考，一个 token 都不吐
		case <-release:
		case <-r.Context().Done():

			return
		}
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer upstream.Close()

	relay := newRelayServer(t, relayDeps{
		upstreamURL: upstream.URL,
		heartbeat:   30 * time.Millisecond,
	})
	defer relay.Close()

	resp, err := http.Post(relay.URL+"/chat", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// 首 token 之前应当只读到注释帧
	buf := make([]byte, 256)
	n, err := resp.Body.Read(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	got := string(buf[:n])
	if !strings.HasPrefix(got, ":") {
		t.Fatalf("首 token 之前读到 %q，want 心跳注释帧（以 ':' 开头）", got)
	}

	close(release)
}

// TestRelayCancelsUpstreamWhenClientGone 验证下游断开会取消上游请求——
// 否则前端关了页面，服务端还在为它烧 token。
func TestRelayCancelsUpstreamWhenClientGone(t *testing.T) {
	t.Parallel()

	upstreamDone := make(chan struct{}, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)

		// 持续输出，直到上游请求被取消
		for {
			select {
			case <-r.Context().Done():
				upstreamDone <- struct{}{}

				return
			default:
			}
			if _, err := w.Write([]byte("data: {\"delta\":\"x\"}\n\n")); err != nil {
				upstreamDone <- struct{}{}

				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(5 * time.Millisecond)
		}
	}))
	defer upstream.Close()

	relay := newRelayServer(t, relayDeps{upstreamURL: upstream.URL})
	defer relay.Close()

	resp, err := http.Post(relay.URL+"/chat", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	// 读到首帧确认转发已开始，然后断开
	if _, err := resp.Body.Read(make([]byte, 32)); err != nil {
		t.Fatalf("read: %v", err)
	}
	_ = resp.Body.Close()

	select {
	case <-upstreamDone:
	case <-time.After(5 * time.Second):
		t.Fatal("下游断开后上游请求未被取消：会继续为已离开的用户烧 token")
	}
}
