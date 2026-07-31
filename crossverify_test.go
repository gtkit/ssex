package ssex

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// ============================================================================
// 往返交叉验证:写侧产出的字节交给读侧解回。
// 两侧对 SSE 规范的实现互相独立,往返一致才说明双方理解一致。
// ============================================================================

// roundTrip 用 Writer 写出若干帧,再用 Decode 解回。
func roundTrip(t *testing.T, write func(*Writer) error) []Message {
	t.Helper()

	w, recorder := newTestWriter(t)
	if err := write(w); err != nil {
		t.Fatalf("write error = %v", err)
	}

	msgs, err := collectMessages(t, recorder.Body.String())
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	return msgs
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		write func(*Writer) error
		want  []Message
	}{
		{
			name:  "命名事件",
			write: func(w *Writer) error { return w.Event("chunk", map[string]string{"delta": "hi"}) },
			want:  []Message{{Name: "chunk", Data: []byte(`{"delta":"hi"}`)}},
		},
		{
			name:  "带 id 的事件",
			write: func(w *Writer) error { return w.EventWithID("7", "chunk", map[string]int{"n": 1}) },
			want:  []Message{{ID: "7", Name: "chunk", Data: []byte(`{"n":1}`)}},
		},
		{
			name:  "data-only 帧",
			write: func(w *Writer) error { return w.Data(map[string]string{"delta": "hi"}) },
			want:  []Message{{Data: []byte(`{"delta":"hi"}`)}},
		},
		{
			name:  "DONE 哨兵",
			write: func(w *Writer) error { return w.Data(Raw("[DONE]")) },
			want:  []Message{{Data: []byte("[DONE]")}},
		},
		{
			name:  "多行 raw 载荷往返后内容不变",
			write: func(w *Writer) error { return w.Data(Raw("line1\nline2")) },
			want:  []Message{{Data: []byte("line1\nline2")}},
		},
		{
			name:  "注释帧不产生事件",
			write: func(w *Writer) error { return w.Comment("keepalive") },
			want:  nil,
		},
		{
			name:  "载荷里的换行被 JSON 转义,不影响帧结构",
			write: func(w *Writer) error { return w.Event("chunk", map[string]string{"text": "a\nb\rc"}) },
			want:  []Message{{Name: "chunk", Data: []byte(`{"text":"a\nb\rc"}`)}},
		},
		{
			name:  "带前后空格的增量 token 往返后不变",
			write: func(w *Writer) error { return w.Data(Raw(" token ")) },
			want:  []Message{{Data: []byte(" token ")}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := roundTrip(t, tt.write)
			if len(got) != len(tt.want) {
				t.Fatalf("frames = %d, want %d (got %+v)", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i].ID != tt.want[i].ID || got[i].Name != tt.want[i].Name ||
					string(got[i].Data) != string(tt.want[i].Data) {
					t.Fatalf("frame[%d] = {ID:%q Name:%q Data:%q}, want {ID:%q Name:%q Data:%q}",
						i, got[i].ID, got[i].Name, got[i].Data,
						tt.want[i].ID, tt.want[i].Name, tt.want[i].Data)
				}
			}
		})
	}
}

// TestRoundTripInjectionAttempts 是帧注入修复的端到端验证:
// 由独立的规范解码器确认伪造的 event / data 行不会变成真事件——
// 只断言写出的字节形状不足以证明这一点,得让解析器给出判决。
func TestRoundTripInjectionAttempts(t *testing.T) {
	t.Parallel()

	t.Run("raw data 中的孤立 CR", func(t *testing.T) {
		t.Parallel()

		got := roundTrip(t, func(w *Writer) error {
			return w.Data(Raw("hello\revent: evil\rdata: pwned"))
		})
		if len(got) != 1 {
			t.Fatalf("frames = %d, want 1(注入产生了额外帧: %+v)", len(got), got)
		}
		if got[0].Name != "" {
			t.Fatalf("event name = %q, want empty(伪造的事件名逃出了 data 字段)", got[0].Name)
		}
		if want := "hello\nevent: evil\ndata: pwned"; string(got[0].Data) != want {
			t.Fatalf("data = %q, want %q", got[0].Data, want)
		}
	})

	t.Run("注释中的孤立 CR", func(t *testing.T) {
		t.Parallel()

		got := roundTrip(t, func(w *Writer) error {
			return w.Comment("keepalive\revent: evil\rdata: pwned")
		})
		if len(got) != 0 {
			t.Fatalf("frames = %+v, want none(注释内容逃出了注释语义)", got)
		}
	})
}

// ============================================================================
// 真实连接端到端:ResponseRecorder 不经过 TCP,无法暴露断开检测与 Hub 的协作
// ============================================================================

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

// firstMessage 从流里解出第一条消息就退出,不等流结束。
func firstMessage(r io.Reader) (Message, error) {
	for msg, err := range Decode(r) {
		return msg, err
	}

	return Message{}, errors.New("流中没有消息")
}

// TestNetHTTPHandler 验证在标准库 handler 里直接可用(库不依赖任何 web 框架),
// 并顺带验证 LastEventID 从真实请求头读取。
func TestNetHTTPHandler(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/sse", func(w http.ResponseWriter, r *http.Request) {
		stream := NewStream(w, r)
		if id := LastEventID(r); id != "" {
			_ = stream.Comment("resume from " + id)
		}
		_ = stream.EventWithID("2", "chunk", map[string]string{"delta": "hi"})
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL+"/sse", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Last-Event-ID", "1")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET sse failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if got := resp.Header.Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
		t.Fatalf("content type = %q", got)
	}

	msg, err := firstMessage(resp.Body)
	if err != nil {
		t.Fatalf("decode client stream: %v", err)
	}
	if msg.ID != "2" || msg.Name != "chunk" || string(msg.Data) != `{"delta":"hi"}` {
		t.Fatalf("message = {ID:%q Name:%q Data:%q}", msg.ID, msg.Name, msg.Data)
	}
}

// TestClientGoneOnRealDisconnect 验证真实客户端断开后写入被判定为 ErrClientGone。
// 这是 README 承诺的核心用法(据此取消上游大模型请求以停止计费),必须在真实连接上验证:
// 手动 cancel 上下文只能证明代码路径通,证明不了 net/http 会在客户端断开时取消它。
func TestClientGoneOnRealDisconnect(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	result := make(chan error, 1)

	mux.HandleFunc("/sse", func(w http.ResponseWriter, r *http.Request) {
		stream := NewStream(w, r)
		if err := stream.Event("hello", nil); err != nil {
			result <- err

			return
		}

		deadline := time.After(5 * time.Second)
		for {
			select {
			case <-deadline:
				result <- errors.New("超时:客户端已断开但写入始终没有失败")

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

	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/sse")
	if err != nil {
		t.Fatalf("GET sse failed: %v", err)
	}

	// 读到首帧确认流已建立,然后不读完就关闭连接
	if _, err := resp.Body.Read(make([]byte, 64)); err != nil {
		t.Fatalf("read first frame failed: %v", err)
	}
	_ = resp.Body.Close()

	if err := <-result; !errors.Is(err, ErrClientGone) {
		t.Fatalf("error = %v, want errors.Is(err, ErrClientGone)", err)
	}
}

// TestStreamOverHTTP2 在真实 HTTP/2 连接上验证起流、写帧、flush 与每帧写截止时间都工作,
// 且不下发 HTTP/2 禁止的 Connection 头。生产环境常经 ALB / nginx 走 h2,
// 而 h2 的 ResponseWriter 与 HTTP/1.x 是两套实现,伪造 ProtoMajor 的单元测试覆盖不到。
func TestStreamOverHTTP2(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/sse", func(w http.ResponseWriter, r *http.Request) {
		stream := NewStream(w, r)
		stream.Start()
		if err := stream.Event("status", map[string]string{"status": "paid"}); err != nil {
			t.Errorf("Event() over h2 failed: %v", err)

			return
		}
		if err := stream.Close(map[string]string{"reason": "done"}); err != nil {
			t.Errorf("Close() over h2 failed: %v", err)
		}
	})

	server := httptest.NewUnstartedServer(mux)
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()

	resp, err := server.Client().Get(server.URL + "/sse")
	if err != nil {
		t.Fatalf("GET sse over h2 failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.ProtoMajor != 2 {
		t.Fatalf("proto = %s, want HTTP/2", resp.Proto)
	}
	if got := resp.Header.Get("Connection"); got != "" {
		t.Fatalf("Connection = %q, want empty on HTTP/2", got)
	}

	var got []Message
	for msg, err := range Decode(resp.Body) {
		if err != nil {
			t.Fatalf("decode h2 stream: %v", err)
		}
		got = append(got, msg)
	}
	if len(got) != 2 || got[0].Name != "status" || got[1].Name != "close" {
		t.Fatalf("messages = %+v, want status then close", got)
	}
}

// TestHubStreamEndToEnd 验证 README 里的 handler 模板在真实连接上确实可用:
// Hub 投递 → handler 写出 → 客户端侧用 Decode 解回,顺带确认断开后 release 被执行。
func TestHubStreamEndToEnd(t *testing.T) {
	t.Parallel()

	hub := NewHub()
	mux := http.NewServeMux()
	mux.HandleFunc("/sse", func(w http.ResponseWriter, r *http.Request) {
		stream := NewStream(w, r)
		// 必须先起流:纯推送型 handler 在收到第一条事件前不写任何字节,
		// 不起流则客户端一直等响应头(实测会一直挂到超时)。
		stream.Start()

		events, release := hub.Subscribe("u1")
		defer release()

		for {
			select {
			case <-stream.Context().Done():
				return
			case e := <-events:
				if err := stream.Send(e); err != nil {
					return
				}
			}
		}
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/sse")
	if err != nil {
		t.Fatalf("GET sse failed: %v", err)
	}

	waitFor(t, "handler 完成 Subscribe", func() bool { return hub.Online("u1") == 1 })

	delivered, dropped := hub.Push("u1", Event{
		ID:   "1",
		Name: "status",
		Data: map[string]string{"status": "paid"},
	})
	if delivered != 1 || dropped != 0 {
		t.Fatalf("Push() = (%d, %d), want (1, 0)", delivered, dropped)
	}

	msg, err := firstMessage(resp.Body)
	if err != nil {
		t.Fatalf("decode client stream: %v", err)
	}
	if msg.ID != "1" || msg.Name != "status" || string(msg.Data) != `{"status":"paid"}` {
		t.Fatalf("message = {ID:%q Name:%q Data:%q}", msg.ID, msg.Name, msg.Data)
	}

	// 客户端断开后 handler 应退出并执行 defer release()
	_ = resp.Body.Close()
	waitFor(t, "客户端断开后 release 生效", func() bool { return hub.Online("u1") == 0 })
}
