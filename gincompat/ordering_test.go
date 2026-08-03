package gincompat

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gtkit/ssex"
)

// TestNoEventLossBetweenSubscribeAndSnapshot 验证读快照期间发生的状态变更不会丢。
//
// 回归场景：模板曾是 Load → Start → Subscribe。Load 与 Subscribe 之间发生的
// 状态变更此时无人订阅，Push 直接丢弃，客户端拿到旧快照后再也收不到更新，
// 永远停在 pending 上——正好命中订单支付与登录态这两个主要场景。
//
// 这里在 load 回调里触发一次 Push 来精确模拟那个窗口：只有"先订阅、后读快照"
// 的顺序才能让这条事件落进队列。
func TestNoEventLossBetweenSubscribeAndSnapshot(t *testing.T) {
	t.Parallel()

	hub := ssex.NewHub()
	deps := orderEventsDeps{
		hub:         hub,
		appShutdown: context.Background(),
		load: func(_ context.Context, orderID string) (int64, gin.H, error) {
			// 读快照期间支付回调到达：此刻必须已经订阅，否则这条 paid 永久丢失
			hub.Push(orderID, ssex.Event{ID: "5", Name: "status", Data: gin.H{"status": "paid"}})

			// 快照仍是这次变更之前的版本（revision 1）
			return 1, gin.H{"status": "pending"}, nil
		},
		terminal: func(e ssex.Event) bool { return e.ID == "5" },
	}

	server := httptest.NewServer(newEngine(deps, "u1"))
	defer server.Close()

	resp, err := http.Get(server.URL + "/events/orders/o1")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var got []string
	for msg, err := range ssex.Decode(resp.Body) {
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		got = append(got, msg.ID+":"+msg.Name)
		if msg.Name == "close" {
			break
		}
	}

	// 快照(rev 1) → 窗口期内的 paid(rev 5) → 终态 close
	if len(got) != 3 || got[0] != "1:status" || got[1] != "5:status" {
		t.Fatalf("events = %v, want [1:status 5:status ...close]（rev 5 丢失说明先读快照后订阅）", got)
	}
}

// TestStaleEventsSkippedByRevision 验证不比快照新的事件被跳过。
//
// 先订阅后读快照会让订阅期间积压的事件里含有已经体现在快照中的旧版本，
// 直接转发会让前端出现状态回退。
func TestStaleEventsSkippedByRevision(t *testing.T) {
	t.Parallel()

	hub := ssex.NewHub(ssex.WithQueueSize(8))
	deps := orderEventsDeps{
		hub:         hub,
		appShutdown: context.Background(),
		load: func(_ context.Context, orderID string) (int64, gin.H, error) {
			// 订阅之后积压两条旧事件，它们的变更已经包含在下面这个 revision 7 的快照里
			hub.Push(orderID, ssex.Event{ID: "3", Name: "status", Data: gin.H{"status": "created"}})
			hub.Push(orderID, ssex.Event{ID: "7", Name: "status", Data: gin.H{"status": "pending"}})

			return 7, gin.H{"status": "pending"}, nil
		},
		terminal: func(e ssex.Event) bool { return e.ID == "9" },
	}

	server := httptest.NewServer(newEngine(deps, "u1"))
	defer server.Close()

	resp, err := http.Get(server.URL + "/events/orders/o1")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	waitFor(t, "handler 完成订阅", func() bool { return hub.Online("o1") == 1 })
	hub.Push("o1", ssex.Event{ID: "9", Name: "status", Data: gin.H{"status": "paid"}})

	var got []string
	for msg, err := range ssex.Decode(resp.Body) {
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		got = append(got, msg.ID)
		if msg.Name == "close" {
			break
		}
	}

	// 只应有快照 7 与新事件 9；revision 3 与 7 的积压事件被跳过
	if len(got) != 3 || got[0] != "7" || got[1] != "9" {
		t.Fatalf("event ids = %v, want [7 9 ...]（出现 3 说明旧事件没被 revision 过滤掉）", got)
	}
}
