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

// TestOutOfOrderEventsDoNotRegress 验证快照之后乱序到达的旧版本不会被转发。
//
// 回归场景：过滤条件曾是 revisionOf(e) <= snapRev，只与快照比较。Hub 明确不保证
// 跨并发推送方的到达顺序（支付回调、MQ 消费者、后台任务可能同时推同一订单），
// 因此快照之后完全可能先到 rev 10、后到 rev 9——只比快照会把 9 也发出去，
// 前端从 10 回退到 9。正确做法是维护随成功发送推进的 lastRev。
func TestOutOfOrderEventsDoNotRegress(t *testing.T) {
	t.Parallel()

	hub := ssex.NewHub(ssex.WithQueueSize(8))
	deps := orderEventsDeps{
		hub:         hub,
		appShutdown: context.Background(),
		load: func(_ context.Context, _ string) (int64, gin.H, error) {
			return 7, gin.H{"status": "pending"}, nil
		},
		terminal: func(e ssex.Event) bool { return e.ID == "12" },
	}

	server := httptest.NewServer(newEngine(deps, "u1"))
	defer server.Close()

	resp, err := http.Get(server.URL + "/events/orders/o1")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	waitFor(t, "handler 完成订阅", func() bool { return hub.Online("o1") == 1 })

	// 先到新版本，再到乱序的旧版本，最后到终态
	hub.Push("o1", ssex.Event{ID: "10", Name: "status", Data: gin.H{"status": "paid"}})
	hub.Push("o1", ssex.Event{ID: "9", Name: "status", Data: gin.H{"status": "pending"}})
	hub.Push("o1", ssex.Event{ID: "12", Name: "status", Data: gin.H{"status": "delivered"}})

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

	// 快照 7 → 10 → 12；乱序的 9 必须被丢弃
	if len(got) != 4 || got[0] != "7" || got[1] != "10" || got[2] != "12" {
		t.Fatalf("event ids = %v, want [7 10 12 ...]（出现 9 说明只与快照比较、没维护 lastRev）", got)
	}
}

// TestInvalidRevisionIsReportedNotSilentlyDropped 验证缺少有效 revision 的事件
// 会被上报，而不是静默消失。
//
// 生产者忘填 Event.ID 时，若把解析失败当作 revision 0，事件会因为 0 <= lastRev
// 被默默丢掉，且没有任何信号——排查起来只能看到"前端收不到状态"。
func TestInvalidRevisionIsReportedNotSilentlyDropped(t *testing.T) {
	t.Parallel()

	hub := ssex.NewHub(ssex.WithQueueSize(8))
	reported := make(chan ssex.Event, 4)
	deps := orderEventsDeps{
		hub:               hub,
		appShutdown:       context.Background(),
		onInvalidRevision: func(e ssex.Event) { reported <- e },
		load: func(_ context.Context, _ string) (int64, gin.H, error) {
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

	waitFor(t, "handler 完成订阅", func() bool { return hub.Online("o1") == 1 })

	hub.Push("o1", ssex.Event{Name: "status", Data: gin.H{"status": "paid"}}) // 没填 ID
	hub.Push("o1", ssex.Event{ID: "5", Name: "status", Data: gin.H{"status": "delivered"}})

	for msg, err := range ssex.Decode(resp.Body) {
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if msg.Name == "close" {
			break
		}
	}

	select {
	case e := <-reported:
		if e.ID != "" {
			t.Fatalf("reported event = %+v, want the one without revision", e)
		}
	default:
		t.Fatal("缺少 revision 的事件被静默丢弃，未上报")
	}
}
