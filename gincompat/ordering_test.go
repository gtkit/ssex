package gincompat

import (
	"context"
	"io"
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
		load: func(_ context.Context, res resource) (int64, gin.H, error) {
			// 读快照期间支付回调到达：此刻必须已经订阅，否则这条 paid 永久丢失
			hub.Push(res.scopeKey(), ssex.Event{ID: "5", Name: "status", Data: gin.H{"status": "paid"}})

			// 快照仍是这次变更之前的版本（revision 1）
			return 1, gin.H{"status": "pending"}, nil
		},
		terminal: func(e ssex.Event) bool { return e.ID == "5" },
	}

	server := httptest.NewServer(newEngine(deps, authedIdentity()))
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
		load: func(_ context.Context, res resource) (int64, gin.H, error) {
			// 订阅之后积压两条旧事件，它们的变更已经包含在下面这个 revision 7 的快照里
			hub.Push(res.scopeKey(), ssex.Event{ID: "3", Name: "status", Data: gin.H{"status": "created"}})
			hub.Push(res.scopeKey(), ssex.Event{ID: "7", Name: "status", Data: gin.H{"status": "pending"}})

			return 7, gin.H{"status": "pending"}, nil
		},
		terminal: func(e ssex.Event) bool { return e.ID == "9" },
	}

	server := httptest.NewServer(newEngine(deps, authedIdentity()))
	defer server.Close()

	resp, err := http.Get(server.URL + "/events/orders/o1")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	waitFor(t, "handler 完成订阅", func() bool { return hub.Online(keyFor("t1", "o1")) == 1 })
	hub.Push(keyFor("t1", "o1"), ssex.Event{ID: "9", Name: "status", Data: gin.H{"status": "paid"}})

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
		load: func(_ context.Context, _ resource) (int64, gin.H, error) {
			return 7, gin.H{"status": "pending"}, nil
		},
		terminal: func(e ssex.Event) bool { return e.ID == "12" },
	}

	server := httptest.NewServer(newEngine(deps, authedIdentity()))
	defer server.Close()

	resp, err := http.Get(server.URL + "/events/orders/o1")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	waitFor(t, "handler 完成订阅", func() bool { return hub.Online(keyFor("t1", "o1")) == 1 })

	// 先到新版本，再到乱序的旧版本，最后到终态
	hub.Push(keyFor("t1", "o1"), ssex.Event{ID: "10", Name: "status", Data: gin.H{"status": "paid"}})
	hub.Push(keyFor("t1", "o1"), ssex.Event{ID: "9", Name: "status", Data: gin.H{"status": "pending"}})
	hub.Push(keyFor("t1", "o1"), ssex.Event{ID: "12", Name: "status", Data: gin.H{"status": "delivered"}})

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
		load: func(_ context.Context, _ resource) (int64, gin.H, error) {
			return 1, gin.H{"status": "pending"}, nil
		},
		terminal: func(e ssex.Event) bool { return e.ID == "5" },
	}

	server := httptest.NewServer(newEngine(deps, authedIdentity()))
	defer server.Close()

	resp, err := http.Get(server.URL + "/events/orders/o1")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	waitFor(t, "handler 完成订阅", func() bool { return hub.Online(keyFor("t1", "o1")) == 1 })

	hub.Push(keyFor("t1", "o1"), ssex.Event{Name: "status", Data: gin.H{"status": "paid"}}) // 没填 ID
	hub.Push(keyFor("t1", "o1"), ssex.Event{ID: "5", Name: "status", Data: gin.H{"status": "delivered"}})

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

// TestEmptyKeyIsRejectedBeforeSubscribe 验证认证异常的请求不会进入 Hub。
//
// 回归场景：简版模板曾读取 uid 但不检查空串。空 key 会让所有认证异常的请求
// 共享同一个队列、互相收到对方的事件——这是跨用户串流。
// 模板必须在 Subscribe 之前拒掉空 key。
func TestEmptyKeyIsRejectedBeforeSubscribe(t *testing.T) {
	t.Parallel()

	hub := ssex.NewHub()
	deps := orderEventsDeps{hub: hub, appShutdown: context.Background()}
	server := httptest.NewServer(newEngine(deps, identity{})) // 中间件不注入 uid
	defer server.Close()

	resp, err := http.Get(server.URL + "/events/orders/o1")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}

	// 关键：空 key 不得进入注册表
	if got := hub.Online(""); got != 0 {
		t.Fatalf(`Online("") = %d, want 0（空 key 进了 Hub，认证异常的请求会互相串流）`, got)
	}
	if got := hub.Online(keyFor("t1", "o1")); got != 0 {
		t.Fatalf(`Online("o1") = %d, want 0`, got)
	}
}

// TestScopeKeyHasNoCollision 验证 scopeKey 的长度前缀编码不会碰撞。
//
// 直接 tenantID + ":" + orderID 时，("a:b","c") 与 ("a","b:c") 会得到同一个 key，
// 两个不相关的资源就此共享事件队列。
func TestScopeKeyHasNoCollision(t *testing.T) {
	t.Parallel()

	first := resource{tenantID: "a:b", internalID: "c"}.scopeKey()
	second := resource{tenantID: "a", internalID: "b:c"}.scopeKey()

	if first == second {
		t.Fatalf("scopeKey 碰撞: %q == %q（含分隔符的取值必须产生不同 key）", first, second)
	}
}

// collectIDs 读一条真实 SSE 连接直到 close 事件，返回收到的事件 id 序列。
func collectIDs(t *testing.T, body io.Reader) []string {
	t.Helper()

	var ids []string
	for msg, err := range ssex.Decode(body) {
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		ids = append(ids, msg.ID)
		if msg.Name == "close" {
			break
		}
	}

	return ids
}

// TestTenantCannotReachOtherTenantOrder 验证跨租户隔离，走的是真实 handler 链路：
// 从 gin.Context 取 tenant → 资源授权 → 计算 scoped key → Subscribe → 同作用域读快照。
//
// 两个租户用的是**同一个 orderID**。若 key 不带租户作用域（直接用 orderID），
// 租户 B 的连接会收到租户 A 的订单状态。
//
// 负向断言必须读租户 B 的**真实连接**：早先的版本在推送之后另开一个订阅来检查，
// 而 Hub 不重放历史事件，那个通道天然为空——无论隔离是否生效都会通过。
// 这里给两个租户各推一条自己的终态事件，于是两条流都有明确终止点，
// 可以完整读出各自收到的 id 序列再做断言。
func TestTenantCannotReachOtherTenantOrder(t *testing.T) {
	t.Parallel()

	hub := ssex.NewHub(ssex.WithQueueSize(4))
	deps := orderEventsDeps{
		hub:         hub,
		appShutdown: context.Background(),
		authorize: func(_ context.Context, id identity, orderID string) (resource, error) {
			return resource{tenantID: id.tenant, internalID: orderID}, nil
		},
		load: func(_ context.Context, res resource) (int64, gin.H, error) {
			return 1, gin.H{"tenant": res.tenantID}, nil
		},
		// 两个租户各自的终态：A 是 9，B 是 8
		terminal: func(e ssex.Event) bool { return e.ID == "9" || e.ID == "8" },
	}

	// 两个租户，同一个 orderID
	serverA := httptest.NewServer(newEngine(deps, identity{uid: "uA", tenant: "tenantA"}))
	defer serverA.Close()
	serverB := httptest.NewServer(newEngine(deps, identity{uid: "uB", tenant: "tenantB"}))
	defer serverB.Close()

	respA, err := http.Get(serverA.URL + "/events/orders/100")
	if err != nil {
		t.Fatalf("租户 A GET failed: %v", err)
	}
	defer func() { _ = respA.Body.Close() }()

	respB, err := http.Get(serverB.URL + "/events/orders/100")
	if err != nil {
		t.Fatalf("租户 B GET failed: %v", err)
	}
	defer func() { _ = respB.Body.Close() }()

	keyA := keyFor("tenantA", "100")
	keyB := keyFor("tenantB", "100")
	if keyA == keyB {
		t.Fatalf("两个租户算出了同一个 key: %q", keyA)
	}
	waitFor(t, "两个租户都完成订阅", func() bool {
		return hub.Online(keyA) == 1 && hub.Online(keyB) == 1
	})

	// A 收到 rev 9（终态），B 收到它自己的 rev 8（终态）
	if delivered, _ := hub.Push(keyA, ssex.Event{ID: "9", Name: "status", Data: gin.H{"status": "paid"}}); delivered != 1 {
		t.Fatalf("Push 到 keyA delivered = %d, want 1", delivered)
	}
	if delivered, _ := hub.Push(keyB, ssex.Event{ID: "8", Name: "status", Data: gin.H{"status": "shipped"}}); delivered != 1 {
		t.Fatalf("Push 到 keyB delivered = %d, want 1", delivered)
	}

	gotA := collectIDs(t, respA.Body)
	gotB := collectIDs(t, respB.Body)

	// 各自只应看到 自己的快照(1) → 自己的终态 → close
	if len(gotA) != 3 || gotA[1] != "9" {
		t.Fatalf("租户 A event ids = %v, want [1 9 ...]", gotA)
	}
	if len(gotB) != 3 || gotB[1] != "8" {
		t.Fatalf("租户 B event ids = %v, want [1 8 ...]", gotB)
	}

	// 真正的负向断言：租户 B 的连接里绝不能出现租户 A 的 rev 9
	for _, id := range gotB {
		if id == "9" {
			t.Fatalf("租户 B 收到了租户 A 的事件（ids = %v）", gotB)
		}
	}
}

// TestZeroAuthResultIsRejected 验证授权实现返回零值 resource 时请求被拒。
//
// 授权实现若因 bug 返回 (resource{}, nil)，scopeKey() 会算出一个固定值，
// 所有这类异常请求就落进同一个队列、互相收到对方的事件。handler 必须在
// 订阅之前二次校验授权结果。
func TestZeroAuthResultIsRejected(t *testing.T) {
	t.Parallel()

	hub := ssex.NewHub()
	deps := orderEventsDeps{
		hub:         hub,
		appShutdown: context.Background(),
		// 模拟有 bug 的授权实现：既不报错也不返回有效标识
		authorize: func(context.Context, identity, string) (resource, error) {
			return resource{}, nil
		},
	}

	server := httptest.NewServer(newEngine(deps, authedIdentity()))
	defer server.Close()

	resp, err := http.Get(server.URL + "/events/orders/o1")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	// 关键：零值授权结果算出的固定 key 不得进入注册表
	if got := hub.Online(resource{}.scopeKey()); got != 0 {
		t.Fatalf("Online(%q) = %d, want 0（零值授权结果进了 Hub）", resource{}.scopeKey(), got)
	}
}
