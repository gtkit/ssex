package ssex

import (
	"fmt"
	"sync"
	"testing"
)

func TestHubPush(t *testing.T) {
	t.Parallel()
	hub := NewHub()

	events, release := hub.Subscribe("u1")
	defer release()

	delivered, dropped := hub.Push("u1", Event{Name: "status", Data: "paid"})
	if delivered != 1 || dropped != 0 {
		t.Fatalf("Push() = (%d, %d), want (1, 0)", delivered, dropped)
	}
	if got := <-events; got.Name != "status" || got.Data != "paid" {
		t.Fatalf("event = %+v", got)
	}
}

func TestHubMultipleConnectionsPerKey(t *testing.T) {
	t.Parallel()
	hub := NewHub()

	const conns = 3
	queues := make([]<-chan Event, conns)
	for i := range conns {
		events, release := hub.Subscribe("u1")
		defer release()
		queues[i] = events
	}

	if got := hub.Online("u1"); got != conns {
		t.Fatalf("Online() = %d, want %d", got, conns)
	}

	delivered, dropped := hub.Push("u1", Event{Name: "status"})
	if delivered != conns || dropped != 0 {
		t.Fatalf("Push() = (%d, %d), want (%d, 0)", delivered, dropped, conns)
	}
	for i, queue := range queues {
		if got := <-queue; got.Name != "status" {
			t.Fatalf("queue[%d] event = %+v", i, got)
		}
	}
}

func TestHubUnsubscribeIsIdempotent(t *testing.T) {
	t.Parallel()
	hub := NewHub()

	_, release := hub.Subscribe("u1")
	release()
	release() // 幂等:重复注销不得 panic

	if got := hub.Online("u1"); got != 0 {
		t.Fatalf("Online() = %d, want 0", got)
	}
	if delivered, dropped := hub.Push("u1", Event{}); delivered != 0 || dropped != 0 {
		t.Fatalf("Push() after release = (%d, %d), want (0, 0)", delivered, dropped)
	}
}

func TestHubPushToOfflineKey(t *testing.T) {
	t.Parallel()
	hub := NewHub()

	if delivered, dropped := hub.Push("nobody", Event{}); delivered != 0 || dropped != 0 {
		t.Fatalf("Push() = (%d, %d), want (0, 0)", delivered, dropped)
	}
	if got := hub.Online("nobody"); got != 0 {
		t.Fatalf("Online() = %d, want 0", got)
	}
}

func TestHubBroadcast(t *testing.T) {
	t.Parallel()
	hub := NewHub()

	_, release1 := hub.Subscribe("u1")
	defer release1()
	_, release2 := hub.Subscribe("u2")
	defer release2()
	_, release3 := hub.Subscribe("u2")
	defer release3()

	delivered, dropped := hub.Broadcast(Event{Name: "notice"})
	if delivered != 3 || dropped != 0 {
		t.Fatalf("Broadcast() = (%d, %d), want (3, 0)", delivered, dropped)
	}
}

// TestHubLatestWinsOnFullQueue 验证队列满时挤掉最旧的一条,让最新状态一定送达。
//
// 回归场景:此前实现丢弃新事件、保留旧事件——订单的 paid、登录的 logged_in
// 会被丢掉,客户端却继续收到旧的 pending,停在错误状态上。
func TestHubLatestWinsOnFullQueue(t *testing.T) {
	t.Parallel()
	hub := NewHub(WithQueueSize(1))

	events, release := hub.Subscribe("u1")
	defer release()

	if delivered, dropped := hub.Push("u1", Event{Name: "status", Data: "pending"}); delivered != 1 || dropped != 0 {
		t.Fatalf("first Push() = (%d, %d), want (1, 0)", delivered, dropped)
	}
	if delivered, dropped := hub.Push("u1", Event{Name: "status", Data: "paid"}); delivered != 1 || dropped != 1 {
		t.Fatalf("second Push() = (%d, %d), want (1, 1)", delivered, dropped)
	}

	// 消费方读到的必须是最新状态,而不是被挤掉的旧状态
	if got := <-events; got.Data != "paid" {
		t.Fatalf("event = %+v, want the latest status paid", got)
	}
}

func TestHubKeepsOnlyLatestOnRepeatedOverflow(t *testing.T) {
	t.Parallel()
	hub := NewHub(WithQueueSize(1))

	events, release := hub.Subscribe("u1")
	defer release()

	for _, status := range []string{"s1", "s2", "s3"} {
		hub.Push("u1", Event{Data: status})
	}

	if got := <-events; got.Data != "s3" {
		t.Fatalf("event = %+v, want s3", got)
	}
	select {
	case extra := <-events:
		t.Fatalf("queue should hold exactly one event, got extra %+v", extra)
	default:
	}
}

func TestHubKeepsLatestWindow(t *testing.T) {
	t.Parallel()
	hub := NewHub(WithQueueSize(2))

	events, release := hub.Subscribe("u1")
	defer release()

	for _, status := range []string{"s1", "s2", "s3"} {
		hub.Push("u1", Event{Data: status})
	}

	for _, want := range []string{"s2", "s3"} {
		if got := <-events; got.Data != want {
			t.Fatalf("event = %+v, want %s", got, want)
		}
	}
}

func TestWithQueueSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts []HubOption
		want int
	}{
		{"未配置时用默认容量", nil, defaultQueueSize},
		{"显式容量生效", []HubOption{WithQueueSize(4)}, 4},
		{"零值被忽略", []HubOption{WithQueueSize(0)}, defaultQueueSize},
		{"负值被忽略", []HubOption{WithQueueSize(-1)}, defaultQueueSize},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			hub := NewHub(tt.opts...)

			_, release := hub.Subscribe("u1")
			defer release()

			// 不消费的前提下恰好能投递 want 条而不挤掉任何旧事件
			for i := range tt.want {
				if delivered, dropped := hub.Push("u1", Event{}); delivered != 1 || dropped != 0 {
					t.Fatalf("Push() #%d = (%d, %d), want (1, 0)", i+1, delivered, dropped)
				}
			}
			// 再一条仍然送达,代价是挤掉队首最旧的一条
			if delivered, dropped := hub.Push("u1", Event{}); delivered != 1 || dropped != 1 {
				t.Fatalf("Push() beyond capacity = (%d, %d), want (1, 1)", delivered, dropped)
			}
		})
	}
}

// TestHubConcurrent 验证注册 / 注销 / 定向推送 / 广播 / 在线数查询并发安全。
// 必须在 -race 下运行才有意义。
func TestHubConcurrent(t *testing.T) {
	t.Parallel()
	hub := NewHub(WithQueueSize(1))

	const workers = 8

	var wg sync.WaitGroup
	wg.Add(workers * 3)

	for w := range workers {
		key := fmt.Sprintf("u%d", w%3)

		go func() { // 反复注册注销
			defer wg.Done()
			for range 50 {
				_, release := hub.Subscribe(key)
				release()
			}
		}()
		go func() { // 定向推送
			defer wg.Done()
			for range 50 {
				hub.Push(key, Event{Name: "tick"})
			}
		}()
		go func() { // 广播 + 在线数查询
			defer wg.Done()
			for range 50 {
				hub.Broadcast(Event{Name: "notice"})
				_ = hub.Online(key)
			}
		}()
	}
	wg.Wait()
}
