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

// TestHubDropsWhenQueueFull 验证慢消费者只丢自己的事件,不阻塞推送方。
func TestHubDropsWhenQueueFull(t *testing.T) {
	t.Parallel()
	hub := NewHub(WithQueueSize(1))

	_, release := hub.Subscribe("u1")
	defer release()

	if delivered, dropped := hub.Push("u1", Event{Name: "1"}); delivered != 1 || dropped != 0 {
		t.Fatalf("first Push() = (%d, %d), want (1, 0)", delivered, dropped)
	}
	for i := range 2 {
		if delivered, dropped := hub.Push("u1", Event{}); delivered != 0 || dropped != 1 {
			t.Fatalf("Push() #%d = (%d, %d), want (0, 1)", i+2, delivered, dropped)
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

			// 不消费的前提下恰好能投递 want 条,第 want+1 条被丢弃
			for i := range tt.want {
				if delivered, _ := hub.Push("u1", Event{}); delivered != 1 {
					t.Fatalf("Push() #%d delivered = %d, want 1", i+1, delivered)
				}
			}
			if delivered, dropped := hub.Push("u1", Event{}); delivered != 0 || dropped != 1 {
				t.Fatalf("Push() beyond capacity = (%d, %d), want (0, 1)", delivered, dropped)
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
