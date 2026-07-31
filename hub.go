package ssex

import "sync"

// defaultQueueSize 是每个连接事件队列的默认容量。
const defaultQueueSize = 32

// HubOption 配置 Hub。
type HubOption func(*hubOptions)

type hubOptions struct {
	queueSize int
}

// WithQueueSize 设置每个连接事件队列的容量，默认 32；非正值被忽略。
// 队列满时 Push / Broadcast 丢弃该连接的这一条事件，不阻塞推送方。
func WithQueueSize(n int) HubOption {
	return func(o *hubOptions) {
		if n <= 0 {
			return
		}
		o.queueSize = n
	}
}

// Hub 按 key 管理在线 SSE 连接，支持从任意 goroutine 定向推送或广播。
// 典型用途是带外推送：支付回调推订单状态、后台踢下线推登录态——
// 产生事件的 goroutine 并不持有连接。
//
// 并发安全。Hub 自身不写连接：Push 只把事件投进目标连接的有界队列，
// 由持有连接的 handler 取出后写出。反过来（Hub 直接写）在 handler 返回后
// 会踩到已失效的 ResponseWriter。
//
// Hub 没有后台 goroutine，因此不需要关闭。
type Hub struct {
	mu        sync.RWMutex
	subs      map[string]map[*subscriber]struct{}
	queueSize int
}

// subscriber 是一条在线连接的事件队列。用指针身份做 key，
// 因此同一 key 下的多个连接互不干扰。
type subscriber struct {
	ch chan Event
}

// NewHub 创建一个连接注册表。
func NewHub(opts ...HubOption) *Hub {
	o := hubOptions{queueSize: defaultQueueSize}
	for _, opt := range opts {
		opt(&o)
	}

	return &Hub{
		subs:      make(map[string]map[*subscriber]struct{}),
		queueSize: o.queueSize,
	}
}

// Subscribe 以 key 注册当前连接，返回事件队列与注销函数。
// 同一 key 可注册多个连接（同一用户多标签页 / 多端），各自收到完整副本。
//
// 调用方必须 defer 调用 release（重复调用安全）。事件队列不会被 Hub 关闭：
// 关闭它会与并发的 Push 构成"向已关闭 channel 发送"的 panic 竞态。
// 用连接上下文结束消费循环：
//
//	stream := ssex.NewStream(c)
//	stream.Start() // 必须先起流:否则响应头要等到第一条推送才发出,前端迟迟不触发 onopen
//
//	events, release := hub.Subscribe(uid)
//	defer release()
//	for {
//	    select {
//	    case <-stream.Context().Done():
//	        return
//	    case e := <-events:
//	        if err := stream.Send(e); err != nil {
//	            return
//	        }
//	    }
//	}
func (h *Hub) Subscribe(key string) (events <-chan Event, release func()) {
	sub := &subscriber{ch: make(chan Event, h.queueSize)}

	h.mu.Lock()
	if h.subs[key] == nil {
		h.subs[key] = make(map[*subscriber]struct{})
	}
	h.subs[key][sub] = struct{}{}
	h.mu.Unlock()

	return sub.ch, func() { h.unsubscribe(key, sub) }
}

// Push 把 e 投给 key 下所有在线连接，返回投递成功与被丢弃的连接数。
// key 不在线时返回 (0, 0)，不报错、不阻塞。
//
// 目标队列已满时丢弃该连接的这一条，既不阻塞也不重试：SSE 是状态推送而非
// 可靠队列，让一个卡住的浏览器拖住支付回调这类关键路径是更坏的结果，
// 而下一条状态本身就比重投的旧状态更新。dropped 交给调用方决定是否告警。
func (h *Hub) Push(key string, e Event) (delivered, dropped int) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return deliver(h.subs[key], e)
}

// Broadcast 把 e 投给所有在线连接，返回投递成功与被丢弃的连接数。
func (h *Hub) Broadcast(e Event) (delivered, dropped int) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, subs := range h.subs {
		d, dr := deliver(subs, e)
		delivered += d
		dropped += dr
	}

	return delivered, dropped
}

// Online 返回 key 当前的在线连接数，可用于判断"还有没有人在等这个结果"。
func (h *Hub) Online(key string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return len(h.subs[key])
}

// unsubscribe 把连接从注册表摘除;key 下已无连接时连 key 一起删掉,避免 map 无限增长。
func (h *Hub) unsubscribe(key string, sub *subscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()

	subs := h.subs[key]
	if subs == nil {
		return
	}
	delete(subs, sub)
	if len(subs) == 0 {
		delete(h.subs, key)
	}
}

// deliver 非阻塞地把 e 投给一组连接。
func deliver(subs map[*subscriber]struct{}, e Event) (delivered, dropped int) {
	for sub := range subs {
		select {
		case sub.ch <- e:
			delivered++
		default:
			dropped++
		}
	}

	return delivered, dropped
}
