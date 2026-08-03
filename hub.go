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
// 队列满时 Push / Broadcast 挤掉该连接队首最旧的一条，不阻塞推送方。
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
// 适用边界：Hub 面向**状态推送**——每条事件自带完整状态，队列满时挤掉旧的、
// 保留最新的（latest-wins）即可。AI token 流每一条都是文本的一部分，
// 少一条就损坏输出，应在 handler 内直接用 Stream.Data 写出，不经 Hub。
//
// 顺序契约：事件顺序在**单个连接内**按入队顺序保证。多个 goroutine 并发 Push /
// Broadcast 时，它们之间的相对顺序由调度决定，不同连接可能观察到不同顺序。
// 因此订单、登录这类状态事件应携带单调递增的 revision（放进 Event.ID 或载荷），
// 消费端按 revision 取大者、忽略更旧的值。要提供跨推送方的全局顺序，就必须把所有
// 投递串行化到单点，广播吞吐会退化为单 goroutine——而网络与客户端处理本身也不保证顺序。
//
// 容量与顺序的组合风险：latest-wins 按**到达顺序**保留最后一条，不比较业务
// revision。若同一 key 有多个并发推送方，到达顺序可能与 revision 顺序相反——
// 容量为 1 时，后到的旧版本会挤掉先到的新版本，消费端再也看不到那条新版本，
// 按 revision 过滤也救不回来（它已经不在队列里了）。
//
// 因此 WithQueueSize(1) 只在"同一 key 的推送已经串行化"时才安全。否则至少满足一条：
//   - 同一 key 的事件经单点串行化后再 Push，使到达顺序与 revision 顺序一致；
//   - 容量留大于 1，并由消费端维护 lastRev 按 revision 过滤；
//   - 终态事件到达后回读一次事实源做校准，不完全相信内存队列。
//
// 容量与限流：Hub 不做全局连接数上限、单 key 上限或 IP 限流——这些策略属于应用
// 与网关。Online 与 Push 的返回值提供了做这些决策所需的原始信号。
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
//
// mu 串行化本连接的溢出处理：Push / Broadcast 只持 Hub 的读锁，
// 多个推送方可以并发进入 deliver，而"挤掉最旧 + 放入最新"两步必须原子，
// 否则两个推送方交错会让队列里留下旧值。不同连接之间仍然并发。
type subscriber struct {
	mu sync.Mutex
	ch chan Event
}

// NewHub 创建一个连接注册表；nil Option 跳过。
func NewHub(opts ...HubOption) *Hub {
	o := hubOptions{queueSize: defaultQueueSize}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
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
//	events, release := hub.Subscribe(uid)
//	defer release()
//
//	// 订阅之后再读快照,否则两步之间的状态变更无人接收、会永久丢失
//	snapRev, snapshot, err := svc.Load(r.Context(), uid)
//	if err != nil {
//	    // 尚未起流,这里还能回普通 JSON
//	    return
//	}
//
//	stream := ssex.NewStream(w, r) // gin 里传 c.Writer, c.Request
//	if err := stream.Start(); err != nil {
//	    return // 必须先起流,否则响应头要等到第一条推送才发出,前端迟迟不触发 onopen
//	}
//	if err := stream.EventWithID(strconv.FormatInt(snapRev, 10), "status", snapshot); err != nil {
//	    return
//	}
//
//	for {
//	    select {
//	    case <-stream.Context().Done():
//	        return
//	    case e := <-events:
//	        if revisionOf(e) <= snapRev { // 快照里已经含了的旧事件,跳过
//	            continue
//	        }
//	        if err := stream.Send(e); err != nil {
//	            return
//	        }
//	    }
//	}
//
// 完整模板（含心跳收尾、应用停机、错误分界）见 README 第 9 节。
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

// Push 把 e 投给 key 下所有在线连接，返回投递成功的连接数与被挤掉的旧事件数。
// key 不在线时返回 (0, 0)，不报错、不阻塞。
//
// 目标队列已满时挤掉该连接队首最旧的一条，让 e 一定入队（latest-wins），
// 既不阻塞也不重试：状态推送里最新一条描述的就是当前状态，必须送达；
// 而让一个卡住的浏览器拖住支付回调这类关键路径是更坏的结果。
// dropped 是被挤掉的旧事件数，交给调用方决定是否告警。
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

// deliver 非阻塞地把 e 投给一组连接，队列满时按 latest-wins 挤掉最旧的一条。
func deliver(subs map[*subscriber]struct{}, e Event) (delivered, dropped int) {
	for sub := range subs {
		d, dr := sub.offer(e)
		delivered += d
		dropped += dr
	}

	return delivered, dropped
}

// offer 把 e 放进队列;队列已满时先挤掉队首最旧的一条再放入(latest-wins)。
//
// 状态推送场景里最新一条描述的是当前真实状态:丢掉 paid / logged_in / done
// 而保留旧的 pending,会让客户端停在错误状态上——这比丢掉中间过程严重得多。
// 返回 (1, 0) 表示直接入队,(1, 1) 表示挤掉一条旧事件后入队。
func (s *subscriber) offer(e Event) (delivered, dropped int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case s.ch <- e:
		return 1, 0
	default:
	}

	// 腾出一个位置。消费方可能刚好取走一条,此时无需丢弃。
	select {
	case <-s.ch:
		dropped = 1
	default:
	}

	select {
	case s.ch <- e:
		return 1, dropped
	default:
		// 容量为 0 之类的极端配置下仍放不进去,如实计为丢弃。
		return 0, dropped + 1
	}
}
