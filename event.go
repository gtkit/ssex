package ssex

// Event 是一条待推送给前端的 SSE 事件，用于 Hub 投递与 Stream.Send。
//
// 与解码方向的 Message 相对：Message 描述"从上游读到的一帧"，Data 是原始字节；
// Event 描述"要写给前端的一帧"，Data 由写入层序列化。
type Event struct {
	// ID 非空时写出 `id:` 行，供客户端重连时通过 Last-Event-ID 回传。
	// 状态类推送建议填单调递增的 revision，消费端按 revision 取大者（见 Hub 的顺序契约）。
	ID string
	// Name 是事件名；为空时写出 data-only 帧，前端经 EventSource 的 onmessage 接收。
	Name string
	// Data 是事件载荷，自动 JSON 序列化；Raw(...) 原样透传。
	//
	// 所有权：交给 Hub.Push / Hub.Broadcast 之后即视为交出所有权。同一份载荷会被
	// 该 key 下的多个连接各自序列化，且发生在它们各自的 handler goroutine 里——
	// 投递后再修改其中的 map、切片或指针内容会构成数据竞争。需要复用结构体时，
	// 投递前拷贝一份。
	Data any
}
