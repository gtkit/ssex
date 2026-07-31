package ssex

// Event 是一条待推送给前端的 SSE 事件，用于 Hub 投递与 Stream.Send。
//
// 与解码方向的 Message 相对：Message 描述"从上游读到的一帧"，Data 是原始字节；
// Event 描述"要写给前端的一帧"，Data 由写入层序列化。
type Event struct {
	// ID 非空时写出 `id:` 行，供客户端重连时通过 Last-Event-ID 回传。
	ID string
	// Name 是事件名；为空时写出 data-only 帧，前端经 EventSource 的 onmessage 接收。
	Name string
	// Data 是事件载荷，自动 JSON 序列化；Raw(...) 原样透传。
	Data any
}
