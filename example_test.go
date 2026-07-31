package ssex_test

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/gtkit/ssex"
)

// newExampleStream 构造一个可打印输出的 Stream，仅供 Example 使用。
// 真实代码里传 handler 的 w 与 r（gin 里是 c.Writer 与 c.Request）。
func newExampleStream(opts ...ssex.Option) (*ssex.Stream, *httptest.ResponseRecorder) {
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/sse", nil)

	return ssex.NewStream(recorder, req, opts...), recorder
}

// Example_lLMRelay 演示把大模型的流式输出转发给前端：
// data-only 帧 + [DONE] 哨兵，并按错误类别决定是否取消上游请求。
func Example_lLMRelay() {
	stream, recorder := newExampleStream()

	for _, delta := range []string{"Hel", "lo"} {
		if err := stream.Data(map[string]string{"delta": delta}); err != nil {
			if errors.Is(err, ssex.ErrClientGone) {
				return // 前端已断开：停止转发并取消上游大模型请求，避免继续计费
			}
			fmt.Println("write failed:", err)

			return
		}
	}
	_ = stream.Data(ssex.Raw("[DONE]"))

	fmt.Print(recorder.Body.String())
	// Output:
	// data: {"delta":"Hel"}
	//
	// data: {"delta":"lo"}
	//
	// data: [DONE]
}

// Example_orderStatus 演示订单状态推送：状态事件 + 终态后终止流。
// 前端须监听 close 事件并调用 EventSource.close()，否则流结束后会自动重连。
func Example_orderStatus() {
	stream, recorder := newExampleStream()

	_ = stream.Event("status", map[string]string{"status": "pending"})
	_ = stream.Event("status", map[string]string{"status": "delivered"})
	_ = stream.Close(map[string]string{"reason": "delivered"})

	fmt.Print(recorder.Body.String())
	// Output:
	// event: status
	// data: {"status":"pending"}
	//
	// event: status
	// data: {"status":"delivered"}
	//
	// event: close
	// data: {"reason":"delivered"}
}

// ExampleDecode 演示解析上游大模型的 SSE 流（转发链路的读侧）。
func ExampleDecode() {
	upstream := strings.NewReader(
		"data: {\"delta\":\"Hel\"}\n\n" +
			"data: {\"delta\":\"lo\"}\n\n" +
			"data: [DONE]\n\n",
	)

	for msg, err := range ssex.Decode(upstream) {
		if err != nil {
			fmt.Println("decode failed:", err)

			return
		}
		if string(msg.Data) == "[DONE]" {
			break // 真实代码里此处 stream.Close(...) 收尾
		}
		fmt.Printf("%s\n", msg.Data)
	}
	// Output:
	// {"delta":"Hel"}
	// {"delta":"lo"}
}

// ExampleHub_Push 演示带外推送：支付回调把订单状态推给正在等待的连接。
func ExampleHub_Push() {
	hub := ssex.NewHub()

	// 连接侧：handler 注册自己，退出时注销
	events, release := hub.Subscribe("u1")
	defer release()

	// 推送侧：支付回调，与持有连接的 handler 不在同一个 goroutine
	delivered, dropped := hub.Push("u1", ssex.Event{Name: "status", Data: "paid"})
	fmt.Printf("delivered=%d dropped=%d online=%d\n", delivered, dropped, hub.Online("u1"))

	// 连接侧：取出后写给客户端（真实代码里是 stream.Send(e)）
	e := <-events
	fmt.Printf("%s %v\n", e.Name, e.Data)
	// Output:
	// delivered=1 dropped=0 online=1
	// status paid
}

// ExampleWithWriteTimeout 演示为弱网客户端放宽单帧写超时（默认 10s）。
func ExampleWithWriteTimeout() {
	stream, recorder := newExampleStream(ssex.WithWriteTimeout(30 * time.Second))

	_ = stream.Event("status", map[string]string{"status": "pending"})

	fmt.Print(recorder.Body.String())
	// Output:
	// event: status
	// data: {"status":"pending"}
}
