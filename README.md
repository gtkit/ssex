# SSE Package

`github.com/gtkit/ssex` 提供 Server-Sent Events 的服务端写入与上游解码能力。

入口只用标准库的 `http.ResponseWriter` 与 `*http.Request`，net/http、gin、chi 都可直接使用；唯一的第三方依赖是 `github.com/gtkit/json/v2`。

## 1. 能力总览

| 能力 | API |
|---|---|
| 写响应头并立即刷给客户端 | `Stream.Start()` / `Writer.WriteHeaders()` |
| 命名事件 / 带 id 事件 / data-only 帧 | `Event` / `EventWithID` / `Data` / `Send` |
| 保活 | `Comment` / `Ping` / `Heartbeat` |
| 重连间隔建议 | `Retry` |
| 起流状态判断 | `Started` |
| 终止流并抑制客户端自动重连 | `Close` |
| 错误分类 | `ErrClientGone` / `ErrStreamClosed` |
| 单帧写超时 | `WithWriteTimeout` |
| 解析上游 SSE 流（转发大模型输出） | `Decode` |
| 在线连接注册表与带外推送 | `Hub` |
| 读取客户端重连位点 | `LastEventID` |

针对长连接做的事：起流时清零 `http.Server.WriteTimeout`（否则待支付订单、大模型长响应会在全局超时到期时被服务端 RST）、每帧设置独立写截止时间、下发 `X-Accel-Buffering: no` 关闭 nginx 缓冲、HTTP/2 下按 RFC 9113 省略 `Connection` 头、`Flush` 错误在当帧就暴露。

帧安全：`id` / `event` 字段值含换行或 NUL 时报错且不写字节；注释与原样透传载荷中的 `CRLF` / 孤立 `CR` 归一后逐行加前缀，内容无法逃出所在字段伪造帧。

## 2. 包结构

当前主要有两层：

### 2.1 `Writer`

文件：

- [writer.go](./writer.go)

职责：

- 最底层 SSE 帧写入
- 写响应头
- 写 `event/data`
- 写 `comment`
- 写 `retry`

适合：

- 你需要完全自己控制输出顺序
- 你明确知道什么时候开始响应

### 2.2 `Stream`

文件：

- [stream.go](./stream.go)

职责：

- 在 `Writer` 之上增加一层业务友好的能力
- 首次输出时自动写 Header
- 提供 `Started()` 状态
- 提供 `Ping()` / `Error()` / `Comment()` / `Retry()` 这些标准辅助方法

适合：

- 大多数业务 SSE 场景
- 比如订单状态流、LLM 流式输出

当前建议：

- **业务代码优先使用 `Stream`**
- 只有极少数需要完全手控底层帧顺序的场景，才直接用 `Writer`

### 2.3 其他文件

- [decode.go](./decode.go)：上游 SSE 解码器（`Decode` / `Message`）
- [hub.go](./hub.go)：在线连接注册表与带外推送（`Hub`）
- [event.go](./event.go)：待推送的事件值（`Event`）
- [errors.go](./errors.go)：错误哨兵与分类（`ErrClientGone` / `ErrStreamClosed`）
- [options.go](./options.go)：Functional Options（`WithWriteTimeout`）

## 3. 快速开始

### 3.1 最小示例

```go
package demo

import (
    "net/http"
    "time"

    "github.com/gin-gonic/gin"
    "github.com/gtkit/ssex"
)

func StreamDemo(c *gin.Context) {
    stream := ssex.NewStream(c.Writer, c.Request)

    // 首次 Event 会自动写 text/event-stream 头
    if err := stream.Event("status", gin.H{
        "status": "pending",
    }); err != nil {
        return
    }

    if err := stream.Ping(time.Now()); err != nil {
        return
    }

    if err := stream.Event("status", gin.H{
        "status": "done",
        "final":  true,
    }); err != nil {
        return
    }
}
```

### 3.2 net/http 原生

```go
func StreamDemo(w http.ResponseWriter, r *http.Request) {
    stream := ssex.NewStream(w, r)

    if err := stream.Event("status", map[string]string{"status": "pending"}); err != nil {
        return
    }
    _ = stream.Close(map[string]string{"reason": "done"})
}
```

## 4. API 说明

### 4.1 `NewStream(w, r)`

创建一个业务层友好的 SSE 输出器。

```go
stream := ssex.NewStream(c.Writer, c.Request)
```

### 4.2 `stream.Event(name, payload)`

发送具名事件。

```go
_ = stream.Event("status", gin.H{
    "status": "pending",
})
```

输出类似：

```text
event: status
data: {"status":"pending"}

```

### 4.3 `stream.Ping(at)`

发送标准保活注释帧。

```go
_ = stream.Ping(time.Now())
```

输出类似：

```text
: ping 2026-03-30T10:00:00Z

```

### 4.4 `stream.Error(payload)`

发送标准业务错误事件。

```go
_ = stream.Error(gin.H{
    "error": "order not found",
})
```

输出类似：

```text
event: error
data: {"error":"order not found"}

```

注意：

- 这里是**服务端业务事件**
- 不是浏览器 `EventSource.onerror` 那个网络层错误回调

### 4.5 `stream.Data(payload)`

发送 data-only 帧（无 `event:` 行），适合 OpenAI 风格流式接口。

普通 payload 会经 `github.com/gtkit/json/v2` 序列化：

```go
_ = stream.Data(gin.H{"delta": "hello"})
```

需要字面输出 `[DONE]` 这类非 JSON 哨兵时，用 `ssex.Raw`：

```go
_ = stream.Data(ssex.Raw("[DONE]"))
```

### 4.6 `stream.Comment(text)`

发送一条 SSE 注释帧。

```go
_ = stream.Comment("keepalive")
```

输出类似：

```text
: keepalive

```

用途：

- 保活
- 调试
- 某些代理环境下防止长时间空闲断开

### 4.7 `stream.Retry(milliseconds)`

发送 SSE 的 `retry` 指令，提示客户端后续重连间隔。

```go
_ = stream.Retry(3000)
```

输出类似：

```text
retry: 3000

```

用途：

- 给浏览器原生 `EventSource` 提供重连节奏建议

### 4.8 `stream.Started()`

判断当前响应是否已经开始输出。

这个方法在“业务失败时还想回普通 JSON”的场景里很有用。

例如：

```go
if err != nil {
    if !stream.Started() {
        // 还能回普通 JSON
        c.JSON(http.StatusBadRequest, ...)
        return
    }
    // 已经开始 SSE 输出，只能继续发 SSE error
    _ = stream.Error(gin.H{"error": "internal error"})
    return
}
```

### 4.9 `stream.Close(payload)`

终止本轮推送：发送一条 `event: close` 事件，并拒绝后续所有写入。

**为什么必须有这一步**：浏览器 `EventSource` 在服务端正常结束流后会按重连间隔**自动重连**。订单进入终态、LLM 输出结束后直接 `return`，前端会马上重连，形成无限循环。

```go
// 服务端：终态后终止流
_ = stream.Close(gin.H{"reason": "delivered"})
```

```js
// 前端：收到 close 事件后主动断开，否则仍会重连
es.addEventListener('close', () => es.close());
```

`Close` 只终结本流的写入许可，不关闭 HTTP 连接（连接在 handler 返回时结束）。
终止后任何写入返回 `ssex.ErrStreamClosed`，重复 `Close` 同样返回它。

### 4.10 错误判定

写方法的错误分三类，用 `errors.Is` 区分：

| 判定 | 含义 | 建议处理 |
|---|---|---|
| `errors.Is(err, ssex.ErrClientGone)` | 客户端已断开 | 静默结束，并取消上游请求（如正在进行的大模型调用）以停止计费 |
| `errors.Is(err, ssex.ErrStreamClosed)` | 自己已 `Close` 过 | 调用顺序问题，检查业务逻辑 |
| 其余 | 真实写失败（写超时、序列化失败等） | 记录日志 / 告警 |

`ErrClientGone` 的错误链保留原因，因此 `errors.Is(err, context.Canceled)` 同样成立。

```go
for chunk := range upstream {
    if err := stream.Data(chunk); err != nil {
        if errors.Is(err, ssex.ErrClientGone) {
            cancelUpstream() // 前端已关页面，别再为它烧 token
            return
        }
        logger.Error("sse 写入失败", err)
        return
    }
}
```

注意：客户端读取过慢导致的**写超时不算断开**，归入真实写失败。

### 4.11 Option

```go
stream := ssex.NewStream(c.Writer, c.Request, ssex.WithWriteTimeout(30*time.Second))
```

| Option | 默认值 | 说明 |
|---|---|---|
| `WithWriteTimeout(d)` | 10s | 单帧写入的截止时长；非正值忽略。弱网移动端可放大，内网可收紧以更快发现死连接 |

该截止时间只作用于单帧，长连接整体不受它限制——长连接本身靠起流时清零 `http.Server.WriteTimeout` 保活。

### 4.12 `stream.Heartbeat(ctx, interval)`

长时间无数据的流（等支付结果、等登录态）必须有心跳，否则代理层会按空闲连接断开。

```go
go func() { _ = stream.Heartbeat(c.Request.Context(), 15*time.Second) }()
```

阻塞运行，启动时机与所在 goroutine 由你控制。`ctx` 取消返回 `ctx` 错误，客户端断开返回 `ErrClientGone`，流已 `Close` 返回 `ErrStreamClosed`；`interval` 非正立即报错。

### 4.13 `ssex.Decode(r)` — 解析上游 SSE

把上游大模型的 `text/event-stream` 解码成结构化帧，用于转发：

```go
for msg, err := range ssex.Decode(resp.Body) {
    if err != nil {
        return err
    }
    if string(msg.Data) == "[DONE]" {
        break
    }
    if err := stream.Data(ssex.Raw(string(msg.Data))); err != nil {
        return err
    }
}
```

`Message` 字段：`ID`、`Name`（`event:` 字段，空为默认事件）、`Data []byte`、`Retry`。

严格按 WHATWG event stream 规范：

- 行分隔符支持 `CRLF` / `CR` / `LF`
- 字段值**只剥掉冒号后的一个空格**——大模型的增量 token 常以空格开头，多剥会损坏拼出的文本
- `Data` 原样保留上游字节：`[DONE]` 这类非 JSON 哨兵可直接比对，增量 token 的前后空格完整保留
- 多行 `data:` 以 `\n` 拼接；流首 BOM、注释行与未知字段跳过
- 帧是否产出以本帧是否出现过 `data` 字段为准（与浏览器 `EventSource` 一致），值为空串也照常产出
- `ID` 与 `Retry` 是连接级状态，按规范跨帧沿用（`The buffer does not get reset`），转发时 id 连续性不会断；`Name` 每帧独立
- 流尾残留的不完整帧（缺结尾空行）按规范丢弃，与浏览器一致——连接被掐断时残留字节通常是截断的半条 JSON
- 单帧上限 1MB，超限报错而非静默截断
- 读到流尾正常结束

### 4.14 `ssex.NewHub()` — 带外推送

产生事件的 goroutine 不持有连接时（支付回调推订单状态、后台踢登录态）用它：

```go
hub := ssex.NewHub()                        // 无后台 goroutine，不需要关闭

// 推送方（支付回调、MQ 消费者……）
delivered, dropped := hub.Push(uid, ssex.Event{Name: "status", Data: order})
if delivered == 0 {
    // 没人在线，落库让客户端下次拉快照
}
```

连接侧的 handler 模板：

```go
func Subscribe(c *gin.Context) {
    uid := c.GetString("uid")
    stream := ssex.NewStream(c.Writer, c.Request)
    stream.Start() // 必须先起流：纯推送型 handler 在第一条事件到来前不写字节，
                   // 不起流则前端一直等响应头、迟迟不触发 onopen

    events, release := hub.Subscribe(uid)
    defer release()

    go func() { _ = stream.Heartbeat(stream.Context(), 15*time.Second) }()

    for {
        select {
        case <-stream.Context().Done():
            return
        case e := <-events:
            if err := stream.Send(e); err != nil {
                return
            }
        }
    }
}
```

| 方法 | 说明 |
|---|---|
| `Subscribe(key)` | 注册连接，返回事件队列与注销函数（必须 `defer` 调用，重复调用安全） |
| `Push(key, event)` | 定向投递，返回 `(delivered, dropped)` |
| `Broadcast(event)` | 投给所有在线连接，返回 `(delivered, dropped)` |
| `Online(key)` | 该 key 当前在线连接数 |
| `WithQueueSize(n)` | 每连接队列容量，默认 32，非正值忽略 |

三条使用约束：

1. **先 `Start()` 再进消费循环**：纯推送型 handler 在第一条事件到来前不写字节，起流才能让前端及时拿到响应头并触发 `onopen`。
2. **投递与写出分离**：`Push` 只把事件放进队列，写出由持有连接的 handler 完成，因此写动作始终发生在 handler 自己的 goroutine、`ResponseWriter` 仍有效的窗口内。
3. **队列满即丢弃**：`dropped` 告诉你丢了多少，`Push` 立即返回。SSE 是状态推送，下一条状态本身就比重投的旧状态更新；要求必达的数据请落库，让客户端重连后拉快照。

消费循环用 `stream.Context().Done()` 退出，队列由 GC 回收。

## 5. 常见模式

### 5.1 转发大模型输出

先 `session`，中间连续 `chunk`，结束 `done`，出错 `error`；上游用 `Decode` 读，下游用 `Data` / `Send` 写。

要点：

- 客户端断开时（`ErrClientGone`）必须取消上游请求，否则前端关了页面你还在为它烧 token
- 结束时用 `Close` 终止流，前端 `es.close()`，避免自动重连
- 需要断线续传就用 `EventWithID` 带上 id，客户端重连时经 `LastEventID(c)` 读回起点，由业务决定从哪一条开始续推

### 5.2 订单状态流

先发一条 `status` 快照，后续由支付回调经 `Hub.Push` 带外推送，定期注释心跳，终态后 `Close`。

要点：

- 快照先发：客户端可能在状态变更之后才连上来
- 心跳必须有：等支付的连接可能几分钟没有数据
- 终态后 `Close`，否则 `EventSource` 会一直重连
- `Push` 返回 `delivered == 0` 说明没人在等，把状态落库即可

## 6. 推荐实践

### 6.1 业务层统一使用 `Stream`

推荐：

```go
stream := ssex.NewStream(c.Writer, c.Request)
_ = stream.Event("status", payload)
```

不推荐每个模块都自己手写：

- `WriteHeaders`
- `started` 标志
- 心跳格式
- `error` 格式

### 6.2 事件名保持稳定

SSE 的事件名本质上就是客户端协议的一部分。

一旦对外开放，尽量不要频繁改：

- `status`
- `ping`
- `error`
- `chunk`
- `done`

### 6.3 `error` 事件和网络错误分开处理

必须区分两种错误：

1. 服务端发出的 `event: error`
2. 浏览器/客户端自己的连接错误回调

这两者不是一个层级。

### 6.4 终态后主动结束流

如果业务天然有终态：

- 订单 `delivered/closed/...`
- LLM `done`

服务端在终态后调用 `stream.Close(...)`，前端监听 `close` 事件并执行 `es.close()`。
只 `return` 而不发终止事件，`EventSource` 会自动重连，形成无限重连。

### 6.5 让 SSE 路由绕过压缩中间件

压缩中间件（如 gzip）会先把响应攒进自己的缓冲区，`Flush` 只刷到它的下一层，帧于是停在中间层不落地——表现是前端长时间收不到任何事件，直到缓冲被填满才一次性到达。

给 SSE 路由单独注册路由组、让它跳过压缩中间件即可。反向代理侧同理：本包已下发 `X-Accel-Buffering: no`（nginx 据此关闭该响应的缓冲），其他网关按各自方式关闭响应缓冲。

### 6.6 不要把双向交互硬塞进 SSE

SSE 只适合：

- 服务端 -> 客户端

如果你的业务需要：

- 客户端持续发消息
- 双向实时交互
- 房间/广播/多人会话

那应该考虑 WebSocket，而不是继续堆 SSE。

## 7. 测试

当前公共层测试：

- [writer_test.go](./writer_test.go)
- [stream_test.go](./stream_test.go)
- [frames_test.go](./frames_test.go)
- [errors_test.go](./errors_test.go)
- [options_test.go](./options_test.go)
- [concurrent_test.go](./concurrent_test.go)
- [decode_test.go](./decode_test.go)
- [hub_test.go](./hub_test.go)
- [crossverify_test.go](./crossverify_test.go)
- [example_test.go](./example_test.go)

覆盖点包括：

- 写头（含 HTTP/2 不下发 `Connection`）、起流即刷出响应头
- 普通事件、带 `id` 事件、data-only 帧与 `Raw` 哨兵
- 帧注入防护：字段值含换行 / NUL 被拒，注释与 raw data 中的孤立 `\r` 被归一
- `retry` 值域校验
- context cancel 判定为 `ErrClientGone`，序列化失败不误判
- 写超时可配置
- 自动起流、`Close` 终止后拒绝写入
- ping 注释心跳、error、comment、retry、`Heartbeat` 间隔与退出条件
- 上游解码：三种换行、只剥一个空格、多行 data、`[DONE]`、id/retry 合法与非法、末帧无空行、读取错误、超长帧
- Hub：定向投递、同 key 多连接、注销幂等、广播、在线数、满队列丢弃计数、队列容量配置
- 并发写入与 Hub 并发注册/推送（`-race`）、写入与解码热路径 benchmark
- 交叉验证：写侧输出交由解码器解回（含两处帧注入交给解析器判决）、规范章节的官方示例向量、真实连接上客户端断开触发 `ErrClientGone`、Hub 与 Stream 真实连接端到端、真实 HTTP/2 连接

## 8. 依赖与版本

- 直接依赖：`github.com/gtkit/json/v2`
- 入口只用标准库 HTTP 接口，因此 net/http、gin、chi 共用同一套实现
- 版本按 [SemVer](https://semver.org/lang/zh-CN/) 走，破坏性变更在 [CHANGELOG.md](./CHANGELOG.md) 以 ⚠ 标注并附迁移写法
