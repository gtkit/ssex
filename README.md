# SSE Package

`github.com/gtkit/ssex` 提供 Server-Sent Events 的服务端写入与上游解码能力。

入口只用标准库的 `http.ResponseWriter` 与 `*http.Request`，net/http、gin、chi 都可直接使用；唯一的**直接**第三方依赖是 `github.com/gtkit/json/v2`（它自身另有传递依赖）。

## 1. 能力总览

| 能力 | API |
|---|---|
| 写响应头并立即刷给客户端 | `Stream.Start()` / `Writer.WriteHeaders()` |
| 命名事件 / 带 id 事件 / data-only 帧 | `Event` / `EventWithID` / `Data` / `Send` |
| 保活 | `Comment` / `Ping` / `Heartbeat` |
| 重连间隔建议 | `Retry` |
| 起流状态判断 | `Started` |
| 终止流并抑制客户端自动重连 | `Close` |
| 错误分类 | `ErrClientGone` / `ErrStreamClosed` / `ErrInvalidArgument` / `ErrWriteTimeout` / `ErrFrameTooLarge` |
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
- [errors.go](./errors.go)：错误哨兵与分类（5 个哨兵，见 4.10）
- [options.go](./options.go)：Functional Options（`WithWriteTimeout`）

## 3. 快速开始

### 3.1 最小示例

```go
package demo

import (
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
| `ErrClientGone` | 客户端已断开 | 静默结束，并取消上游请求（如正在进行的大模型调用）以停止计费 |
| `ErrStreamClosed` | 自己已 `Close` 过 | 调用顺序问题，检查业务逻辑 |
| `ErrInvalidArgument` | 参数非法：字段值含换行/NUL、`retry` 为负、心跳间隔非正 | 编程错误；此时响应头尚未提交，可改回普通 JSON |
| `ErrWriteTimeout` | 单帧写入超过 `WithWriteTimeout` | 客户端读取过慢，连接仍活着；按需告警 |
| `ErrFrameTooLarge` | 解码时单帧超过 1MB | 上游异常或恶意，终止本次转发 |
| 其余 | 真实写失败 | 记录日志 / 告警 |

`ErrClientGone` 的错误链保留原因，因此 `errors.Is(err, context.Canceled)` 同样成立。
写超时不会被判定为 `ErrClientGone`——慢客户端不等于断开。

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

**首帧失败仍可回 JSON**：`Stream` 的写方法先构造并校验完整帧，只有构造成功才提交响应头。
因此首帧因 `ErrInvalidArgument` 或序列化失败而报错时 `Started()` 仍为 `false`，
调用方可以改用普通 JSON 响应回错。

**起流错误要处理**：`Start()` 与 `WriteHeaders()` 返回 `error`。纯推送型 handler
起流失败（客户端已断开）时应直接返回，不必再进消费循环白等。

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
// 错误结果走 hbErr，"已退出"走 hbDone——两者必须分开，理由见下
hbCtx, stopHeartbeat := context.WithCancel(c.Request.Context())
hbErr := make(chan error, 1)
hbDone := make(chan struct{})
go func() {
    defer close(hbDone)
    if err := stream.Heartbeat(hbCtx, 15*time.Second); err != nil {
        hbErr <- err
    }
}()
defer func() {
    stopHeartbeat()
    <-hbDone // 等它真的退出，再让 handler 返回
}()
```

阻塞运行，启动时机与所在 goroutine 由你控制。`ctx` 取消返回 `ctx` 错误，客户端断开返回 `ErrClientGone`，流已 `Close` 返回 `ErrStreamClosed`；`interval` 非正立即报错。

两条约束都不能省：

- **必须等它退出**：底层 `ResponseWriter` 只在 handler 执行期间有效（gin 会池化复用，见 9.1）。handler 返回后心跳若还在写，就写到别人的响应上了。
- **错误信号与退出信号必须分开**：若用同一个 channel 兼任，主循环消费掉唯一那次发送后，`defer` 里的第二次接收就永远等不到发送者——handler 永久阻塞，连排在后面的 `release()` 都不会执行。`hbDone` 用 `close`，读多少次都不会阻塞。

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

按 WHATWG event stream 规范解析，实际行为如下：

- 行分隔符支持 `CRLF` / `CR` / `LF`
- 字段值**只剥掉冒号后的一个空格**——大模型的增量 token 常以空格开头，多剥会损坏拼出的文本
- `Data` 原样保留上游字节：`[DONE]` 这类非 JSON 哨兵可直接比对，增量 token 的前后空格完整保留
- 多行 `data:` 以 `\n` 拼接；流首 BOM、注释行与未知字段跳过
- 帧是否产出以本帧是否出现过 `data` 字段为准（与浏览器 `EventSource` 一致），值为空串也照常产出
- `ID` 与 `Retry` 是连接级状态，按规范跨帧沿用（`The buffer does not get reset`），转发时 id 连续性不会断；`Name` 每帧独立
- 流尾残留的不完整帧（缺结尾空行）按规范丢弃，与浏览器一致——连接被掐断时残留字节通常是截断的半条 JSON
- 单帧 `data` 累计上限 1MB（单行同上限），超限返回 `ErrFrameTooLarge` 而非静默截断
- `retry` 只接受纯 ASCII 数字，且限制在不会让 `time.Duration` 溢出的范围内，超范围时忽略该字段
- 读到流尾正常结束
- 与规范的一处差异：`Data` 原样保留上游字节，不执行 UTF-8 解码替换。转发链路上改写字节会让服务端交给前端的内容与上游不一致，而浏览器接收端本身会按 UTF-8 解码；需要严格校验时自行处理 `Data`

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

    // 必须先起流：纯推送型 handler 在第一条事件到来前不写字节，
    // 不起流则前端一直等响应头、迟迟不触发 onopen。
    // 起流失败说明连接已不可用，此时响应头尚未提交，还能回普通 JSON。
    if err := stream.Start(); err != nil {
        return
    }

    events, release := hub.Subscribe(uid)
    defer release()

    // 心跳错误要能被主循环看到：慢客户端会让心跳返回 ErrWriteTimeout，
    // 而此时请求上下文可能还没结束，只丢弃错误会让 handler 继续空等。
    //
    // 错误走 hbErr，"已退出"走 hbDone，两者必须分开（见 4.12）；
    // 并且必须等它退出——c.Writer 会被 gin 池化复用（见 9.1）。
    hbCtx, stopHeartbeat := context.WithCancel(c.Request.Context())
    hbErr := make(chan error, 1)
    hbDone := make(chan struct{})
    go func() {
        defer close(hbDone)
        if err := stream.Heartbeat(hbCtx, 15*time.Second); err != nil {
            hbErr <- err
        }
    }()
    defer func() {
        stopHeartbeat()
        <-hbDone
    }()

    for {
        select {
        case <-shutdownCtx.Done(): // 应用级停机信号，见 6.9
            _ = stream.Close(gin.H{"reason": "server shutting down"})
            return
        case <-stream.Context().Done():
            return
        case <-hbErr: // 心跳写失败：连接已不可用
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
| 返回值 | `Push` / `Broadcast` 返回 `(delivered, dropped)`：`dropped` 是被挤掉的**旧**事件数 |

六条使用约束：

1. **先 `Start()` 再进消费循环**：纯推送型 handler 在第一条事件到来前不写字节，起流才能让前端及时拿到响应头并触发 `onopen`；`Start()` 返回错误说明连接已不可用，直接返回。
2. **投递与写出分离**：`Push` 只把事件放进队列，写出由持有连接的 handler 完成，因此写动作始终发生在 handler 自己的 goroutine、`ResponseWriter` 仍有效的窗口内。
3. **队列满时保留最新（latest-wins）**：挤掉队首最旧的一条，让最新事件一定入队，`dropped` 是被挤掉的旧事件数。状态推送里最新一条描述的就是当前状态，必须送达。
4. **顺序只在单连接内保证**：多个 goroutine 并发 `Push` / `Broadcast` 时，它们之间的相对顺序由调度决定，不同连接可能观察到不同顺序。状态事件应携带**单调递增的 revision**（放进 `Event.ID` 或载荷），消费端按 revision 取大者、忽略更旧的值。
5. **载荷投递后不可再改**：同一份 `Event.Data` 会被该 key 下的多个连接各自序列化，且发生在它们各自的 goroutine 里；投递后再修改其中的 map、切片或指针内容会构成数据竞争。需要复用结构体就投递前拷贝一份。
6. **适用边界与容量**：Hub 面向**状态推送**——每条事件自带完整状态，丢掉中间过程无妨；状态查询类场景建议 `WithQueueSize(1)`，容量大于 1 只会积压已经过时的中间状态。AI token 流每一条都是文本的一部分，少一条就损坏输出，应在 handler 内直接用 `stream.Data` 写出，不经 Hub。

消费循环用 `stream.Context().Done()` 退出，队列由 GC 回收。

## 5. 常见模式

### 5.1 转发大模型输出

先 `session`，中间连续 `chunk`，结束 `done`，出错 `error`；上游用 `Decode` 读，下游用 `Data` / `Send` 写。

```go
// 上游请求绑定下游 ctx：客户端一断开，上游请求随之取消
upCtx, cancel := context.WithCancel(c.Request.Context())
defer cancel()

req, err := http.NewRequestWithContext(upCtx, http.MethodPost, upstreamURL, body)
if err != nil {
    return err
}
resp, err := httpClient.Do(req)
if err != nil {
    return err
}
defer func() { _ = resp.Body.Close() }() // 始终关闭，否则连接与 goroutine 泄漏

if resp.StatusCode != http.StatusOK {
    return fmt.Errorf("上游状态码 %d", resp.StatusCode)
}
if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
    return fmt.Errorf("上游 Content-Type 非 SSE: %q", ct) // 上游报错时常返回 JSON
}

for msg, err := range ssex.Decode(resp.Body) {
    if err != nil {
        return err
    }
    if string(msg.Data) == "[DONE]" {
        break
    }
    if err := stream.Data(ssex.Raw(string(msg.Data))); err != nil {
        cancel() // 下游断开或写超时：立刻停掉上游，别再为它烧 token
        return nil
    }
}
_ = stream.Close(gin.H{"reason": "done"})
```

要点：

- 上游请求用 `http.NewRequestWithContext` 绑定下游 `r.Context()`，并在下游写失败（`ErrClientGone` / `ErrWriteTimeout`）时立即 `cancel()`
- 校验上游状态码与 `Content-Type`：上游出错时往往返回 JSON 而非事件流
- `defer resp.Body.Close()` 一定要有
- token 流在 handler 内直接写出，**不经 Hub**：Hub 的队列会在满时丢弃事件，而 token 少一条就损坏文本
- 结束时用 `Close` 终止流，前端 `es.close()`，避免自动重连
- 需要断线续传就用 `EventWithID` 带上 id，客户端重连时经 `LastEventID(r)` 读回起点，由业务决定从哪一条开始续推（见 6.7）

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

### 6.7 断线续传以存储为事实源

`LastEventID(r)` 只负责读回客户端的重连位点，重放数据从哪来由业务决定。可靠的做法：

- 状态以数据库/缓存为**唯一事实源**，每次变更写入一个**单调递增的 revision**，并把它作为事件 id 下发（`EventWithID`）
- 客户端重连时带回 `Last-Event-ID`，服务端据此从存储里取 `revision > LastEventID` 的记录补发
- 顺序上**先订阅、后取快照**：反过来（先快照再订阅）会漏掉两步之间发生的变更
- 先订阅时，快照的 revision 可能比队列里已有的事件旧，按 revision 取大者即可，别让旧事件覆盖新快照

```go
events, release := hub.Subscribe(uid)   // 先订阅
defer release()

snapshot := svc.Load(ctx, orderToken)   // 后取快照
_ = stream.EventWithID(strconv.FormatInt(snapshot.Revision, 10), "status", snapshot)
```

### 6.8 浏览器接入：鉴权与 POST 流

原生 `EventSource` 的构造参数只有 URL 与 `withCredentials`，不能携带自定义请求头，也不能发 POST body。据此选接入方式：

| 场景 | 做法 |
|---|---|
| 登录态 / 订单状态（同源或可带 Cookie） | `new EventSource(url, { withCredentials: true })`，鉴权走 Cookie；服务端从 Cookie 解出用户 |
| Bearer Token 鉴权 | 用 `fetch` + `ReadableStream` 自行读流，把 token 放进 `Authorization` 头 |
| AI 对话（请求体较大，需 POST） | 同上用 `fetch`：`method: 'POST'` 发 body，响应仍是 `text/event-stream` |

用 `fetch` 时前端需要自己解析帧（按空行分帧、`data:` 行以 `\n` 拼接），并自行实现重连——`EventSource` 的自动重连与 `Last-Event-ID` 回传都不再由浏览器代劳。服务端侧本包的输出格式不变。

```js
// Bearer Token / POST 流
const resp = await fetch('/api/chat', {
  method: 'POST',
  headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
  body: JSON.stringify({ prompt }),
});
const reader = resp.body.pipeThrough(new TextDecoderStream()).getReader();
```

### 6.9 优雅停机要监听应用级信号

`http.Server.Shutdown` 会等活跃连接变为空闲，而 SSE handler 只要还在循环就一直是活跃的——只依赖它，发布时会一直等到 `Shutdown` 的超时。

因此长连接 handler 必须同时监听应用级的 shutdown context：收到信号后发 `Close`（让前端 `es.close()` 而不是重连），再返回。参见 4.14 的 handler 模板里的 `shutdownCtx` 分支。

```go
// 应用启动时准备一个全局停机 context
shutdownCtx, shutdownDone := context.WithCancel(context.Background())

// 收到停机信号：先让 SSE handler 收尾，再关 HTTP server
shutdownDone()
time.Sleep(gracePeriod) // 给正在收尾的流留出时间
_ = srv.Shutdown(ctx)
```

### 6.10 容量、限流与监控由应用提供

Hub 只维护注册表，不做全局连接数上限、单 key 连接上限或 IP 限流——这些策略属于应用与网关。库把做决策所需的原始信号交出来：

| 信号 | 来源 |
|---|---|
| 在线连接数 / 单 key 连接数 | `Hub.Online(key)` |
| 被挤掉的旧事件数 | `Push` / `Broadcast` 的 `dropped` 返回值 |
| 客户端断开、写超时 | `ErrClientGone` / `ErrWriteTimeout` |
| 帧超限 | `ErrFrameTooLarge` |

生产上建议再自行记录：连接活跃时长与异常断开率、上游 AI 请求的取消率、每实例的文件描述符数量。

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
- [reliability_test.go](./reliability_test.go)
- [fuzz_test.go](./fuzz_test.go)
- [startup_test.go](./startup_test.go)
- [gincompat/](./gincompat)（独立模块：gin 集成回归）
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
- 可靠性：Hub latest-wins（最新状态送达、连续溢出只留最后一条、容量窗口）、帧构造失败时不提交响应头、起流错误可观察、整帧与单行大小上限、`retry` 溢出、非法 UTF-8 原样保留
- Fuzz：`FuzzDecode` 与 `FuzzRoundTrip`（随机 CR/LF、非法 UTF-8、超大多行帧、超大 retry、提前停止迭代）
- 起流写路径：刷新受 per-write deadline 约束、解除连接级截止时间失败时不提交响应头、nil Option 被跳过
- gin 集成（`gincompat` 独立模块）：鉴权在起流前拒绝、快照+推送+终态 `Close`、心跳 goroutine 在 handler 返回前收尾（`-race` 并发多轮）、长连接不被 `WriteTimeout` 截断、断开判定、`Started()` 分界、应用停机收尾

## 8. 依赖与版本

- 唯一直接依赖：`github.com/gtkit/json/v2`；`go mod graph` 里的其余条目均为它的传递依赖
- 入口只用标准库 HTTP 接口，因此 net/http、gin、chi 共用同一套实现
- 版本按 [SemVer](https://semver.org/lang/zh-CN/) 走，破坏性变更在 [CHANGELOG.md](./CHANGELOG.md) 以 ⚠ 标注并附迁移写法

## 9. Gin 集成

库不依赖 gin，但 gin 是最常见的使用场景。这一节的三件事都是 gin 特有的坑。

### 9.1 `c.Writer` 的生命周期等于 handler 的生命周期

gin 的 `c.Writer` 是 `Context` 的内部字段（`c.Writer = &c.writermem`）。handler 返回后 `Context` 被归还对象池，下一个请求会把这个 writer 重置到**另一个响应**上。

因此：**在 handler goroutine 之外写入的 goroutine，必须在 handler 返回前退出**。否则会写到别人的响应里。

`c.Copy()` 解决不了这个问题——它把副本的 `writermem.ResponseWriter` 置为 nil，副本只能用来读请求参数与头部，不能写响应。需要在后台 goroutine 里做的事，只有两种正确形态：

- 要写响应：goroutine 必须在 handler 返回前收尾（如下面模板里的心跳）
- 只读请求元数据：用 `c.Copy()`，或提前把需要的值取成局部变量

### 9.2 完整 handler 模板

```go
func OrderEvents(c *gin.Context) {
    // 1. 鉴权与参数校验：必须全部在 Start() 之前完成，
    //    此时还没有提交响应头，可以正常返回 JSON 错误。
    uid := c.GetString("uid") // 由鉴权中间件写入
    if uid == "" {
        c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "未登录"})
        return
    }
    orderID := c.Param("id")

    // 2. 先订阅，再读快照。顺序反过来会永久漏事件：Load 与 Subscribe 之间发生的
    //    状态变更此时无人订阅，推送直接丢弃，客户端会永远停在旧快照上（见 6.7）。
    events, release := hub.Subscribe(orderID)
    defer release()

    // 3. 从持久化事实源读快照。失败时还没起流，可以回普通 JSON。
    snapRev, snapshot, err := svc.Load(c.Request.Context(), orderID, uid)
    if err != nil {
        c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"error": "订单不存在"})
        return
    }

    stream := ssex.NewStream(c.Writer, c.Request, ssex.WithWriteTimeout(10*time.Second))

    // 4. 起流：失败时响应头尚未提交，仍可回 JSON。
    if err := stream.Start(); err != nil {
        handleStreamError(c, stream, err)
        return
    }

    // 5. 发快照，revision 放进事件 id。
    if err := stream.EventWithID(strconv.FormatInt(snapRev, 10), "status", snapshot); err != nil {
        handleStreamError(c, stream, err)
        return
    }

    // 6. 心跳：错误走 hbErr、"已退出"走 hbDone，并在 handler 返回前等它退出（见 4.12、9.1）。
    hbCtx, stopHeartbeat := context.WithCancel(c.Request.Context())
    hbErr := make(chan error, 1)
    hbDone := make(chan struct{})
    go func() {
        defer close(hbDone)
        if err := stream.Heartbeat(hbCtx, 15*time.Second); err != nil {
            hbErr <- err
        }
    }()
    defer func() {
        stopHeartbeat()
        <-hbDone
    }()

    // 7. 事件循环：四个退出口。
    for {
        select {
        case <-appShutdown.Done(): // 应用停机（见 6.9）
            _ = stream.Close(gin.H{"reason": "server shutting down"})
            return

        case <-stream.Context().Done(): // 客户端断开
            return

        case err := <-hbErr: // 心跳写失败，连接已不可用
            handleStreamError(c, stream, err)
            return

        case e := <-events:
            // 跳过不比快照新的事件：订阅到读快照之间积压的那些，快照里已经含了，
            // 直接转发会让前端出现状态回退。
            if revisionOf(e) <= snapRev {
                continue
            }
            if err := stream.Send(e); err != nil {
                handleStreamError(c, stream, err)
                return
            }
            if isTerminal(e) { // 终态：主动结束，避免前端自动重连（见 6.4）
                _ = stream.Close(gin.H{"reason": "final"})
                return
            }
        }
    }
}
```

这段模板的可执行版本在 [gincompat/handler_test.go](./gincompat/handler_test.go)——模板改了那里会挂，文档不会悄悄失真。

### 9.3 错误处理规范

gin 场景最容易写错的是流已开始后又调用 `c.JSON`——响应头早已提交，再写普通响应体只会产生一个损坏的响应。用 `Started()` 分界：

```go
func handleStreamError(c *gin.Context, stream *ssex.Stream, err error) {
    // 客户端断开与上下文取消是正常收尾，调试级记录即可
    if errors.Is(err, ssex.ErrClientGone) || errors.Is(err, context.Canceled) {
        logger.Debug("sse 客户端断开", zap.Error(err))
        c.Abort()
        return
    }

    // 写超时、序列化失败与未知写错误需要告警
    _ = c.Error(err)
    logger.Error("sse 写入失败", zap.Error(err))

    if !stream.Started() {
        // 响应头尚未提交，还能回普通 JSON
        c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "建立事件流失败"})
        return
    }

    // 已开始 SSE：只能记录并结束 handler
    c.Abort()
}
```

规则：

| 条件 | 允许的动作 |
|---|---|
| `stream.Started() == false` | 可以 `c.JSON` / `AbortWithStatusJSON` 返回普通 JSON |
| `stream.Started() == true` | 只能记录错误 + `c.Abort()`；**禁止** `c.JSON` / `c.String` / `AbortWithStatusJSON` |

错误分级：`ErrClientGone` 与 `context.Canceled` 属正常收尾，调试级记录；`ErrWriteTimeout`、序列化错误与未知写错误需要告警。

返回什么状态码与响应体是业务策略，因此库只提供 `Started()` 这个分界信号，不封装响应内容。

### 9.4 中间件兼容

SSE 路由要避开这几类中间件：

| 类型 | 后果 |
|---|---|
| 响应体缓存 / 统一包装 | 帧被攒在中间层，前端收不到 |
| 压缩（gzip 等） | 同上，`Flush` 只刷到压缩层（见 6.5） |
| 固定请求总时长的 timeout | 长连接被按普通请求掐断 |
| handler 返回后重写响应体 | 响应已提交，重写产生损坏响应 |
| 把所有错误转成 JSON | 流已开始后再写 JSON 同样损坏响应 |

鉴权、Recovery、Tracing 可以正常使用，但**鉴权必须在 `Start()` 之前完成**——起流之后就没法再回 401 了。

用独立路由组隔离：

```go
sseGroup := router.Group("/events")
sseGroup.Use(AuthMiddleware(), gin.Recovery()) // 不挂压缩、缓存、timeout
sseGroup.GET("/orders/:id", OrderEvents)
```

### 9.5 兼容性测试

gin 的集成由仓库内的独立模块 [gincompat](./gincompat) 持续验证（自带 `go.mod`，因此主模块的依赖清单里没有 gin）：

```bash
cd gincompat && go test -race ./...
```

覆盖：长连接不被 `http.Server.WriteTimeout` 截断、客户端断开判定为 `ErrClientGone`、Hub 端到端推送并由客户端 `Decode` 解回、心跳 goroutine 在 handler 返回前收尾（`-race` 下并发多轮）、起流前后错误处理分界、与 Recovery / Logger 中间件共存。
