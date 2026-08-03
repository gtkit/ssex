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
    _ = stream.Close(map[string]string{"reason": "done"}) // 生产代码要处理它，见 4.9
}
```

## 4. API 说明

### 4.1 `NewStream(w, r)`

创建一个业务层友好的 SSE 输出器。`w` 与 `r` 都必须非 nil——传 nil 是编程错误，库不做防御性降级，因为一个写不出字节的 `Stream` 比直接 panic 难定位得多。同理 `Decode(r)` 的 `r`、`LastEventID(r)` 的 `r`、`Heartbeat(ctx, …)` 的 `ctx` 都必须非 nil。

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
if err := stream.Close(gin.H{"reason": "delivered"}); err != nil {
    // 终止帧没送达前端会继续重连，除客户端已断开外都要能发现（见下）
    logger.Warn("发送终止事件失败", zap.Error(err))
}
```

```js
// 前端：收到 close 事件后主动断开，否则仍会重连
es.addEventListener('close', () => es.close());
```

`Close` 只终结本流的写入许可，不关闭 HTTP 连接（连接在 handler 返回时结束）。
终止后任何写入返回 `ssex.ErrStreamClosed`，重复 `Close` 同样返回它。

**它的错误不该一律忽略。** 终止帧没送达时前端收不到 `close`，会继续按重连间隔重连，而服务端毫无信号。客户端已断开（`ErrClientGone`）属正常收尾，其余要能被发现：

```go
func closeStream(stream *ssex.Stream, payload any) {
    err := stream.Close(payload)
    if err == nil || errors.Is(err, ssex.ErrClientGone) {
        return // 正常收尾
    }
    // 写超时或未知写错误：连接还在但帧没发出去，前端会继续重连
    logger.Warn("发送终止事件失败", zap.Error(err))
}
```

### 4.10 错误判定

写方法的错误分三类，用 `errors.Is` 区分：

| 判定 | 含义 | 建议处理 |
|---|---|---|
| `ErrClientGone` | 客户端已断开 | 静默结束，并取消上游请求（如正在进行的大模型调用）以停止计费 |
| `ErrStreamClosed` | 自己已 `Close` 过 | 调用顺序问题，检查业务逻辑 |
| `ErrInvalidArgument` | 参数非法：字段值含换行/NUL、`retry` 为负、心跳间隔非正 | 编程错误。**仅当 `Started()` 为 false 时**（即首帧就失败）响应头尚未提交、可改回普通 JSON；流已开始后它只表示这一帧被拒，此时**不能**再写普通响应体 |
| `ErrWriteTimeout` | 单帧写入超过 `WithWriteTimeout` | 客户端读取过慢，连接仍活着；按需告警 |
| `ErrFrameTooLarge` | 解码时单帧的 `Message.Data` 超过 1048575 字节 | 上游异常或恶意，终止本次转发 |
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
- 产出的 `Message.Data` 最多 1048575 字节，超限返回 `ErrFrameTooLarge` 而非静默截断；单行与多行同一口径（多行按拼接后的长度算），单行长度另有一个略高的硬上限防止超长行撑爆缓冲
- `retry` 只接受纯 ASCII 数字，且限制在不会让 `time.Duration` 溢出的范围内，超范围时忽略该字段
- 读到流尾正常结束
- 与规范的一处差异：`Data` 原样保留上游字节，不执行 UTF-8 解码替换。转发链路上改写字节会让服务端交给前端的内容与上游不一致，而浏览器接收端本身会按 UTF-8 解码；需要严格校验时自行处理 `Data`

### 4.14 `ssex.NewHub()` — 带外推送

产生事件的 goroutine 不持有连接时（支付回调推订单状态、后台踢登录态）用它：

```go
hub := ssex.NewHub()                        // 无后台 goroutine，不需要关闭

// 推送方（支付回调、MQ 消费者……）
// 顺序不能反：先在事务里持久化状态与 revision、提交成功，再发布事件。
delivered, dropped := hub.Push(uid, ssex.Event{ID: rev, Name: "status", Data: order})
metrics.Observe(delivered, dropped) // 只做监控，不参与任何业务判断
```

**`delivered` 不是交付确认。** 它只表示事件成功进入了多少个**本机内存队列**，不代表客户端收到、`Flush` 成功、浏览器处理完成、其他实例上的连接收到、连接没有随即断开，也不代表这条事件没有被后续溢出挤掉。因此绝不能写成"`delivered == 0` 才落库"——状态必须先持久化再发布，`delivered` / `dropped` 只用于监控。

连接侧的 handler 模板：

```go
func Subscribe(c *gin.Context) {
    // key 必须由服务端身份计算，且非空：空 key 会让所有认证异常的请求
    // 共享同一个队列、互相收到对方的事件（见 4.15）。
    uid := c.GetString("uid")
    if uid == "" {
        c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "未登录"})
        return
    }

    // 先订阅、再起流：Start 最坏要等一个完整的 WithWriteTimeout，
    // 这段时间里的推送若无人订阅就永久丢失。
    events, release := hub.Subscribe(uid)
    defer release()

    stream := ssex.NewStream(c.Writer, c.Request)

    // 纯推送型 handler 在第一条事件到来前不写字节，不起流则前端一直等响应头、
    // 迟迟不触发 onopen。起流失败时响应头尚未提交，还能回普通 JSON。
    if err := stream.Start(); err != nil {
        return
    }

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
            closeStream(stream, gin.H{"reason": "server shutting down"}) // 见 4.9
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

七条使用约束：

1. **先 `Start()` 再进消费循环**：纯推送型 handler 在第一条事件到来前不写字节，起流才能让前端及时拿到响应头并触发 `onopen`；`Start()` 返回错误说明连接已不可用，直接返回。
2. **投递与写出分离**：`Push` 只把事件放进队列，写出由持有连接的 handler 完成，因此写动作始终发生在 handler 自己的 goroutine、`ResponseWriter` 仍有效的窗口内。
3. **队列满时保留最新（latest-wins）**：挤掉队首最旧的一条，让最新事件一定入队，`dropped` 是被挤掉的旧事件数。状态推送里最新一条描述的就是当前状态，必须送达。
4. **顺序只在单连接内保证**：多个 goroutine 并发 `Push` / `Broadcast` 时，它们之间的相对顺序由调度决定，不同连接可能观察到不同顺序。状态事件应携带**单调递增的 revision**（放进 `Event.ID` 或载荷），消费端按 revision 取大者、忽略更旧的值。
5. **载荷投递后不可再改**：同一份 `Event.Data` 会被该 key 下的多个连接各自序列化，且发生在它们各自的 goroutine 里；投递后再修改其中的 map、切片或指针内容会构成数据竞争。需要复用结构体就投递前拷贝一份。
6. **容量与顺序的组合风险**：latest-wins 按**到达顺序**保留最后一条，不比较业务 revision。多个并发推送方存在时，到达顺序可能与 revision 顺序相反——容量为 1 时后到的旧版本会挤掉先到的新版本，消费端再也看不到它，`lastRev` 过滤也救不回来。因此 `WithQueueSize(1)` 只在**同一 key 的推送已串行化**时才安全；否则至少满足一条：同一 key 经单点串行化后再 `Push`／容量留大于 1 并由消费端按 revision 过滤／终态后回读一次事实源做校准。
7. **适用边界**：Hub 面向**状态推送**——每条事件自带完整状态，丢掉中间过程无妨。AI token 流每一条都是文本的一部分，少一条就损坏输出，应在 handler 内直接用 `stream.Data` 写出，不经 Hub。

消费循环用 `stream.Context().Done()` 退出，队列由 GC 回收。

### 4.15 Hub 的两条硬边界

**一、Hub 只管当前进程的连接。** 多实例部署时，连接在实例 A、支付回调打到实例 B，实例 B 的 `Push` 找不到那条连接，返回 `delivered == 0`，前端收不到更新；`Online` 也只反映本机而非集群。滚动发布、负载均衡、`EventSource` 重连换实例都会触发。

单实例可以直接用。多实例必须让**每个实例都收到状态事件**，再各自推给本地 Hub：

```text
数据库事务：写状态 + 单调递增 revision
        ↓ 提交成功
Outbox / MQ / Redis Pub/Sub
        ↓ 广播式消费（每个实例都收到）
各实例 hub.Push(key, event)
        ↓
本实例上的连接收到状态
```

也可以用一致性哈希把同一 key 的连接与事件路由到同一实例，但运维复杂度更高。无论哪种，**存储始终是事实源**，Hub 只是加速通道。

AI 对话流在 handler 内直连上游、不经 Hub，因此不受这条限制。

**二、key 必须是服务端计算出的安全作用域。** Hub 的 key 就是一个字符串，两个不同租户的 `orderID=100` 会落进同一个 key，造成跨租户状态串流。约束：

- key 只能由**已认证身份 + 已授权资源**计算，不能直接用 URL、query 或客户端传入的值
- 多租户必须带 tenant 作用域，如 `tenantID:userID:orderID`，或用服务端生成的全局唯一不可猜测 ID
- **禁止空 key**：空 key 会让所有认证异常的请求共享同一个队列、互相收到对方的事件
- **不要裸拼接**：`"a:b" + ":" + "c"` 与 `"a" + ":" + "b:c"` 会得到同一个 key。用长度前缀编码或服务端生成的全局唯一 ID，并统一封装成一个 key 构造函数，禁止业务代码各自拼接
- **读资源时沿用授权阶段的同一标识**，不要退回裸资源 ID——否则快照可能读到另一个租户的数据
- **订阅前二次校验授权结果**：授权实现若因 bug 返回零值标识与 `nil` error，key 会退化成一个固定值，多个异常请求就此落进同一队列。检查租户与资源标识都非空，可以把这类 bug 从静默串流降级成一次 403
- `Broadcast` 只用于确实允许所有在线用户看到的内容

`gincompat` 里的 `resource.scopeKey()` 用长度前缀编码做了示范，并有 `TestScopeKeyHasNoCollision` 与走真实 handler 的 `TestTenantCannotReachOtherTenantOrder` 两个回归测试。

## 5. 常见模式

### 5.1 转发大模型输出

先 `session`，中间连续 `chunk`，结束 `done`，出错 `error`；上游用 `Decode` 读，下游用 `Data` / `Send` 写。

上游客户端要按**阶段**设超时，不要用 `http.Client.Timeout`——它覆盖整个响应体读取周期，会把正常的长流掐断：

```go
// 全局复用一个客户端。Timeout 留零值，改为限制建连、TLS 与等响应头这三个阶段；
// "最长生成时长"由业务用 context.WithTimeout 单独控制。
//
// 必须从 DefaultTransport 克隆再覆盖，不要自己 new(http.Transport)——后者会丢掉
// ProxyFromEnvironment（企业代理失效）、ForceAttemptHTTP2（自定义 DialContext 时
// 不再尝试 HTTP/2）、MaxIdleConns / IdleConnTimeout / ExpectContinueTimeout 等
// 标准库调好的默认值。
var upstreamClient = &http.Client{Transport: newUpstreamTransport()}

func newUpstreamTransport() *http.Transport {
    tr := http.DefaultTransport.(*http.Transport).Clone()
    tr.TLSHandshakeTimeout = 5 * time.Second
    tr.ResponseHeaderTimeout = 30 * time.Second // 首个响应头就绪的上限，不含流式响应体
    // 只调建连超时，保留 KeepAlive
    tr.DialContext = (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext

    return tr
}
```

```go
// 上游请求绑定下游 ctx：客户端一断开，上游请求随之取消；
// 同时叠加一个业务级的最长生成时长。
upCtx, cancel := context.WithTimeout(c.Request.Context(), maxGenerationTime)
defer cancel()

req, err := http.NewRequestWithContext(upCtx, http.MethodPost, upstreamURL, body)
if err != nil {
    return err
}
resp, err := upstreamClient.Do(req)
if err != nil {
    return err
}
defer func() { _ = resp.Body.Close() }() // 始终关闭，否则连接与 goroutine 泄漏

if resp.StatusCode != http.StatusOK {
    return fmt.Errorf("上游状态码 %d", resp.StatusCode)
}

// 用 mime.ParseMediaType 精确比较，不要 strings.HasPrefix：后者会接受
// text/event-streaming、text/event-stream-error 这类非 SSE 类型，
// 又会拒绝合法的大小写与空白变体（media type 本身大小写不敏感）。
rawCT := resp.Header.Get("Content-Type")
mediaType, _, err := mime.ParseMediaType(rawCT)
if err != nil || mediaType != "text/event-stream" {
    return fmt.Errorf("上游 Content-Type 非 SSE: %q", rawCT) // 上游报错时常返回 JSON
}

// 上游校验通过、确定要开这条流了，显式起流把响应头发出去。
// 不起流的话，响应头要等到第一个 token（可能几十秒）或第一次心跳才提交，
// 前端迟迟不触发 onopen；而在这之前失败还能回普通 JSON。
if err := stream.Start(); err != nil {
    return err
}

// 起流之后启动心跳：模型思考、工具调用、上游暂时停顿期间下游一个字节
// 都没有，网关 / CDN / Ingress 的空闲超时会在模型还在工作时掐断连接。
//
// 心跳失败要连带取消上游——转发循环阻塞在读上游，没法同时 select hbErr，
// 靠 cancel 让 Decode 的读失败、循环自然退出。
hbCtx, stopHeartbeat := context.WithCancel(c.Request.Context())
hbErr := make(chan error, 1)
hbDone := make(chan struct{})
go func() {
    defer close(hbDone)
    if err := stream.Heartbeat(hbCtx, 15*time.Second); err != nil {
        hbErr <- err
        cancel() // 下游写不出去了，别再让上游继续生成
    }
}()
defer func() {
    stopHeartbeat()
    <-hbDone // 等心跳退出，见 9.1
}()

// completed 记录是否真的收到了结束哨兵。循环还可能因为上游正常 EOF、
// 代理提前结束响应、上游异常关闭但没返回读取错误而结束——那几种情况下
// 内容是被截断的，绝不能当成正常完成。
var completed bool
for msg, err := range ssex.Decode(resp.Body) {
    if err != nil {
        return finish(stream, hbErr, "incomplete", err)
    }
    if string(msg.Data) == "[DONE]" {
        completed = true

        break
    }
    if err := stream.Data(ssex.Raw(string(msg.Data))); err != nil {
        cancel() // 无论哪种失败都要停掉上游，别再为它烧 token

        // 按 9.3 的分级处理：断开是正常收尾，写超时与未知错误要能告警
        if errors.Is(err, ssex.ErrClientGone) || errors.Is(err, context.Canceled) {
            return nil
        }
        return err
    }
}

if !completed {
    // 让前端能区分"生成完成"与"被截断"：终止原因不同，前端可以据此提示重试，
    // 而不是把半截回答当成最终答案。
    return finish(stream, hbErr, "incomplete", errors.New("上游未返回结束哨兵，输出可能被截断"))
}
closeStream(stream, gin.H{"reason": "done"}) // 终止帧失败要能发现，见 4.9
```

两个异常出口都走 `finish`，它同时解决两件事：报告哪个错误、还要不要写终止帧。

```go
// finish 在转发异常结束时收尾。
//
// 先看心跳，有两个原因：
//
//  1. 心跳失败会 cancel 上游，因此上游返回的 context canceled 只是连带结果，
//     真正的原因是下游写不出去。不优先取心跳错误，ErrWriteTimeout 就会被上游
//     读错误遮蔽，生产上丢掉"这条连接写不动"的信号。
//  2. 心跳失败说明连接已经写不动了，此时再写终止帧只会白等一个 WithWriteTimeout
//     才失败，拖慢这条失败连接的释放，还会产生一次重复告警。所以直接跳过 closeStream。
func finish(stream *ssex.Stream, hbErr <-chan error, reason string, cause error) error {
    select {
    case hbFail := <-hbErr:
        if errors.Is(hbFail, ssex.ErrClientGone) || errors.Is(hbFail, context.Canceled) {
            return nil // 下游已断开，正常收尾
        }

        return hbFail
    default:
    }

    closeStream(stream, gin.H{"reason": reason})

    return cause
}
```

要点：

- 上游请求用 `http.NewRequestWithContext` 绑定下游 `r.Context()`，并在下游写失败（`ErrClientGone` / `ErrWriteTimeout`）时立即 `cancel()`
- 校验上游状态码，并用 `mime.ParseMediaType` 精确比较 `Content-Type`：上游出错时往往返回 JSON 而非事件流
- **区分"收到结束哨兵"与"只是读到 EOF"**：后者意味着内容被截断（上游异常关闭、代理提前结束响应都会走到这里），必须用不同的终止原因告诉前端，否则半截回答会被当成完整答案
- `defer resp.Body.Close()` 一定要有
- token 流在 handler 内直接写出，**不经 Hub**：Hub 的队列会在满时丢弃事件，而 token 少一条就损坏文本
- **校验通过后显式 `Start()`**：否则响应头要等第一个 token 或第一次心跳才提交
- **长空闲期要有心跳**：模型思考、工具调用期间下游没有字节，链路上任何一层的空闲超时都会掐断连接。只有当上游能保证最大空闲间隔小于链路最短 idle timeout 时才可以省掉心跳
- **上游客户端按阶段设超时**：`http.Client.Timeout` 覆盖整个响应体读取，会掐断正常长流；改用 `DialContext` / `TLSHandshakeTimeout` / `ResponseHeaderTimeout`，最长生成时长用 `context.WithTimeout` 控制
- 结束时用 `Close` 终止流，前端 `es.close()`，避免自动重连
- 需要断线续传就用 `EventWithID` 带上 id，客户端重连时经 `LastEventID(r)` 读回起点，由业务决定从哪一条开始续推（见 6.7）

这段模板的可执行版本在 [gincompat/relay_test.go](./gincompat/relay_test.go)：用 `httptest` 起假上游跑完整 handler，覆盖结束哨兵有/无、`Content-Type` 精确比较（含大小写变体与 `text/event-streaming` 这类前缀陷阱）、校验通过即起流、长空闲期心跳、下游断开取消上游。模板改坏了那里会挂，文档不会悄悄失真。

### 5.2 订单状态流

先发一条 `status` 快照，后续由支付回调经 `Hub.Push` 带外推送，定期注释心跳，终态后 `Close`。

要点：

- 快照先发：客户端可能在状态变更之后才连上来
- 心跳必须有：等支付的连接可能几分钟没有数据
- 终态后 `Close`，否则 `EventSource` 会一直重连
- 状态先持久化再发布事件；`delivered == 0` 只说明本机此刻没有在等的连接，不能用它决定是否落库（见 4.14）

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
res, err := svc.Authorize(ctx, tenantID, uid, orderToken) // 先授权，拿到内部安全标识
if err != nil {
    return err
}

events, release := hub.Subscribe(res.ScopeKey())          // 再订阅
defer release()

snapshot, err := svc.Load(ctx, res)                       // 后取快照，沿用同一标识
if err != nil {
    return err
}
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

`http.Server.Shutdown` 会先关闭监听器、再关闭空闲连接，然后等活动连接变空闲。SSE handler 只要还在循环就一直是活动的——因此**只调 `Shutdown` 而 handler 不监听停机信号**，会一直等到 `Shutdown` 的 context 超时。

反过来，handler 一旦监听了应用级停机信号，`Shutdown` 自己就完成了等待：信号让 handler `Close` 并返回 → 连接变空闲 → `Shutdown` 返回。

```go
// 应用启动时准备一个全局停机 context
shutdownCtx, shutdownDone := context.WithCancel(context.Background())

// 收到停机信号
shutdownDone() // 让所有 SSE handler 走 Close 分支主动收尾（见 9.2 模板的 appShutdown 分支）

// Shutdown 关掉监听器后不再有新请求，随后等已有连接变空闲——
// 正在收尾的 SSE handler 返回时连接就变空闲了。
if err := srv.Shutdown(ctx); err != nil {
    logger.Error("优雅停机未在期限内完成", zap.Error(err))
}
```

**不要额外维护一个统计在线 handler 的 `WaitGroup` 再 `Wait()`**：`Wait()` 期间监听器还没关，新请求的 handler 仍会 `Add(1)`，而 `sync.WaitGroup` 要求"计数器为零时开始的正数 `Add` 必须发生在 `Wait` 之前"——这既可能漏等新 handler，也是对 `WaitGroup` 的误用。把关闭监听器这件事交给 `Shutdown` 做，顺序才是安全的。

### 6.10 容量、限流与监控由应用提供

Hub 只维护注册表，不做全局连接数上限、单 key 连接上限或 IP 限流——这些策略属于应用与网关。库把做决策所需的原始信号交出来：

| 信号 | 来源 |
|---|---|
| 单 key 连接数（瞬时快照） | `Hub.Online(key)` |
| 被挤掉的旧事件数 | `Push` / `Broadcast` 的 `dropped` 返回值 |
| 客户端断开、写超时 | `ErrClientGone` / `ErrWriteTimeout` |
| 帧超限 | `ErrFrameTooLarge` |

生产上建议再自行记录：连接活跃时长与异常断开率、上游 AI 请求的取消率、每实例的文件描述符数量。

**`Online` 不能用于严格限流。** 它只是某个时刻单个 key 的快照，也不提供全局在线总数；`if hub.Online(key) < limit { hub.Subscribe(key) }` 是非原子的 check-then-act，并发请求照样会突破上限。它适合监控与近似判断，严格限流要用应用级 semaphore、原子计数器或网关。

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
- [gincompat/](./gincompat)（独立模块：gin 集成回归，24 个测试）
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
- gin 集成（`gincompat` 独立模块）：鉴权在起流前拒绝、快照+推送+终态 `Close`、心跳 goroutine 在 handler 返回前收尾（`-race` 并发多轮）、长连接不被 `WriteTimeout` 截断、断开判定、`Started()` 分界、应用停机收尾、跨租户隔离与 scope key 无碰撞
- AI 转发模板（5.1 的可执行版本，`gincompat/relay_test.go`）：结束哨兵有/无（缺哨兵必须报 `incomplete`）、上游状态码与 `Content-Type` 精确校验、校验通过即起流、长空闲期心跳保活、下游断开取消上游

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
    // 1. 认证：必须在 Start() 之前，此时还没提交响应头，可以正常返回 JSON 错误。
    uid, tenantID := c.GetString("uid"), c.GetString("tenant") // 由鉴权中间件写入
    if uid == "" || tenantID == "" {
        c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "未登录"})
        return
    }
    orderID := c.Param("id")

    // 2. 资源授权：与"读快照"分开做。授权是廉价的归属检查，先做掉可以避免
    //    产生未授权订阅（会让 Online 出现伪在线）。
    //    它返回一个内部安全标识，后续所有操作都用它——避免"授权带 tenantID、
    //    读快照只用 orderID"这种作用域漂移读到别的租户数据。
    res, err := svc.Authorize(c.Request.Context(), tenantID, uid, orderID)
    if err != nil || !res.Valid() {
        // 二次校验：授权实现若因 bug 返回零值标识与 nil error，key 会退化成一个
        // 固定值，多个异常请求就此落进同一队列、互相串流（见 4.15）。
        c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "无权访问"})
        return
    }

    // 3. key 由服务端从已授权资源计算。统一走一个构造函数，禁止业务代码各自拼接：
    //    裸拼接 tenantID + ":" + orderID 在任一段含分隔符时会碰撞
    //    （"a:b"+":"+"c" 与 "a"+":"+"b:c" 得到同一个 key），见 4.15。
    key := res.ScopeKey()

    // 4. 先订阅，再读快照。顺序反过来会永久漏事件：Load 与 Subscribe 之间发生的
    //    状态变更此时无人订阅，推送直接丢弃，客户端会永远停在旧快照上（见 6.7）。
    events, release := hub.Subscribe(key)
    defer release()

    // 5. 用同一个已验证标识读快照，不能退回裸 orderID。失败时还没起流，可以回 JSON。
    snapRev, snapshot, err := svc.Load(c.Request.Context(), res)
    if err != nil {
        c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"error": "订单不存在"})
        return
    }

    stream := ssex.NewStream(c.Writer, c.Request, ssex.WithWriteTimeout(10*time.Second))

    // 6. 起流：失败时响应头尚未提交，仍可回 JSON。
    if err := stream.Start(); err != nil {
        handleStreamError(c, stream, err)
        return
    }

    // 7. 发快照，revision 放进事件 id。
    if err := stream.EventWithID(strconv.FormatInt(snapRev, 10), "status", snapshot); err != nil {
        handleStreamError(c, stream, err)
        return
    }

    // 8. 心跳：错误走 hbErr、"已退出"走 hbDone，并在 handler 返回前等它退出（见 4.12、9.1）。
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

    // 9. 事件循环：四个退出口。lastRev 从快照起步并随成功发送推进——
    //    只比快照不够：Hub 不保证跨推送方的到达顺序，快照之后仍可能先到 rev 10、
    //    后到 rev 9，只比快照会把 9 也转发出去，前端状态回退。
    lastRev := snapRev
    for {
        select {
        case <-appShutdown.Done(): // 应用停机（见 6.9）
            closeStream(stream, gin.H{"reason": "server shutting down"})
            return

        case <-stream.Context().Done(): // 客户端断开
            return

        case err := <-hbErr: // 心跳写失败，连接已不可用
            handleStreamError(c, stream, err)
            return

        case e := <-events:
            rev, ok := revisionOf(e)
            if !ok {
                // 生产者没填 revision：告警而不是静默丢弃。若把解析失败当成 0，
                // 事件会因为 0 <= lastRev 被默默吃掉，排查时只看到"前端收不到状态"。
                // 只记可定位的元数据：Event.Data 可能含身份信息、token、
                // 订单详情或 AI 对话内容，不要整个序列化进日志。
                logger.Warn("事件缺少 revision",
                    zap.String("key", maskKey(key)),
                    zap.String("event_id", e.ID),
                    zap.String("event_name", e.Name),
                    zap.String("payload_type", fmt.Sprintf("%T", e.Data)),
                    zap.String("trace_id", traceID(c)))
                continue
            }
            if rev <= lastRev {
                continue // 积压的旧事件，或乱序到达的回退版本
            }
            if err := stream.Send(e); err != nil {
                handleStreamError(c, stream, err)
                return
            }
            lastRev = rev

            if isTerminal(e) { // 终态：主动结束，避免前端自动重连（见 6.4）
                closeStream(stream, gin.H{"reason": "final"})
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

**日志不要序列化整个 `Event.Data` 或响应载荷。** 它可能含身份信息、登录 token、订单详情、AI 对话内容、手机号或地址。只记可定位的元数据：key 的脱敏值、`Event.ID`、`Event.Name`、载荷类型、Trace ID、错误原因。

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

**替换 `c.Writer` 的自定义中间件必须实现 `Unwrap() http.ResponseWriter` 并正确透传 `Flush`。** 否则 `http.ResponseController` 沿不到底层连接，`SetWriteDeadline` 返回 `http.ErrNotSupported`，本库按约定静默降级——后果是逐帧写超时失效，且**解除 `http.Server.WriteTimeout` 的能力一并失效**，长连接仍会在全局超时到期时被掐断。这个降级不会报错，所以上线前要核对真实的中间件链，而不只是裸 gin。

```go
type myWriter struct {
    gin.ResponseWriter
    // ...自己的字段
}

// 必须有：否则 SetWriteDeadline 与 WriteTimeout 清除都会降级
func (w *myWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
```

自查办法：在真实中间件链下跑一个把 `http.Server.WriteTimeout` 设成几百毫秒的用例，持续写若干秒并断言最后一帧仍能收到——`gincompat` 里的 `TestSurvivesServerWriteTimeout` 就是这个形状，把它挪到你自己的路由与中间件上即可。

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

覆盖：长连接不被 `http.Server.WriteTimeout` 截断、客户端断开判定为 `ErrClientGone`、Hub 端到端推送并由客户端 `Decode` 解回、心跳 goroutine 在 handler 返回前收尾（`-race` 下并发多轮）、心跳写错误不阻塞 handler、起流前后错误处理分界、与 Recovery / Logger 中间件共存。

状态推送的正确性契约也在这里固定：读快照期间的变更不丢、积压与乱序的旧 revision 被过滤、无效 revision 被上报而非静默丢弃、空身份与零值授权结果不进入 Hub、跨租户隔离（两个租户用同一 orderID，断言各自真实连接的完整事件序列）、终止帧失败被上报。其中隔离与终止帧两类断言做过反向验证——故意改坏实现后测试确实会失败。
