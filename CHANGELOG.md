# Changelog

本文件记录本项目所有值得下游关注的变更。

格式遵循 [Keep a Changelog 1.1.0](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循 [Semantic Versioning 2.0.0](https://semver.org/lang/zh-CN/)。

## [Unreleased]

**⚠ 破坏性变更**：`New` / `NewStream` / `LastEventID` 签名改为标准库 HTTP 接口；错误类型、`Retry` 负值处理、注释与 `Raw` data 帧中孤立 `\r` 的处理均有变化。详见 Changed 区段。

### Removed

- ⚠ 移除对 `github.com/gin-gonic/gin` 的依赖。构造函数改用标准库接口后，模块的直接依赖只剩 `github.com/gtkit/json/v2`，随之移除的传递依赖包括 `quic-go`（HTTP/3 协议栈）、`mongo-driver/v2`（BSON 编解码）、`validator/v10`、`protobuf`、`go-yaml` 等，间接依赖数量从 30 个降到 13 个。

  迁移写法：

  ```go
  // 旧
  stream := ssex.NewStream(c)
  id := ssex.LastEventID(c)

  // 新
  stream := ssex.NewStream(c.Writer, c.Request)
  id := ssex.LastEventID(c.Request)
  ```

  行为无变化：per-write deadline、`http.Server.WriteTimeout` 清零、起流即刷头、`X-Accel-Buffering`、HTTP/2 头处理、错误分类在 gin 下全部经独立模块实测确认无回归（`http.ResponseController` 沿 gin `ResponseWriter` 的 `Unwrap` 链取到底层连接）。net/http、chi 等现在也可直接使用。

### Added

- 新增上游 SSE 解码器 `Decode(r io.Reader) iter.Seq2[Message, error]`，用于把上游大模型的 `text/event-stream` 转发给前端。按 WHATWG event stream 规范实现：支持 `CRLF` / `CR` / `LF` 三种行分隔符；字段值只剥掉冒号后的一个空格，`Data` 原样保留上游字节，增量 token 的前后空格与 `[DONE]` 这类非 JSON 哨兵都完整可用；多行 `data:` 以 `\n` 拼接；`ID` 与 `Retry` 按规范跨帧沿用，转发时 id 连续性得以保持；流尾不完整帧按规范丢弃；单帧上限 1MB，超限报错。实现对照规范章节的官方示例做了交叉验证。
- 新增 `Hub`（`NewHub` / `Subscribe` / `Push` / `Broadcast` / `Online` / `WithQueueSize`）：按 key 管理在线连接，供支付回调、踢下线等带外场景定向推送或广播。Hub 只投递到每个连接的有界队列、由持有连接的 handler 写出；队列满即丢弃并把 `dropped` 返回给调用方，不阻塞推送方也不重试。Hub 无后台 goroutine，因此不需要关闭。
- 新增 `Event` 结构与 `Stream.Send(Event)`：把"一条待推送事件"表达为值，供 Hub 的消费循环直接写出。
- 新增 `Stream.Heartbeat(ctx, interval)`：阻塞式保活循环，由调用方显式在自己的 goroutine 里启动。
- 新增 `Stream.Close`：发送一条约定的 `close` 事件并终止本流；终止后所有写入返回 `ErrStreamClosed`。用于解决服务端结束流后 `EventSource` 自动重连造成的无限重连——前端监听 `close` 事件并调用 `EventSource.close()`。
- 新增错误哨兵 `ErrClientGone` 与 `ErrStreamClosed`：可用 `errors.Is` 区分"客户端已断开"（应静默结束并取消上游请求，如正在进行的大模型调用）与"真实写失败"（应记录告警）。
- 新增 Option `WithWriteTimeout`，可配置单帧写入的截止时长（默认 10s，非正值忽略）；`New` 与 `NewStream` 相应接受可变 Option 参数。
- 新增 Example 测试，覆盖大模型输出转发、订单状态推送、上游解码、Hub 定向推送与写超时配置。
- 新增交叉验证测试：写侧输出交由解码器解回（含两处帧注入场景由解析器判决）、真实 HTTP 连接上客户端断开触发 `ErrClientGone`、Hub 与 Stream 在真实连接上的端到端推送、规范官方示例向量。

### Fixed

- 修复注释帧的帧注入：注释文本中的孤立 `\r` 此前原样透传，而 SSE 规范以 `CRLF | CR | LF` 三者任一分行，客户端会据此另起一行并派发伪造事件。现已归一为换行并逐行加 `: ` 前缀。
- 修复 `Raw` 透传 data 帧的同类注入：孤立 `\r` 此前不参与拆行。现已归一后逐行加 `data: ` 前缀。
- 起流（`Writer.WriteHeaders` / `Stream.Start`）现在立即把响应头刷给客户端。此前 gin 的惰性响应头会把前端 `onopen` 推迟到首帧，大模型首 token 期间连接上零字节，易被代理层按空闲连接掐断。
- 修正 README 中错误的模块路径（`github.com/gtkit/streaming/sse` → `github.com/gtkit/ssex`）及全部导入与调用示例——此前照抄 README 无法编译；同时修正包注释 `Package sse` → `Package ssex`。

### Changed

- ⚠ 客户端断开时，写方法返回包装错误而非裸 `context.Canceled`。`errors.Is(err, context.Canceled)` 仍然成立，但 `err == context.Canceled` 不再成立。
- ⚠ `Retry` 对负值毫秒返回错误且不写出任何字节。此前写出 `retry: -1`，客户端按规范静默忽略，属于静默失败。
- ⚠ 注释帧与 `Raw` data 帧中的孤立 `\r`，由原样透传改为按 SSE 规范归一拆行。
- 所有错误消息统一以包名 `ssex:` 开头（此前部分为 `sse:`，部分无前缀）。
- README 补充错误判定、Option、`Close` 终止语义与前端不重连约定的说明。
