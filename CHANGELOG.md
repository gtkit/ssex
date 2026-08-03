# Changelog

本文件记录本项目所有值得下游关注的变更。

格式遵循 [Keep a Changelog 1.1.0](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循 [Semantic Versioning 2.0.0](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### Fixed

- 起流的响应头刷新纳入 per-write 写截止时间（`WithWriteTimeout`）。此前 `WriteHeaders` 先清除 `http.Server.WriteTimeout`、随后直接刷新，而逐帧截止时间只作用于后续帧，异常或恶意连接可以让 handler 无上界地卡在起流上。
- 解除 `http.Server.WriteTimeout` 失败时不再静默继续：除 `http.ErrNotSupported`（底层不支持，降级）外一律返回错误，且此时响应头尚未提交、`Started()` 保持 false，调用方仍可改回普通 JSON。吞掉这个错误等于谎称长连接已保活，而连接其实仍会在全局写超时到期时被服务端中断。
- `Option` 与 `HubOption` 跳过 nil，不再因调用方传入零值 Option 而 panic。

### Added

- 新增 `gincompat/` 独立测试模块（自带 go.mod，主模块依赖清单因此仍不含任何 web 框架）：把 gin 集成做成可持续运行的回归测试，覆盖鉴权在起流前拒绝、快照+带外推送+终态 `Close`、心跳 goroutine 在 handler 返回前收尾、长连接不被 `http.Server.WriteTimeout` 截断、客户端断开判定、`Started()` 错误分界、应用停机收尾、与 Recovery / Logger 中间件共存。
- README 新增「Gin 集成」一节：完整 handler 模板（鉴权 → 起流错误 → 先订阅后快照 → 心跳收尾 → 四出口事件循环）、以 `Started()` 分界的错误处理规范与错误分级、中间件兼容清单与路由组隔离、`c.Writer` 生命周期约束。

### Fixed

- 修正 README 的心跳模板：原写法只发停止信号、不等 goroutine 退出。gin 的 `c.Writer` 是 `Context` 的内部字段，handler 返回后 `Context` 归还对象池并在下个请求中被重置，因此心跳 goroutine 若在 handler 返回后仍在写入，就会写到另一个请求的响应上。模板改为独立 ctx + `defer stop(); <-hbDone` 等待退出。

### Changed

- `Stream` 与 `Stream.Heartbeat` 的 GoDoc 补写生命周期约束：底层 `http.ResponseWriter` 只在 handler 执行期间有效，在 handler goroutine 之外写入的 goroutine 必须在 handler 返回前退出；`c.Copy()` 的副本只能读请求元数据（gin 会把副本的 `ResponseWriter` 置为 nil），不能用于写响应。
- `Event.Data` 补充所有权约束：投递给 `Hub.Push` / `Hub.Broadcast` 后即视为交出所有权，同一份载荷会被多个连接在各自 goroutine 里序列化，投递后修改其中的 map / 切片 / 指针会构成数据竞争。
- `Hub` 补充顺序契约：事件顺序在单个连接内按入队顺序保证，跨并发推送方的相对顺序由调度决定；状态事件应携带单调递增的 revision，消费端按 revision 取大者。状态查询类场景建议 `WithQueueSize(1)`。
- `ErrInvalidArgument` 的文档措辞修正：只有首帧构造失败时响应头尚未提交，流已开始之后出现该错误仅表示这一帧被拒绝。
- README 的 Hub handler 模板改为处理 `Start()` 错误、把心跳错误接进主 `select`，并新增应用级停机分支；新增大模型转发的上游约束（绑定下游 context、校验状态码与 `Content-Type`、始终关闭响应体、下游写失败即取消上游、token 不经 Hub）、优雅停机与运维边界（容量/限流归应用、建议监控的信号）两节。
- CI 的 action 固定到 commit SHA，避免版本 tag 被移动导致构建不可复现。

## [v0.2.0] - 2026-07-31

> 本段已修订：原先把 v0.1.0 引入的能力误记为本版本新增。v0.2.0 相对 v0.1.0 只包含以下修复与变更。
> （v0.2.0 的 tag message 保留了修订前的措辞，标签已推送不可覆盖。）

**⚠ 破坏性变更**：`Writer.WriteHeaders` 与 `Stream.Start` 改为返回 `error`；Hub 队列溢出策略由丢弃新事件改为丢弃最旧事件；解码器对超限帧与超范围 `retry` 的处理变化。

### Added

- 新增错误哨兵 `ErrInvalidArgument`（字段值含换行或 NUL、`retry` 为负、心跳间隔非正）、`ErrWriteTimeout`（单帧写入超过 `WithWriteTimeout`，慢客户端不等于断开）、`ErrFrameTooLarge`（解码时单帧超过 1MB）。
- 新增 Fuzz 测试 `FuzzDecode` 与 `FuzzRoundTrip`，覆盖随机 CR/LF、非法 UTF-8、超大多行帧、超大 `retry`、提前停止迭代。
- README 新增断线续传的正确做法（以存储为事实源 + 单调递增 revision + 先订阅后取快照）与浏览器接入边界（`EventSource` 只接受 URL 与 `withCredentials`，Bearer Token 与 POST 流需改用 `fetch`）。

### Fixed

- ⚠ 修复 Hub 会丢掉最新状态的缺陷：队列满时原实现丢弃**本次新事件**、保留队列里的旧事件，订单的 `paid`、登录的 `logged_in` 会被丢掉而客户端继续收到旧的 `pending`。现改为 latest-wins——挤掉队首最旧的一条，让最新事件一定入队；`dropped` 的含义随之变为"被挤掉的旧事件数"。同一连接的"挤掉 + 入队"两步由该连接自己的锁串行化，并发推送不会交错。
- ⚠ 修复首帧失败前已提交 HTTP 200 的缺陷：`Stream` 的写方法原先先起流、后校验字段与序列化载荷，首帧因非法事件名或序列化失败而报错时，响应已经是 `200 text/event-stream`，无法改回 4xx/5xx。现改为先构造并校验完整帧，构造失败时不提交响应头，`Started()` 仍为 `false`，调用方可以改回普通 JSON 响应。
- ⚠ 修复起流错误不可观察：`Writer.WriteHeaders` 与 `Stream.Start` 原先无返回值且忽略首次 `Flush` 失败，纯推送型 handler 起流即失败时会挂着白等。二者现在返回 `error`。
- ⚠ 修复解码器的单帧上限只约束单行：`bufio.Scanner` 的 buffer 上限限制的是单个 token（这里是一行），由大量短 `data:` 行组成的一帧仍可让缓冲无限增长。现累计整帧字节数，越界返回 `ErrFrameTooLarge`；单行超长也归入同一判定。
- ⚠ 修复 `retry` 字段的整数溢出：原实现用 `strconv.Atoi` 后直接乘 `time.Millisecond`，极大的合法数字会溢出成负的 `Duration`（最大 `int` 得到 `-1ms`）。现改用 `ParseUint` 并限制在不会溢出的范围内，超范围时按规范忽略该字段。

### Changed

- 文档不再把解码器表述为"严格实现规范"：`Message.Data` 原样保留上游字节、不执行 UTF-8 解码替换，这是与规范的有意差异（转发链路上改写字节会让服务端交给前端的内容与上游不一致），已在 GoDoc 与 README 写明。
- Makefile 移除 ORM 模板残留的 MySQL 集成测试目标与 DSN 配置；CI 固定 golangci-lint 版本并新增 Fuzz 步骤；`Version` 补 GoDoc；README 最小示例移除未使用的 import，依赖表述改为"唯一**直接**第三方依赖"。

## [v0.1.0] - 2026-07-31

首个发布版本。以下能力均包含在本版本中（此前 CHANGELOG 曾把它们误记在 v0.2.0 的 Added 段下，现已归位）。

### Added

- 基于标准库 HTTP 接口的 SSE 写入器 `Writer` 与 `Stream`：统一 `text/event-stream` 响应头、起流即刷出响应头、每帧独立写截止时间、解除 `http.Server.WriteTimeout` 对长连接的写截止、下发 `X-Accel-Buffering: no`、HTTP/2 下省略 `Connection` 头。
- 命名事件、带 `id` 事件、data-only 帧（`Event` / `EventWithID` / `Data` / `Send`）与 `Raw` 原样透传哨兵；`Comment` / `Ping` / `Heartbeat` 保活；`Retry` 重连建议；`Started` 起流状态；`Close` 终止流并抑制客户端自动重连。
- 帧注入防护：`id` / `event` 字段值含换行或 NUL 时报错且不写字节；注释与原样透传载荷中的 `CRLF` 与孤立 `CR` 归一后逐行加前缀。
- 错误哨兵 `ErrClientGone` 与 `ErrStreamClosed`，可用 `errors.Is` 区分客户端断开与服务端已终止流。
- Functional Option `WithWriteTimeout`。
- 上游 SSE 解码器 `Decode(r io.Reader) iter.Seq2[Message, error]`，用于把上游大模型的 `text/event-stream` 转发给前端：支持 `CRLF` / `CR` / `LF` 三种行分隔符；字段值只剥掉冒号后的一个空格，`Data` 原样保留上游字节；多行 `data:` 以 `\n` 拼接；`ID` 与 `Retry` 按规范跨帧沿用；流尾不完整帧按规范丢弃。实现对照规范章节的官方示例做了交叉验证。
- 连接注册表 `Hub`（`NewHub` / `Subscribe` / `Push` / `Broadcast` / `Online` / `WithQueueSize`），供支付回调、踢下线等带外场景定向推送或广播。Hub 只投递到每个连接的有界队列、由持有连接的 handler 写出，无后台 goroutine，因此不需要关闭。
- Example 测试与交叉验证测试（写侧输出交由解码器解回、真实连接上的断开检测、Hub 端到端、真实 HTTP/2）。

### Changed

- ⚠ 不依赖任何 web 框架：入口改用 `http.ResponseWriter` 与 `*http.Request`，因此 net/http、gin、chi 都可直接使用，模块的直接依赖只剩 `github.com/gtkit/json/v2`。gin 调用方写 `ssex.NewStream(c.Writer, c.Request)`。
