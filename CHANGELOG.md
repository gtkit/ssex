# Changelog

本文件记录本项目所有值得下游关注的变更。

格式遵循 [Keep a Changelog 1.1.0](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循 [Semantic Versioning 2.0.0](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### Added

- 新增 `gincompat/` 独立测试模块（自带 go.mod，主模块依赖清单因此仍不含任何 web 框架）：把 gin 集成做成可持续运行的回归测试，覆盖鉴权在起流前拒绝、快照+带外推送+终态 `Close`、心跳 goroutine 在 handler 返回前收尾、长连接不被 `http.Server.WriteTimeout` 截断、客户端断开判定、`Started()` 错误分界、应用停机收尾、与 Recovery / Logger 中间件共存。
- README 新增「Gin 集成」一节：完整 handler 模板（鉴权 → 起流错误 → 先订阅后快照 → 心跳收尾 → 四出口事件循环）、以 `Started()` 分界的错误处理规范与错误分级、中间件兼容清单与路由组隔离、`c.Writer` 生命周期约束。

### Fixed
- ⚠ 修复 README 与 `gincompat` 可执行模板的漂移：README 声称"模板的可执行版本在 `gincompat/handler_test.go`，模板变化会导致测试失败"，但上一轮只改了 README——`gincompat` 的 handler 仍只检查 uid、直接用裸 `orderID` 订阅、`load` 不带租户作用域。因此远端 CI 全绿也无法证明 README 新增的租户隔离与授权顺序是正确的。已让 `gincompat` handler 与 README 9.2 完全对齐（identity/tenant、资源授权、scoped key、同作用域读快照），并补上走真实 handler 链路的跨租户回归测试。
- ⚠ 修复完整模板的作用域漂移：授权用 `Authorize(ctx, tenantID, uid, orderID)`，读快照却只用 `Load(ctx, orderID)`。若 `orderID` 只在租户内唯一，快照可能读到另一个租户的数据。已改为授权返回内部安全标识、后续 `ScopeKey()` 与 `Load` 都沿用它。
- 修复 key 的分隔符碰撞：`tenantID + ":" + orderID` 在任一段含分隔符时会碰撞（`"a:b"+":"+"c"` 与 `"a"+":"+"b:c"` 得到同一个 key，两个不相关资源共享事件队列）。示范实现改用长度前缀编码并封装成统一构造函数，新增 `TestScopeKeyHasNoCollision`。
- ⚠ 修复简版 Hub handler 模板仍是"先 `Start` 后 `Subscribe`"：`Start` 最坏要等一个完整的 `WithWriteTimeout`，这段时间里的推送无人订阅、永久丢失。已改为先订阅再起流。
- ⚠ 修复简版模板读取 `uid` 但不检查空串：空 key 会让所有认证异常的请求共享同一个队列、互相收到对方的事件。模板改为在 `Subscribe` 之前拒掉空身份，并新增回归测试断言空 key 不进入注册表。注意这是**应用层**约束：`Hub.Subscribe` 不校验 key（校验需要给它加 error 返回，属破坏性变更），`Hub` 的 GoDoc 已明确把 key 的作用域治理责任写给调用方。
- ⚠ 修正 README 把 `delivered == 0` 当作"是否落库"判据的危险示例：`delivered` 只表示入队的本机连接数，不代表客户端收到、`Flush` 成功、其他实例的连接收到，也不代表事件没被后续溢出挤掉。已改为"先在事务里持久化状态与 revision、提交成功、再发布事件"，`delivered` / `dropped` 只用于监控。该表述此前与 6.7 的"存储是唯一事实源"直接矛盾。
- 修复 `Hub.Subscribe` 的 GoDoc 示例仍是只与快照比较的旧过滤方式（漏改，而 GoDoc 直接出现在 IDE 与 pkg.go.dev）：改为维护 `lastRev`、处理 revision 解析失败，并补上入队不等于交付、key 需带租户作用域、Hub 仅限当前进程三条说明。
- 修正 README 9.2 把整个 `Event` 写进日志的示例：`Event.Data` 可能含身份信息、登录 token、订单详情或 AI 对话内容。改为只记 key 脱敏值、`Event.ID` / `Event.Name`、载荷类型与 Trace ID。

- ⚠ 修复 `Stream.Heartbeat` 的 GoDoc 示例仍是会死锁的旧范式：README 与 gin 模板已修正，但公共 API 的 GoDoc 漏改，而它会直接出现在 IDE 与 pkg.go.dev，比 README 更容易被原样复制。
- ⚠ 修复文档模板的 revision 过滤只与快照比较：`revision <= snapRev` 能挡住订阅到读快照之间积压的旧事件，但挡不住快照之后乱序到达的回退版本（Hub 不保证跨并发推送方的到达顺序，先到 rev 10、后到 rev 9 时 9 也会被转发，前端状态回退）。模板改为维护随成功发送推进的 `lastRev`。
- 修复模板把 revision 解析失败当作 0 的静默丢弃：生产者忘填 `Event.ID` 时事件会因 `0 <= lastRev` 被默默吃掉且没有任何信号。`revisionOf` 改为返回是否有效，模板在无效时上报告警并跳过。
- ⚠ 修复文档模板会让 handler 永久阻塞的缺陷：心跳模板原先用同一个 channel 既传错误、又当"goroutine 已退出"信号。心跳 goroutine 只发送一次结果，主循环消费掉之后，`defer` 里的第二次接收永远等不到发送者。后果不止泄漏一个 goroutine——`defer release()` 按 LIFO 排在阻塞的 defer 之后，因此 Hub 注册表不清理、`Online` 长期不准、`http.Server.Shutdown` 一直等、gin `Context` 无法归还对象池。现改为错误走 `hbErr`、退出走 `close(hbDone)`，两个信号分离。已由确定性复现测试守住（修复前卡到超时，修复后 0.04s 通过）。
- ⚠ 修复文档模板的订阅顺序：README 9.2 原先是 `Load → Start → Subscribe`，与 6.7 自述的"先订阅、后取快照"矛盾。`Load` 与 `Subscribe` 之间发生的状态变更此时无人订阅、推送被直接丢弃，客户端拿到旧快照后再也收不到更新——正好命中订单支付与登录态场景。现改为 `Subscribe → 读快照 → Start → 发快照`，并在消费循环中跳过 revision 不大于快照的积压事件。新增两个测试覆盖"读快照期间发生变更"与"积压旧事件被过滤"。
- 修复 README 4.12 仍保留的不安全心跳示例（丢弃错误、不等待 goroutine 退出）。
- 修复 `hub.go` 中 `Subscribe` 的 GoDoc 示例：仍在用解耦前的 `ssex.NewStream(c)` 签名且忽略 `Start()` 返回值。GoDoc 会直接出现在 IDE 与 pkg.go.dev。
- 设置逐帧写截止时间失败时统一走错误分类：此前直接包装返回普通错误，连接检查通过后客户端立即断开的窗口里，一次正常断线会被记成生产告警。
- 修正 `withWriteDeadline` 中恢复截止时间失败的注释：原注释称"下一次写入会立即失败并返回 `ErrWriteTimeout`"，实际上下一次写入会先设置新的截止时间覆盖过期值。
- 起流的响应头刷新纳入 per-write 写截止时间（`WithWriteTimeout`）。此前 `WriteHeaders` 先清除 `http.Server.WriteTimeout`、随后直接刷新，而逐帧截止时间只作用于后续帧，异常或恶意连接可以让 handler 无上界地卡在起流上。
- 解除 `http.Server.WriteTimeout` 失败时不再静默继续：除 `http.ErrNotSupported`（底层不支持，降级）外一律返回错误，且此时响应头尚未提交、`Started()` 保持 false，调用方仍可改回普通 JSON。吞掉这个错误等于谎称长连接已保活，而连接其实仍会在全局写超时到期时被服务端中断。
- `Option` 与 `HubOption` 跳过 nil，不再因调用方传入零值 Option 而 panic。
- 修正 README 的心跳模板：原写法只发停止信号、不等 goroutine 退出。gin 的 `c.Writer` 是 `Context` 的内部字段，handler 返回后 `Context` 归还对象池并在下个请求中被重置，因此心跳 goroutine 若在 handler 返回后仍在写入，就会写到另一个请求的响应上。模板改为独立 ctx + `defer stop(); <-hbDone` 等待退出。

### Changed
- `Hub` 的 GoDoc 与 README 新增两条硬边界：**Hub 只管当前进程的连接**（多实例部署时连接在实例 A、回调打到实例 B 会导致 `Push` 找不到连接，需要每个实例都收到状态事件后各自推本地 Hub，或用一致性哈希路由；存储始终是事实源），以及 **key 必须是服务端计算的安全作用域**（两个租户的同名 ID 会落进同一 key 造成跨租户串流，禁止空 key，`Broadcast` 只用于全员可见内容）。
- README 9.2 模板把资源授权与读取快照拆开：授权先做可避免产生未授权订阅（会让 `Online` 出现伪在线）；key 改用 `tenantID:orderID` 形态。

- `Stream` 与 `Stream.Heartbeat` 的 GoDoc 补写生命周期约束：底层 `http.ResponseWriter` 只在 handler 执行期间有效，在 handler goroutine 之外写入的 goroutine 必须在 handler 返回前退出；`c.Copy()` 的副本只能读请求元数据（gin 会把副本的 `ResponseWriter` 置为 nil），不能用于写响应。
- `Event.Data` 补充所有权约束：投递给 `Hub.Push` / `Hub.Broadcast` 后即视为交出所有权，同一份载荷会被多个连接在各自 goroutine 里序列化，投递后修改其中的 map / 切片 / 指针会构成数据竞争。
- `Hub` 补充顺序契约：事件顺序在单个连接内按入队顺序保证，跨并发推送方的相对顺序由调度决定；状态事件应携带单调递增的 revision，消费端按 revision 取大者。状态查询类场景建议 `WithQueueSize(1)`。
- `ErrInvalidArgument` 的文档措辞修正：只有首帧构造失败时响应头尚未提交，流已开始之后出现该错误仅表示这一帧被拒绝。
- README 的 Hub handler 模板改为处理 `Start()` 错误、把心跳错误接进主 `select`，并新增应用级停机分支；新增大模型转发的上游约束（绑定下游 context、校验状态码与 `Content-Type`、始终关闭响应体、下游写失败即取消上游、token 不经 Hub）、优雅停机与运维边界（容量/限流归应用、建议监控的信号）两节。
- CI 的 action 固定到 commit SHA，避免版本 tag 被移动导致构建不可复现。
- `Hub` 的容量建议补上前提：latest-wins 按**到达顺序**保留最后一条、不比较业务 revision，因此 `WithQueueSize(1)` 只在同一 key 的推送已串行化时才安全；否则后到的旧版本会挤掉先到的新版本，消费端按 revision 过滤也救不回来。文档给出三条缓解方式。
- README 9.4 补充中间件约束：替换 `c.Writer` 的自定义中间件必须实现 `Unwrap() http.ResponseWriter` 并透传 `Flush`，否则 `SetWriteDeadline` 与"解除 `http.Server.WriteTimeout`"都会静默降级，长连接仍会被全局超时掐断；并给出自查办法。

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
