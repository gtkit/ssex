package ssex

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
)

// ErrClientGone 表示客户端已断开，本次写入不可能送达；用 errors.Is 判定。
//
// 典型处理：静默结束本次流并取消上游请求（如正在进行的大模型调用），无需告警。
// 判定依据是请求上下文已被取消（net/http 在客户端断开时取消它）或底层连接已关闭；
// 断开发生在上下文取消之前的竞态窗口内，错误归类为普通写失败。
// 客户端读取过慢导致的写超时属于 ErrWriteTimeout，不属于本类。
//
// 错误链保留原因，因此 errors.Is(err, context.Canceled) 同样可判定。
var ErrClientGone = errors.New("ssex: client gone")

// ErrStreamClosed 表示流已由服务端显式终止（见 Stream.Close），不再接受写入；用 errors.Is 判定。
var ErrStreamClosed = errors.New("ssex: stream closed")

// ErrInvalidArgument 表示调用方传入了非法参数：字段值含换行或 NUL、retry 为负、
// 心跳间隔非正等。这类错误在帧构造阶段就返回，不写出任何字节。
//
// 若它发生在**首帧**（流尚未开始，Started() 为 false），响应头还没提交，
// 调用方可以改用普通 JSON 响应回错；流已开始之后出现的这类错误只表示该帧被拒绝。
var ErrInvalidArgument = errors.New("ssex: invalid argument")

// ErrWriteTimeout 表示单帧写入超过了写截止时间（见 WithWriteTimeout），
// 通常意味着客户端读取过慢。它不等于客户端断开，因此不会被判定为 ErrClientGone。
var ErrWriteTimeout = errors.New("ssex: write timeout")

// ErrFrameTooLarge 表示解码时单帧字节数超过上限（1MB），用 errors.Is 判定。
// 单行超长与多行 data 累计超长都归入本类。
var ErrFrameTooLarge = errors.New("ssex: frame too large")

// invalidArgf 构造可判定为 ErrInvalidArgument 的参数错误。
func invalidArgf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalidArgument}, args...)...)
}

// clientGone 构造客户端断开错误，保留 cause 供 errors.Is 判定。
func clientGone(op string, cause error) error {
	return fmt.Errorf("%s: %w: %w", op, ErrClientGone, cause)
}

// classify 归类写入失败:写超时归入 ErrWriteTimeout,客户端断开归入 ErrClientGone,
// 其余原样包装。写超时的判定放在断开之前——慢客户端的连接仍然活着,
// 归错类会让调用方误判为"前端已关页面"而静默丢弃告警。
func classify(ctx context.Context, op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return fmt.Errorf("%s: %w: %w", op, ErrWriteTimeout, err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return clientGone(op, ctxErr)
	}
	if errors.Is(err, net.ErrClosed) {
		return clientGone(op, err)
	}

	return fmt.Errorf("%s: %w", op, err)
}
