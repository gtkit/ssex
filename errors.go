package ssex

import (
	"context"
	"errors"
	"fmt"
	"net"
)

// ErrClientGone 表示客户端已断开，本次写入不可能送达；用 errors.Is 判定。
//
// 典型处理：静默结束本次流并取消上游请求（如正在进行的大模型调用），无需告警。
// 判定依据是请求上下文已被取消（net/http 在客户端断开时取消它）或底层连接已关闭；
// 断开发生在上下文取消之前的竞态窗口内，错误归类为普通写失败。
// 写超时（客户端读取过慢）不属于本类，见 WithWriteTimeout。
//
// 错误链保留原因，因此 errors.Is(err, context.Canceled) 同样可判定。
var ErrClientGone = errors.New("ssex: client gone")

// ErrStreamClosed 表示流已由服务端显式终止（见 Stream.Close），不再接受写入；用 errors.Is 判定。
var ErrStreamClosed = errors.New("ssex: stream closed")

// clientGone 构造客户端断开错误，保留 cause 供 errors.Is 判定。
func clientGone(op string, cause error) error {
	return fmt.Errorf("%s: %w: %w", op, ErrClientGone, cause)
}

// classify 归类写入失败：客户端断开归入 ErrClientGone，其余原样包装。
func classify(ctx context.Context, op string, err error) error {
	if err == nil {
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return clientGone(op, ctxErr)
	}
	if errors.Is(err, net.ErrClosed) {
		return clientGone(op, err)
	}

	return fmt.Errorf("%s: %w", op, err)
}
