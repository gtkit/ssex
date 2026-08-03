package ssex

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"iter"
	"math"
	"strconv"
	"time"
)

// maxFrameSize 是解码单帧时内部拼接缓冲的字节上限。
//
// 对调用方而言更直接的口径是 maxDataSize:产出的 Message.Data 最多这么多字节。
// 两者恒差 1——拼接时每个 data 行都补一个换行,派发前再去掉末尾那个。
//
// 只靠 bufio.Scanner 的 buffer 上限不够:它约束的是单个 token,而这里的 token 是
// scanFrameLines 切出的一行,由大量短 data: 行组成的一帧仍可让缓冲无限增长。
// 因此另外累计整帧字节数。
const maxFrameSize = 1 << 20

// maxDataSize 是产出的 Message.Data 的字节上限,也是文档对外声明的口径。
const maxDataSize = maxFrameSize - 1

// maxLineSize 是单行的字节上限,给字段前缀留出余量。
//
// Scanner 的 token 是一整行,含 "data: " 这样的前缀;若直接用 maxFrameSize 当行上限,
// 前缀就会占掉 data 的配额——实测 payload 到 maxFrameSize-7 就被 Scanner 拒了,
// 与上面声明的口径不符。留出余量后,是否超限统一由 frameSize 的累计判断决定。
const maxLineSize = maxFrameSize + 64

// maxRetryMillis 是 retry 字段允许的最大毫秒数:再大就会让
// time.Duration(ms) * time.Millisecond 溢出成负值。
const maxRetryMillis = uint64(math.MaxInt64 / int64(time.Millisecond))

// bom 是 UTF-8 字节序标记,只可能出现在流首,不得进入字段名。
var bom = []byte("\xef\xbb\xbf")

// Message 是从上游 SSE 流解码出的一帧。
//
// 与推送方向的 Event 相对:Data 是原始字节,不做 JSON 解析也不做 trim——
// OpenAI 风格的 `[DONE]` 哨兵不是合法 JSON,而大模型的增量 token 常带前后空格,
// 任何 trim 都会损坏拼出的文本。
type Message struct {
	// ID 是当前生效的 last event ID。按 SSE 规范它是连接级状态:某帧带了 id 之后,
	// 后续未带 id 的帧沿用该值(规范原文 "The buffer does not get reset"),
	// 只有上游显式发送空 id 才重置。转发时据此保持 id 连续,断线续传起点才不会错。
	ID string
	// Name 是 `event:` 字段值,无该字段时为空串(默认事件)。每帧独立,不跨帧延续。
	Name string
	// Data 是该帧所有 `data:` 行以 \n 拼接的结果,空白原样保留。
	Data []byte
	// Retry 是当前生效的建议重连间隔,同为连接级状态;上游从未发送过合法值时为 0。
	Retry time.Duration
}

// Decode 按 SSE 规范(WHATWG event stream)解码 r,逐帧产出 Message。
//
// 典型用途是把上游大模型的 text/event-stream 转发给前端:
//
//	for msg, err := range ssex.Decode(resp.Body) {
//	    if err != nil {
//	        return err
//	    }
//	    if string(msg.Data) == "[DONE]" {
//	        break
//	    }
//	    if err := stream.Data(ssex.Raw(string(msg.Data))); err != nil {
//	        return err
//	    }
//	}
//
// 行分隔符支持 CRLF / CR / LF;流首 BOM、注释行(以 : 开头)与未知字段被忽略;
// 字段值只剥掉冒号后的一个空格。按规范,从未出现 data 字段的帧不产出
// (与浏览器 EventSource 一致),已出现但值为空串的帧照常产出。
// 读取出错时产出一次该错误并终止;读到流尾正常结束,不产出 io.EOF。
// 流尾残留的不完整帧(缺结尾空行)按规范丢弃,与浏览器一致——连接被中途掐断时,
// 残留字节通常是截断的半条 JSON,交出去只会让前端解析失败。
//
// 产出的 Message.Data 最多 1048575 字节(maxDataSize);超过即产出可判定为
// ErrFrameTooLarge 的错误,不静默截断。这个口径对单行与多行一致——多行时按
// 各 data 行拼接后的长度算。单行长度另有一个略高的硬上限,防止超长行撑爆缓冲。
//
// r 必须非 nil。nil 是调用方的编程错误,迭代时会 panic 而不是静默产出空流——
// 静默降级会把"上游连接没建起来"伪装成"上游没有输出"。
//
// 与规范的一处差异:Data 原样保留上游字节,不执行 UTF-8 解码替换。
// 转发链路上改写字节会让服务端交给前端的内容与上游不一致,而浏览器接收端
// 本身会按 UTF-8 解码;需要严格校验的调用方可自行处理 Data。
func Decode(r io.Reader) iter.Seq2[Message, error] {
	return func(yield func(Message, error) bool) {
		scanner := bufio.NewScanner(r)
		scanner.Buffer(make([]byte, 0, 4096), maxLineSize)
		scanner.Split(scanFrameLines)

		var (
			msg       Message
			data      []byte
			frameSize int
			first     = true
		)

		for scanner.Scan() {
			line := scanner.Bytes()
			if first {
				line = bytes.TrimPrefix(line, bom)
				first = false
			}

			// 空行分隔帧:data 缓冲为空说明本帧从未出现 data 字段,按规范不派发,
			// 并丢弃已累积的事件名,避免它串到下一帧。
			// id / retry 不清空:规范明确 "The buffer does not get reset",
			// 它们是连接级状态,清掉会让转发时的 id 连续性断掉。
			if len(line) == 0 {
				if data == nil {
					msg.Name = ""

					continue
				}
				msg.Data = data[:len(data)-1] // 去掉拼接时多出的末尾换行
				if !yield(msg, nil) {
					return
				}
				msg.Name, msg.Data, data, frameSize = "", nil, nil, 0

				continue
			}
			if line[0] == ':' { // 注释帧
				continue
			}

			name, value := splitField(line)
			switch name {
			case "event":
				msg.Name = string(value)
			case "data":
				// 累计整帧字节数:单行上限拦不住"大量短 data 行 + 迟迟不发空行"。
				frameSize += len(value) + 1
				if frameSize > maxFrameSize {
					yield(Message{}, fmt.Errorf("ssex: decode: %w: data exceeds %d bytes",
						ErrFrameTooLarge, maxDataSize))

					return
				}
				// value 指向 scanner 的复用缓冲,append 会拷贝内容,不会被下一行覆盖。
				data = append(append(data, value...), '\n')
			case "id":
				if !bytes.ContainsRune(value, 0) { // 含 NUL 的 id 按规范忽略
					msg.ID = string(value)
				}
			case "retry":
				if d, ok := retryValue(value); ok {
					msg.Retry = d
				}
			}
		}

		if err := scanner.Err(); err != nil {
			// 单行超长与整帧超长归到同一个判定下,原因链保留 bufio.ErrTooLong。
			if errors.Is(err, bufio.ErrTooLong) {
				yield(Message{}, fmt.Errorf("ssex: decode: %w: %w", ErrFrameTooLarge, err))

				return
			}
			yield(Message{}, fmt.Errorf("ssex: decode: %w", err))
		}
		// 流尾残留的不完整帧按规范丢弃:
		// "Once the end of the file is reached, any pending data must be discarded."
	}
}

// scanFrameLines 是 bufio.SplitFunc:按 SSE 规范以 CRLF / CR / LF 三者任一分行。
// 标准库的按 \n 切分会把 `a\rb` 当成一行,而 SSE 客户端会把它当两行。
func scanFrameLines(data []byte, atEOF bool) (advance int, token []byte, err error) {
	for i, b := range data {
		switch b {
		case '\n':
			return i + 1, data[:i], nil
		case '\r':
			// CR 在缓冲末尾时无法判断后面是否跟 LF(CRLF),先要更多字节,
			// 否则会把 CRLF 误判成 CR + 一个空行,凭空多派发一帧。
			if i+1 < len(data) {
				if data[i+1] == '\n' {
					return i + 2, data[:i], nil
				}

				return i + 1, data[:i], nil
			}
			if atEOF {
				return i + 1, data[:i], nil
			}

			return 0, nil, nil
		}
	}
	if atEOF && len(data) > 0 {
		return len(data), data, nil
	}

	return 0, nil, nil
}

// splitField 拆分字段行:冒号前是字段名,冒号后剥掉恰好一个空格作为值;
// 无冒号时整行是字段名、值为空串。只剥一个空格是规范要求——
// 多剥会吃掉大模型增量 token 的前导空格。
func splitField(line []byte) (name string, value []byte) {
	field, value, found := bytes.Cut(line, []byte(":"))
	if !found {
		return string(line), nil
	}
	if len(value) > 0 && value[0] == ' ' {
		value = value[1:]
	}

	return string(field), value
}

// retryValue 解析 retry 字段:规范要求值全为 ASCII 数字,否则整个字段忽略。
//
// 用 ParseUint 而非 Atoi:后者接受 "+5" / "-5" 这类带符号写法。
// 另外必须卡上限,否则极大的合法数字乘以 time.Millisecond 会溢出成负的 Duration。
func retryValue(v []byte) (time.Duration, bool) {
	if len(v) == 0 {
		return 0, false
	}
	for _, b := range v {
		if b < '0' || b > '9' {
			return 0, false
		}
	}

	ms, err := strconv.ParseUint(string(v), 10, 64)
	if err != nil || ms > maxRetryMillis {
		return 0, false
	}

	return time.Duration(ms) * time.Millisecond, true
}
