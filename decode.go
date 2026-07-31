package ssex

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"iter"
	"strconv"
	"time"
)

// maxFrameSize 是单帧的字节上限。bufio.Scanner 默认 64KB 对含 base64 载荷的帧偏小;
// 超限时返回错误而非静默截断。
const maxFrameSize = 1 << 20

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
// 单帧超过 1MB 时产出错误,不静默截断。
func Decode(r io.Reader) iter.Seq2[Message, error] {
	return func(yield func(Message, error) bool) {
		scanner := bufio.NewScanner(r)
		scanner.Buffer(make([]byte, 0, 4096), maxFrameSize)
		scanner.Split(scanFrameLines)

		var (
			msg   Message
			data  []byte
			first = true
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
				msg.Name, msg.Data, data = "", nil, nil

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

// retryValue 解析 retry 字段:规范要求值全为 ASCII 数字,否则整个字段忽略
// (strconv.Atoi 会接受 "+5" / "-5",不能直接用)。
func retryValue(v []byte) (time.Duration, bool) {
	if len(v) == 0 {
		return 0, false
	}
	for _, b := range v {
		if b < '0' || b > '9' {
			return 0, false
		}
	}
	ms, err := strconv.Atoi(string(v))
	if err != nil { // 超出 int 范围
		return 0, false
	}

	return time.Duration(ms) * time.Millisecond, true
}
