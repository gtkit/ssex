package ssex

import (
	"bufio"
	"errors"
	"strings"
	"testing"
	"time"
)

// collectMessages 把 Decode 的产出收集成切片,首个错误终止收集。
func collectMessages(t *testing.T, input string) ([]Message, error) {
	t.Helper()

	var (
		msgs []Message
		err  error
	)
	for msg, e := range Decode(strings.NewReader(input)) {
		if e != nil {
			err = e

			break
		}
		msgs = append(msgs, msg)
	}

	return msgs, err
}

func TestDecode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  []Message
	}{
		{
			name:  "命名事件",
			input: "event: chunk\ndata: {\"delta\":\"hi\"}\n\n",
			want:  []Message{{Name: "chunk", Data: []byte(`{"delta":"hi"}`)}},
		},
		{
			name:  "data-only 帧",
			input: "data: {\"delta\":\"hi\"}\n\n",
			want:  []Message{{Data: []byte(`{"delta":"hi"}`)}},
		},
		{
			name:  "CR 分行",
			input: "event: chunk\rdata: hi\r\r",
			want:  []Message{{Name: "chunk", Data: []byte("hi")}},
		},
		{
			name:  "CRLF 分行",
			input: "event: chunk\r\ndata: hi\r\n\r\n",
			want:  []Message{{Name: "chunk", Data: []byte("hi")}},
		},
		{
			name:  "注释行与未知字段被忽略",
			input: ": keepalive\nfoo: bar\ndata: x\n\n",
			want:  []Message{{Data: []byte("x")}},
		},
		{
			name:  "流首 BOM 被忽略",
			input: "\xef\xbb\xbfdata: x\n\n",
			want:  []Message{{Data: []byte("x")}},
		},
		{
			name:  "只剥一个前导空格",
			input: "data:  hi\n\n",
			want:  []Message{{Data: []byte(" hi")}},
		},
		{
			name:  "尾随空格保留",
			input: "data: hi \n\n",
			want:  []Message{{Data: []byte("hi ")}},
		},
		{
			name:  "无冒号的字段行",
			input: "data\n\n",
			want:  []Message{{Data: []byte("")}},
		},
		{
			name:  "多行 data 以换行拼接",
			input: "data: line1\ndata: line2\n\n",
			want:  []Message{{Data: []byte("line1\nline2")}},
		},
		{
			name:  "空 data 行",
			input: "data: a\ndata: \n\n",
			want:  []Message{{Data: []byte("a\n")}},
		},
		{
			name:  "出现 data 字段但值为空",
			input: "data: \n\n",
			want:  []Message{{Data: []byte("")}},
		},
		{
			name:  "DONE 哨兵原样返回",
			input: "data: [DONE]\n\n",
			want:  []Message{{Data: []byte("[DONE]")}},
		},
		{
			name:  "无 data 字段的帧不产出且事件名不串到下一帧",
			input: "event: ping\n\ndata: x\n\n",
			want:  []Message{{Data: []byte("x")}},
		},
		{
			name:  "带 id",
			input: "id: 42\ndata: x\n\n",
			want:  []Message{{ID: "42", Data: []byte("x")}},
		},
		{
			name:  "id 含 NUL 被忽略",
			input: "id: 4\x002\ndata: x\n\n",
			want:  []Message{{Data: []byte("x")}},
		},
		{
			name:  "合法 retry",
			input: "retry: 3000\ndata: x\n\n",
			want:  []Message{{Data: []byte("x"), Retry: 3000 * time.Millisecond}},
		},
		{
			name:  "非数字 retry 被忽略",
			input: "retry: abc\ndata: x\n\n",
			want:  []Message{{Data: []byte("x")}},
		},
		{
			name:  "带符号的 retry 被忽略",
			input: "retry: +5\ndata: x\n\n",
			want:  []Message{{Data: []byte("x")}},
		},
		{
			// 规范:"Once the end of the file is reached, any pending data must be discarded."
			name:  "末帧无结尾空行按规范丢弃",
			input: "data: x",
			want:  nil,
		},
		{
			name:  "完整帧后残留不完整帧时只产出完整的那个",
			input: "data: a\n\ndata: trunc",
			want:  []Message{{Data: []byte("a")}},
		},
		{
			// 规范:"The buffer does not get reset",id 是连接级状态
			name:  "id 跨帧粘滞",
			input: "id: 1\ndata: a\n\ndata: b\n\n",
			want:  []Message{{ID: "1", Data: []byte("a")}, {ID: "1", Data: []byte("b")}},
		},
		{
			name:  "空 id 重置粘滞值",
			input: "id: 1\ndata: a\n\nid\ndata: b\n\n",
			want:  []Message{{ID: "1", Data: []byte("a")}, {Data: []byte("b")}},
		},
		{
			name:  "无 data 的帧里的 id 仍然生效",
			input: "id: 9\n\ndata: a\n\n",
			want:  []Message{{ID: "9", Data: []byte("a")}},
		},
		{
			name:  "retry 跨帧粘滞",
			input: "retry: 3000\ndata: a\n\ndata: b\n\n",
			want: []Message{
				{Data: []byte("a"), Retry: 3000 * time.Millisecond},
				{Data: []byte("b"), Retry: 3000 * time.Millisecond},
			},
		},
		{
			name:  "连续多帧",
			input: "data: a\n\ndata: b\n\n",
			want:  []Message{{Data: []byte("a")}, {Data: []byte("b")}},
		},
		{
			name:  "空流",
			input: "",
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := collectMessages(t, tt.input)
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("frames = %d, want %d (got %+v)", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i].ID != tt.want[i].ID || got[i].Name != tt.want[i].Name ||
					string(got[i].Data) != string(tt.want[i].Data) || got[i].Retry != tt.want[i].Retry {
					t.Fatalf("frame[%d] = {ID:%q Name:%q Data:%q Retry:%s}, want {ID:%q Name:%q Data:%q Retry:%s}",
						i, got[i].ID, got[i].Name, got[i].Data, got[i].Retry,
						tt.want[i].ID, tt.want[i].Name, tt.want[i].Data, tt.want[i].Retry)
				}
			}
		})
	}
}

// TestDecodeSpecExamples 用 HTML 规范 server-sent-events 章节的官方示例做交叉验证:
// 这些示例连同规范给出的预期结果一起，用来核对本实现对规范的理解。
func TestDecodeSpecExamples(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  []Message
	}{
		{
			// 规范示例:一个事件,data 由三行拼接
			name:  "多行 data",
			input: "data: YHOO\ndata: +2\ndata: 10\n\n",
			want:  []Message{{Data: []byte("YHOO\n+2\n10")}},
		},
		{
			// 规范示例:注释被忽略、id 设置后粘滞、空 id 重置、值只剥一个空格
			name: "注释与 id 重置",
			input: ": test stream\n\n" +
				"data: first event\nid: 1\n\n" +
				"data:second event\nid\n\n" +
				"data:  third event\n\n",
			want: []Message{
				{ID: "1", Data: []byte("first event")},
				{Data: []byte("second event")},
				{Data: []byte(" third event")},
			},
		},
		{
			// 规范示例:两个事件,data 分别是空串与单个换行;末尾的 `data:` 不完整被丢弃
			name:  "空 data 与末尾不完整帧",
			input: "data\n\ndata\ndata\n\ndata:",
			want: []Message{
				{Data: []byte("")},
				{Data: []byte("\n")},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := collectMessages(t, tt.input)
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("frames = %d, want %d (got %+v)", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i].ID != tt.want[i].ID || got[i].Name != tt.want[i].Name ||
					string(got[i].Data) != string(tt.want[i].Data) {
					t.Fatalf("frame[%d] = {ID:%q Name:%q Data:%q}, want {ID:%q Name:%q Data:%q}",
						i, got[i].ID, got[i].Name, got[i].Data,
						tt.want[i].ID, tt.want[i].Name, tt.want[i].Data)
				}
			}
		})
	}
}

// errReader 先吐出 prefix,随后持续返回 err。
type errReader struct {
	prefix string
	err    error
	done   bool
}

func (r *errReader) Read(p []byte) (int, error) {
	if !r.done {
		r.done = true

		return copy(p, r.prefix), nil
	}

	return 0, r.err
}

func TestDecodeReadError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("boom")

	var (
		got    []Message
		gotErr error
	)
	for msg, err := range Decode(&errReader{prefix: "data: a\n\n", err: wantErr}) {
		if err != nil {
			gotErr = err

			break
		}
		got = append(got, msg)
	}

	if len(got) != 1 || string(got[0].Data) != "a" {
		t.Fatalf("frames = %+v, want single frame with data %q", got, "a")
	}
	if !errors.Is(gotErr, wantErr) {
		t.Fatalf("error = %v, want %v", gotErr, wantErr)
	}
}

// TestDecodeFrameTooLong 覆盖两条超限路径,它们都必须归入 ErrFrameTooLarge:
// 帧内 data 累计超限由 frameSize 判断拦下,单行长度超过硬上限由 Scanner 拦下。
func TestDecodeFrameTooLong(t *testing.T) {
	t.Parallel()

	// 路径一:data 内容超过帧上限(行长仍在 maxLineSize 之内)
	_, err := collectMessages(t, "data: "+strings.Repeat("x", maxFrameSize+1)+"\n\n")
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("帧内累计超限 error = %v, want ErrFrameTooLarge", err)
	}

	// 路径二:单行长度超过硬上限,由 Scanner 拦下并保留原因链
	_, err = collectMessages(t, "data: "+strings.Repeat("x", maxLineSize+1)+"\n\n")
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("超长行 error = %v, want ErrFrameTooLarge", err)
	}
	if !errors.Is(err, bufio.ErrTooLong) {
		t.Fatalf("超长行 error = %v, 原因链应保留 bufio.ErrTooLong", err)
	}
}

func TestDecodeStopsOnBreak(t *testing.T) {
	t.Parallel()

	count := 0
	for range Decode(strings.NewReader("data: a\n\ndata: b\n\n")) {
		count++

		break
	}
	if count != 1 {
		t.Fatalf("iterations = %d, want 1", count)
	}
}

// BenchmarkDecode 测 token 流形态的解码热路径(100 个 data-only 帧)。
func BenchmarkDecode(b *testing.B) {
	var sb strings.Builder
	for range 100 {
		sb.WriteString("data: {\"delta\":\" token\"}\n\n")
	}
	input := sb.String()

	b.ReportAllocs()
	for b.Loop() {
		for _, err := range Decode(strings.NewReader(input)) {
			if err != nil {
				b.Fatalf("Decode() error = %v", err)
			}
		}
	}
}
