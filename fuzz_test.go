package ssex

import (
	"errors"
	"strings"
	"testing"
)

// FuzzDecode 保证解码器对任意字节流都不 panic、不越界、不产出负 Retry，
// 且对超限帧只返回可判定的 ErrFrameTooLarge。
//
// 种子语料覆盖三种换行、非法 UTF-8、超大 retry、超长多行帧与提前停止迭代。
func FuzzDecode(f *testing.F) {
	seeds := []string{
		"data: hello\n\n",
		"event: chunk\rdata: hi\r\r",
		"event: chunk\r\ndata: hi\r\n\r\n",
		": comment\nfoo: bar\ndata: x\n\n",
		"\xef\xbb\xbfdata: x\n\n",
		"data:  leading\ndata: trailing \n\n",
		"data\n\ndata\ndata\n\ndata:",
		"id: 1\ndata: a\n\nid\ndata: b\n\n",
		"id: 4\x002\ndata: x\n\n",
		"retry: 3000\ndata: x\n\n",
		"retry: 9223372036854775807\ndata: x\n\n",
		"retry: +5\ndata: x\n\n",
		"data: [DONE]\n\n",
		"data: \xff\xfe\xc3\n\n",
		"data: " + strings.Repeat("x", 70000) + "\n\n",
		strings.Repeat("data: y\n", 5000) + "\n",
		"\r",
		"\n\n\n",
		"",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		var count int
		for msg, err := range Decode(strings.NewReader(input)) {
			if err != nil {
				// 唯一允许的越界错误必须是可判定的
				if !errors.Is(err, ErrFrameTooLarge) {
					t.Fatalf("unexpected error kind: %v", err)
				}

				break
			}
			if msg.Retry < 0 {
				t.Fatalf("negative retry: %s", msg.Retry)
			}
			if len(msg.Data) > maxFrameSize {
				t.Fatalf("data len %d exceeds limit %d", len(msg.Data), maxFrameSize)
			}

			count++
			if count > 3 { // 顺带覆盖提前停止迭代的路径
				break
			}
		}
	})
}

// FuzzRoundTrip 保证任意载荷经写侧写出后,读侧都能解回同一份字节,
// 且伪造的字段行永远无法逃出 data 字段。
func FuzzRoundTrip(f *testing.F) {
	seeds := []string{
		"hello",
		"[DONE]",
		" leading space",
		"line1\nline2",
		"a\rb",
		"x\revent: evil\rdata: pwned",
		"\xff\xfe",
		"",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, payload string) {
		writer, recorder := newTestWriter(t)
		if err := writer.Data(Raw(payload)); err != nil {
			t.Fatalf("Data() error = %v", err)
		}

		var got []Message
		for msg, err := range Decode(strings.NewReader(recorder.Body.String())) {
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			got = append(got, msg)
		}

		if len(got) != 1 {
			t.Fatalf("frames = %d, want exactly 1 (payload %q 逃出了 data 字段)", len(got), payload)
		}
		if got[0].Name != "" {
			t.Fatalf("event name = %q, want empty (payload %q 伪造了事件名)", got[0].Name, payload)
		}
		if want := normalizeNewlines(payload); string(got[0].Data) != want {
			t.Fatalf("data = %q, want %q", got[0].Data, want)
		}
	})
}
