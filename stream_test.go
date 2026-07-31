package ssex

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestStreamSend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		event Event
		want  string
	}{
		{
			name:  "带名事件",
			event: Event{Name: "status", Data: map[string]string{"status": "paid"}},
			want:  "event: status\ndata: {\"status\":\"paid\"}\n\n",
		},
		{
			name:  "带 id 的事件",
			event: Event{ID: "7", Name: "chunk", Data: Raw("x")},
			want:  "id: 7\nevent: chunk\ndata: x\n\n",
		},
		{
			name:  "data-only 事件",
			event: Event{Data: Raw("[DONE]")},
			want:  "data: [DONE]\n\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			recorder := httptest.NewRecorder()
			stream := newTestStream(recorder)

			if err := stream.Send(tt.event); err != nil {
				t.Fatalf("Send() error = %v", err)
			}
			if got := recorder.Body.String(); got != tt.want {
				t.Fatalf("frame = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestStreamHeartbeat 验证心跳按间隔发帧并在上下文取消时退出。
func TestStreamHeartbeat(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(80 * time.Millisecond)
		cancel()
	}()

	err := NewStream(recorder, newTestRequest().WithContext(ctx)).Heartbeat(ctx, 20*time.Millisecond)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Heartbeat() error = %v, want context.Canceled", err)
	}
	if got := strings.Count(recorder.Body.String(), ": ping "); got < 2 {
		t.Fatalf("ping frames = %d, want >= 2 (body = %q)", got, recorder.Body.String())
	}
}

func TestStreamHeartbeatRejectsNonPositiveInterval(t *testing.T) {
	t.Parallel()

	for _, interval := range []time.Duration{0, -time.Second} {
		recorder := httptest.NewRecorder()
		stream := newTestStream(recorder)

		if err := stream.Heartbeat(context.Background(), interval); err == nil {
			t.Fatalf("Heartbeat(%s) error = nil, want error", interval)
		}
		if recorder.Body.Len() != 0 {
			t.Fatalf("rejected heartbeat must not write bytes, got %q", recorder.Body.String())
		}
	}
}

func TestStreamHeartbeatAfterClose(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	stream := newTestStream(recorder)
	if err := stream.Close(nil); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	err := stream.Heartbeat(context.Background(), time.Millisecond)
	if !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("Heartbeat() error = %v, want ErrStreamClosed", err)
	}
}

func TestStreamEventAutoStarts(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	stream := NewStream(recorder, newTestRequest())
	if stream.Started() {
		t.Fatal("Started() = true before first event, want false")
	}

	if err := stream.Event("status", map[string]any{"status": "pending"}); err != nil {
		t.Fatalf("Event() error = %v", err)
	}

	if !stream.Started() {
		t.Fatal("Started() = false after first event, want true")
	}
	if got := recorder.Header().Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
		t.Fatalf("content type = %q", got)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "event: status") {
		t.Fatalf("body missing event name: %s", body)
	}
	if !strings.Contains(body, `"status":"pending"`) {
		t.Fatalf("body missing payload: %s", body)
	}
}

func TestStreamPing(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	stream := NewStream(recorder, newTestRequest())
	ts := time.Date(2026, 3, 30, 10, 0, 0, 0, time.UTC)
	if err := stream.Ping(ts); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}

	body := recorder.Body.String()
	if strings.Contains(body, "event: ping") {
		t.Fatalf("body should not contain ping event: %s", body)
	}
	if !strings.Contains(body, ": ping 2026-03-30T10:00:00Z") {
		t.Fatalf("body missing ping comment: %s", body)
	}
}

func TestStreamErrorAutoStarts(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	stream := NewStream(recorder, newTestRequest())
	if err := stream.Error(map[string]string{"error": "boom"}); err != nil {
		t.Fatalf("Error() error = %v", err)
	}

	if !stream.Started() {
		t.Fatal("Started() = false after Error(), want true")
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "event: error") {
		t.Fatalf("body missing error event: %s", body)
	}
	if !strings.Contains(body, `"error":"boom"`) {
		t.Fatalf("body missing error payload: %s", body)
	}
}

// TestStreamCloseRejectsLaterWrites 验证显式终止流后不再写出任何字节:
// EventSource 在流正常结束后会自动重连,业务必须能明确终止一轮推送。
func TestStreamCloseRejectsLaterWrites(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	stream := NewStream(recorder, newTestRequest())
	if err := stream.Close(map[string]string{"reason": "delivered"}); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	body := recorder.Body.String()
	if !strings.Contains(body, "event: close") {
		t.Fatalf("body missing close event: %s", body)
	}
	if !strings.Contains(body, `"reason":"delivered"`) {
		t.Fatalf("body missing close payload: %s", body)
	}

	closedLen := recorder.Body.Len()
	writes := map[string]func() error{
		"Event":       func() error { return stream.Event("chunk", nil) },
		"EventWithID": func() error { return stream.EventWithID("1", "chunk", nil) },
		"Data":        func() error { return stream.Data(nil) },
		"Comment":     func() error { return stream.Comment("keepalive") },
		"Retry":       func() error { return stream.Retry(3000) },
		"Ping":        func() error { return stream.Ping(time.Unix(0, 0)) },
		"Error":       func() error { return stream.Error(nil) },
		"Close":       func() error { return stream.Close(nil) },
	}
	for name, write := range writes {
		err := write()
		if !errors.Is(err, ErrStreamClosed) {
			t.Fatalf("%s() after Close: error = %v, want ErrStreamClosed", name, err)
		}
		if errors.Is(err, ErrClientGone) {
			t.Fatalf("%s() after Close: error = %v, must not be judged as ErrClientGone", name, err)
		}
	}
	if recorder.Body.Len() != closedLen {
		t.Fatalf("writes after Close must not emit bytes, got %q", recorder.Body.String()[closedLen:])
	}
}

func TestStreamCommentAndRetryAutoStart(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	stream := NewStream(recorder, newTestRequest())
	if err := stream.Comment("keepalive"); err != nil {
		t.Fatalf("Comment() error = %v", err)
	}
	if err := stream.Retry(1500); err != nil {
		t.Fatalf("Retry() error = %v", err)
	}

	if !stream.Started() {
		t.Fatal("Started() = false after Comment/Retry, want true")
	}
	body := recorder.Body.String()
	if !strings.Contains(body, ": keepalive") {
		t.Fatalf("body missing comment frame: %s", body)
	}
	if !strings.Contains(body, "retry: 1500") {
		t.Fatalf("body missing retry frame: %s", body)
	}
}
