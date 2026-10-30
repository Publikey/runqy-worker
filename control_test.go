package worker

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestProcessorHold(t *testing.T) {
	p := newProcessor(nil, NewServeMux(), Config{Queues: map[string]int{"a.high": 2, "a.low": 1, "b.default": 1}, Logger: NewStdLogger()})
	if got := len(p.dequeueQueues()); got != 3 {
		t.Fatalf("dequeue queues = %d, want 3 (each queue once)", got)
	}
	p.hold("a")
	for _, q := range p.dequeueQueues() {
		if parentQueueName(q) == "a" {
			t.Fatalf("held parent still dequeued: %v", p.dequeueQueues())
		}
	}
	p.trackInflight("t1", "a.high")
	p.trackInflight("t2", "b.default")
	if p.activeFor("a") != 1 || p.activeFor("b") != 1 || p.activeFor() != 2 {
		t.Errorf("activeFor a=%d b=%d all=%d", p.activeFor("a"), p.activeFor("b"), p.activeFor())
	}
	p.release("a")
	if got := len(p.dequeueQueues()); got != 3 || len(p.heldParents()) != 0 {
		t.Errorf("after release: %d queues, held %v", got, p.heldParents())
	}
}

func newControlTestWorker() *Worker {
	cfg := Config{Queues: map[string]int{"a.default": 1, "b.default": 1}, Logger: NewStdLogger(), Version: "0.3.1"}
	w := New(cfg)
	w.queueStates = map[string]*QueueState{"a": {Name: "a"}, "b": {Name: "b"}}
	w.processor = newProcessor(nil, w.mux, w.config)
	w.heartbeat = newHeartbeat(nil, w.config, w.queueStates, "running")
	w.controller = newController(w)
	return w
}

func heartbeatFields(w *Worker) map[string]interface{} {
	data := map[string]interface{}{"status": "running"}
	w.controller.addHeartbeatFields(data)
	return data
}

func send(w *Worker, msg ControlMessage) {
	b, _ := json.Marshal(msg)
	w.controller.handle(string(b))
}

func TestControlDrainAndResume(t *testing.T) {
	w := newControlTestWorker()

	// Draining one queue leaves the worker running.
	send(w, ControlMessage{ID: "1", Action: ActionDrain, Queue: "a"})
	if f := heartbeatFields(w); f["status"] != "running" || f["held"] != "a" {
		t.Fatalf("after drain of a: %v", f)
	}

	// Draining everything: draining while a task runs, drained once it finished.
	w.processor.trackInflight("t1", "b.default")
	send(w, ControlMessage{ID: "2", Action: ActionDrain})
	if f := heartbeatFields(w); f["status"] != "draining" {
		t.Fatalf("status = %v, want draining", f["status"])
	}
	w.processor.untrackInflight("t1")
	f := heartbeatFields(w)
	if f["status"] != "drained" || f["held"] != "a,b" {
		t.Fatalf("after the task finished: %v", f)
	}
	if !strings.Contains(f["control_actions"].(string), ActionWorkerUpdate) {
		t.Errorf("control_actions = %v", f["control_actions"])
	}
	var last controlResult
	json.Unmarshal([]byte(f["control"].(string)), &last)
	if last.ID != "2" || last.State != "done" {
		t.Errorf("last command = %+v", last)
	}

	send(w, ControlMessage{ID: "3", Action: ActionResume})
	if f := heartbeatFields(w); f["status"] != "running" || f["held"] != "" {
		t.Fatalf("after resume: %v", f)
	}

	// Commands for a queue this worker does not serve are ignored.
	send(w, ControlMessage{ID: "4", Action: ActionDrain, Queue: "other"})
	if len(w.processor.heldParents()) != 0 {
		t.Error("drain for another queue was applied")
	}
}

func TestControlRejectsBadCommands(t *testing.T) {
	w := newControlTestWorker()
	send(w, ControlMessage{ID: "1", Action: "explode"})
	var last controlResult
	json.Unmarshal([]byte(heartbeatFields(w)["control"].(string)), &last)
	if last.State != "failed" {
		t.Errorf("unknown action: %+v", last)
	}

	if _, err := w.controller.workerUpdate(ControlMessage{Version: "latest"}); err == nil {
		t.Error("worker_update accepted a non-version")
	}
	if runtime.GOOS != "windows" {
		if done, err := w.controller.workerUpdate(ControlMessage{Version: "v0.3.1"}); err != nil || !strings.Contains(done, "already") {
			t.Errorf("same version: %q %v", done, err)
		}
	}
}

func releaseArchive(t *testing.T, binary []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for name, content := range map[string][]byte{"README.md": []byte("readme"), "runqy-worker": binary} {
		tw.WriteHeader(&tar.Header{Name: name, Mode: 0o755, Size: int64(len(content)), Typeflag: tar.TypeReg})
		tw.Write(content)
	}
	tw.Close()
	gz.Close()
	return buf.Bytes()
}

func TestDownloadRelease(t *testing.T) {
	archive := releaseArchive(t, []byte("#!/bin/sh\necho runqy-worker 9.9.9\n"))
	name := fmt.Sprintf("runqy-worker_9.9.9_%s_%s.tar.gz", runtime.GOOS, runtime.GOARCH)
	sum := sha256.Sum256(archive)
	checksums := hex.EncodeToString(sum[:]) + "  " + name + "\n"

	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v9.9.9/checksums.txt":
			rw.Write([]byte(checksums))
		case "/v9.9.9/" + name:
			rw.Write(archive)
		default:
			http.NotFound(rw, r)
		}
	}))
	defer srv.Close()

	dest := filepath.Join(t.TempDir(), "runqy-worker.new")
	if err := downloadRelease(context.Background(), srv.URL, "9.9.9", dest); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(dest); !strings.Contains(string(got), "9.9.9") {
		t.Fatalf("extracted binary = %q", got)
	}

	// sha256sum's binary-mode marker ("<sum> *<file>") is accepted too.
	checksums = hex.EncodeToString(sum[:]) + " *" + name + "\r\n"
	if err := downloadRelease(context.Background(), srv.URL, "9.9.9", dest); err != nil {
		t.Fatalf("binary-mode checksums: %v", err)
	}

	// A tampered archive is refused.
	checksums = strings.Repeat("0", 64) + "  " + name + "\n"
	if err := downloadRelease(context.Background(), srv.URL, "9.9.9", dest); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Errorf("tampered archive: %v", err)
	}
	// A missing release is an error.
	if err := downloadRelease(context.Background(), srv.URL, "1.0.0", dest); err == nil {
		t.Error("missing release accepted")
	}
}

// worker_update hands its outcome to the re-executed binary, which publishes it instead of
// leaving the heartbeat on "running".
func TestControlHandoffAfterWorkerUpdate(t *testing.T) {
	t.Setenv(controlHandoffEnv, `{"id":"x1","action":"worker_update","state":"done","message":"updated from 0.4.0 to 0.4.1","at":1}`)
	w := newControlTestWorker()
	data := heartbeatFields(w)
	if s, _ := data["control"].(string); !strings.Contains(s, `"state":"done"`) || !strings.Contains(s, "0.4.1") {
		t.Fatalf("control field = %v", data["control"])
	}
	if os.Getenv(controlHandoffEnv) != "" {
		t.Fatal("handoff env not cleared")
	}
}

func TestLastLineOfTraceback(t *testing.T) {
	tb := "Traceback (most recent call last):\n  File \"main.py\", line 3, in refresh\njson.decoder.JSONDecodeError: Expecting value: line 1 column 11 (char 10)\n"
	if got := lastLine(tb); got != "json.decoder.JSONDecodeError: Expecting value: line 1 column 11 (char 10)" {
		t.Fatalf("lastLine = %q", got)
	}
	if got := lastLine("busy"); got != "busy" {
		t.Fatalf("lastLine = %q", got)
	}
}

func TestProcessorSkipsQueueWhoseProcessIsDown(t *testing.T) {
	p := newProcessor(nil, NewServeMux(), Config{Queues: map[string]int{"a.high": 2, "a.low": 1, "b.default": 1}, Logger: NewStdLogger()})
	down := newProcessSupervisor(&DeploymentResult{}, BootstrapDeploymentConfig{}, nil, NewStdLogger()) // never started: unhealthy
	// b has no supervisor (one_shot mode): always ready.
	p.SetQueueStates(map[string]*QueueState{"a": {Name: "a", Supervisor: down}, "b": {Name: "b"}})
	for i := 0; i < 20; i++ { // the order is redrawn at every call
		got := p.dequeueQueues()
		if len(got) != 1 || got[0] != "b.default" {
			t.Fatalf("dequeue queues = %v, want only b.default while a's process is down", got)
		}
	}
	p.hold("b")
	if got := p.dequeueQueues(); len(got) != 0 {
		t.Errorf("dequeue queues = %v, want none (a down, b held)", got)
	}
}

func TestExitDegradedStopsTheWorker(t *testing.T) {
	w := newControlTestWorker()
	w.exitDegraded("a")
	w.exitDegraded("b") // a second degraded queue keeps the first reason
	select {
	case <-w.done:
	default:
		t.Fatal("worker not asked to shut down")
	}
	if !errors.Is(w.exitErr, ErrProcessDegraded) || !strings.Contains(w.exitErr.Error(), "queue a") {
		t.Errorf("exitErr = %v, want ErrProcessDegraded for queue a", w.exitErr)
	}
}
