package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Control channel: the server publishes commands on Redis Pub/Sub and every worker
// subscribes to three channels:
//
//	runqy:control              all workers
//	runqy:control:<queue>      workers serving that parent queue
//	runqy:control:<worker_id>  one worker
//
// Delivery is fire-and-forget (a worker that is not subscribed at that moment misses the
// command); the outcome of the last command is published in the heartbeat ("control").
const controlChannel = "runqy:control"

// Control actions.
const (
	ActionDrain        = "drain"         // stop taking tasks, let in-flight ones finish
	ActionResume       = "resume"        // undo drain
	ActionCodeUpdate   = "code_update"   // git update + dependencies + process restart
	ActionWorkerUpdate = "worker_update" // replace this binary with a release and re-exec
	ActionConfigReload = "config_reload" // ask the Python process to reload its config
)

// controlActions is published in the heartbeat so the server knows what this worker
// supports (workers without the field predate the control channel).
var controlActions = []string{ActionDrain, ActionResume, ActionCodeUpdate, ActionWorkerUpdate, ActionConfigReload}

// Default waits for in-flight tasks before a code or worker update.
const (
	defaultUpdateDrainTimeout = 30 * time.Minute
	defaultReloadTimeout      = 5 * time.Minute
)

// ControlMessage is a command published on a control channel.
type ControlMessage struct {
	ID      string `json:"id"`
	Action  string `json:"action"`
	Queue   string `json:"queue,omitempty"`   // parent queue targeted (empty: all of this worker's queues)
	Version string `json:"version,omitempty"` // worker_update: release to install (e.g. "0.3.2")
	Timeout string `json:"timeout,omitempty"` // wait for in-flight tasks (Go duration)
}

// controlResult is the outcome of the last command, published in the heartbeat.
type controlResult struct {
	ID      string `json:"id"`
	Action  string `json:"action"`
	State   string `json:"state"` // running, done, failed
	Message string `json:"message,omitempty"`
	At      int64  `json:"at"`
}

type controller struct {
	w      *Worker
	logger Logger

	mu       sync.Mutex
	last     *controlResult
	busy     string // long command running (code_update, worker_update, config_reload)
	draining bool   // a drain covers every queue of this worker

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newController(w *Worker) *controller {
	ctx, cancel := context.WithCancel(context.Background())
	return &controller{w: w, logger: w.logger, ctx: ctx, cancel: cancel}
}

// start subscribes to the control channels and publishes the control state in the
// heartbeat.
func (c *controller) start() {
	c.w.heartbeat.setControlSource(c.addHeartbeatFields)
	c.wg.Add(1)
	go c.listen()
}

func (c *controller) stop() {
	c.cancel()
	c.wg.Wait()
}

// channels lists the channels this worker listens on.
func (c *controller) channels() []string {
	out := []string{controlChannel, controlChannel + ":" + c.w.heartbeat.workerID}
	for _, parent := range c.parents() {
		out = append(out, controlChannel+":"+parent)
	}
	return out
}

// parents lists this worker's parent queues.
func (c *controller) parents() []string {
	seen := map[string]bool{}
	for name := range c.w.queueStates {
		seen[name] = true
	}
	if len(seen) == 0 {
		for q := range c.w.config.Queues {
			seen[parentQueueName(q)] = true
		}
	}
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// listen keeps a subscription open, re-subscribing with the current client after a
// connection loss or a Redis reconnection.
func (c *controller) listen() {
	defer c.wg.Done()
	for {
		rdb := c.w.heartbeat.client()
		ps := rdb.Subscribe(c.ctx, c.channels()...)
		if _, err := ps.Receive(c.ctx); err != nil {
			ps.Close()
			if c.ctx.Err() != nil {
				return
			}
			c.logger.Warn("Control channel subscription failed (retrying): %v", err)
		} else {
			c.logger.Info("Listening for control commands on %s", strings.Join(c.channels(), ", "))
			ch := ps.Channel()
		read:
			for {
				select {
				case <-c.ctx.Done():
					ps.Close()
					return
				case m, ok := <-ch:
					if !ok {
						break read
					}
					c.handle(m.Payload)
				}
			}
			ps.Close()
		}
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

// handle validates and dispatches one command.
func (c *controller) handle(payload string) {
	var msg ControlMessage
	if err := json.Unmarshal([]byte(payload), &msg); err != nil {
		c.logger.Warn("Ignoring malformed control message: %v", err)
		return
	}
	if msg.ID == "" {
		msg.ID = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	if msg.Queue != "" && !c.serves(msg.Queue) {
		return // aimed at a queue this worker does not serve
	}
	c.logger.Info("Control command %s received (id=%s, queue=%q)", msg.Action, msg.ID, msg.Queue)

	switch msg.Action {
	case ActionDrain:
		c.drain(msg)
	case ActionResume:
		c.resume(msg)
	case ActionCodeUpdate, ActionWorkerUpdate, ActionConfigReload:
		c.mu.Lock()
		if c.busy != "" {
			busy := c.busy
			c.mu.Unlock()
			c.record(msg, "failed", fmt.Sprintf("busy: %s still running", busy))
			return
		}
		c.busy = msg.Action
		c.mu.Unlock()
		c.record(msg, "running", "")
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			defer func() {
				c.mu.Lock()
				c.busy = ""
				c.mu.Unlock()
			}()
			var err error
			var done string
			switch msg.Action {
			case ActionCodeUpdate:
				done, err = c.codeUpdate(msg)
			case ActionWorkerUpdate:
				done, err = c.workerUpdate(msg)
			case ActionConfigReload:
				done, err = c.configReload(msg)
			}
			if err != nil {
				c.logger.Error("Control command %s failed: %v", msg.Action, err)
				c.record(msg, "failed", err.Error())
				return
			}
			c.logger.Info("Control command %s done: %s", msg.Action, done)
			c.record(msg, "done", done)
		}()
	default:
		c.record(msg, "failed", "unknown action "+msg.Action)
	}
}

func (c *controller) serves(parent string) bool {
	for _, p := range c.parents() {
		if p == parent {
			return true
		}
	}
	return false
}

// targets returns the parent queues a command applies to.
func (c *controller) targets(msg ControlMessage) []string {
	if msg.Queue != "" {
		return []string{msg.Queue}
	}
	return c.parents()
}

func (c *controller) record(msg ControlMessage, state, message string) {
	c.mu.Lock()
	c.last = &controlResult{ID: msg.ID, Action: msg.Action, State: state, Message: message, At: time.Now().Unix()}
	c.mu.Unlock()
}

// addHeartbeatFields publishes the supported actions, the held queues, the last
// command and, while the whole worker is drained, overrides the status.
func (c *controller) addHeartbeatFields(data map[string]interface{}) {
	data["control_actions"] = strings.Join(controlActions, ",")
	p := c.w.processor
	held := []string{}
	if p != nil {
		held = p.heldParents()
	}
	data["held"] = strings.Join(held, ",")

	c.mu.Lock()
	last, draining := c.last, c.draining
	c.mu.Unlock()
	if last != nil {
		if b, err := json.Marshal(last); err == nil {
			data["control"] = string(b)
		}
	}
	if draining && p != nil {
		if p.activeFor() == 0 {
			data["status"] = "drained"
		} else {
			data["status"] = "draining"
		}
	}
}

func (c *controller) drain(msg ControlMessage) {
	parents := c.targets(msg)
	c.w.processor.hold(parents...)
	c.mu.Lock()
	if len(c.w.processor.heldParents()) >= len(c.parents()) {
		c.draining = true
	}
	c.mu.Unlock()
	c.record(msg, "done", fmt.Sprintf("draining %s (%d task(s) in flight)", strings.Join(parents, ", "), c.w.processor.activeFor(parents...)))
}

func (c *controller) resume(msg ControlMessage) {
	parents := c.targets(msg)
	c.w.processor.release(parents...)
	c.mu.Lock()
	c.draining = len(c.w.processor.heldParents()) >= len(c.parents()) && len(c.parents()) > 0
	c.mu.Unlock()
	c.record(msg, "done", "resumed "+strings.Join(parents, ", "))
}

// holdIdle holds the parents (keeping those already held) and waits until none has a
// task in flight. The returned release undoes only the holds it added.
func (c *controller) holdIdle(parents []string, timeout time.Duration) (release func(), err error) {
	already := map[string]bool{}
	for _, p := range c.w.processor.heldParents() {
		already[p] = true
	}
	var added []string
	for _, p := range parents {
		if !already[p] {
			added = append(added, p)
		}
	}
	c.w.processor.hold(added...)
	release = func() { c.w.processor.release(added...) }

	deadline := time.Now().Add(timeout)
	for c.w.processor.activeFor(parents...) > 0 {
		if time.Now().After(deadline) {
			release()
			return nil, fmt.Errorf("%d task(s) still running after %s", c.w.processor.activeFor(parents...), timeout)
		}
		select {
		case <-c.ctx.Done():
			release()
			return nil, fmt.Errorf("worker shutting down")
		case <-time.After(500 * time.Millisecond):
		}
	}
	return release, nil
}

func parseTimeout(s string, def time.Duration) (time.Duration, error) {
	if s == "" {
		return def, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("invalid timeout %q", s)
	}
	return d, nil
}

// codeUpdate re-fetches the queue's config from the server (branch, startup command,
// vaults, git token), updates the code and dependencies, and restarts the process. The
// queue is held while it has tasks in flight; other queues keep running.
func (c *controller) codeUpdate(msg ControlMessage) (string, error) {
	timeout, err := parseTimeout(msg.Timeout, defaultUpdateDrainTimeout)
	if err != nil {
		return "", err
	}
	parents := c.targets(msg)
	release, err := c.holdIdle(parents, timeout)
	if err != nil {
		return "", err
	}
	defer release()

	var done []string
	for _, parent := range parents {
		state := c.w.queueStates[parent]
		if state == nil {
			continue
		}
		rev, err := c.updateQueue(state)
		if err != nil {
			return strings.Join(done, "; "), fmt.Errorf("%s: %w", parent, err)
		}
		done = append(done, parent+" "+rev)
	}
	return "updated " + strings.Join(done, ", "), nil
}

func (c *controller) updateQueue(state *QueueState) (string, error) {
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Minute)
	defer cancel()

	cfg := c.w.config
	cfg.Queue = state.Name
	resp, err := doBootstrap(ctx, cfg, c.logger)
	if err != nil {
		return "", fmt.Errorf("cannot fetch the queue config: %w", err)
	}
	cfg.DeploymentDir = filepath.Join(c.w.config.DeploymentDir, state.Name)
	if resp.GitToken != "" {
		cfg.GitToken = resp.GitToken
	}
	deployment, err := updateCode(ctx, cfg, resp.Deployment, c.logger)
	if err != nil {
		return "", err
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	state.Response = resp
	state.Deployment = deployment

	if state.Mode == "one_shot" {
		state.OneShotHandler = NewOneShotHandler(deployment.CodePath, deployment.VenvPath, resp.Deployment.StartupCmd,
			resp.Vaults, resp.Deployment.StartupTimeoutSecs, resp.Deployment.RedisStorage, c.logger)
		return resp.Deployment.Branch, nil
	}

	// long_running: replace the supervised process (startup command and vaults may have
	// changed), keeping the stdio handler and reconnecting it to the new pipes.
	if state.Recovery != nil {
		state.Recovery.Stop()
		state.Recovery = nil
	}
	if state.Supervisor != nil {
		if err := state.Supervisor.Stop(); err != nil {
			c.logger.Warn("[%s] Error stopping the old process: %v", state.Name, err)
		}
	}
	sup := newProcessSupervisor(deployment, resp.Deployment, resp.Vaults, c.logger)
	if c.w.logBuffer != nil {
		sup.SetLogBuffer(c.w.logBuffer)
	}
	state.Supervisor = sup
	if err := sup.Start(ctx); err != nil {
		return "", fmt.Errorf("new process failed to start: %w", err)
	}
	state.StdioHandler.Reconnect(sup.Stdin(), sup.Stdout())
	if c.w.config.Recovery.Enabled {
		recovery := newProcessRecovery(state.Name, sup, c.w.config.Recovery, c.logger)
		recovery.SetOnRestart(func(s *ProcessSupervisor) {
			state.StdioHandler.Reconnect(s.Stdin(), s.Stdout())
		})
		recovery.Start()
		state.Recovery = recovery
	}
	return resp.Deployment.Branch + " (process restarted)", nil
}

// configReload asks each long-running process that supports it to reload its config.
func (c *controller) configReload(msg ControlMessage) (string, error) {
	timeout, err := parseTimeout(msg.Timeout, defaultReloadTimeout)
	if err != nil {
		return "", err
	}
	var done, skipped []string
	for _, parent := range c.targets(msg) {
		state := c.w.queueStates[parent]
		if state == nil {
			continue
		}
		state.mu.RLock()
		sup, stdio := state.Supervisor, state.StdioHandler
		state.mu.RUnlock()
		if sup == nil || stdio == nil {
			skipped = append(skipped, parent+" (one_shot: next task reads the new config)")
			continue
		}
		if !sup.HasCapability("reload") {
			skipped = append(skipped, parent+" (process has no @reload handler)")
			continue
		}
		ctx, cancel := context.WithTimeout(c.ctx, timeout)
		err := stdio.Control(ctx, "control-"+msg.ID, "reload")
		cancel()
		if err != nil {
			return "", fmt.Errorf("%s: reload failed: %w", parent, err)
		}
		done = append(done, parent)
	}
	if len(done) == 0 && len(skipped) > 0 {
		return "", fmt.Errorf("nothing reloaded: %s", strings.Join(skipped, "; "))
	}
	out := "reloaded " + strings.Join(done, ", ")
	if len(skipped) > 0 {
		out += "; skipped " + strings.Join(skipped, "; ")
	}
	return out, nil
}
