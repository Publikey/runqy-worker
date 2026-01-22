<p align="center">
  <img src="assets/logo.svg" alt="runqy logo" width="80" height="80">
</p>

<h1 align="center">runqy-worker</h1>

<p align="center">
  A Go worker binary for distributed task processing with server-driven bootstrap architecture.
  <br>
  Part of the <a href="https://github.com/publikey/runqy">runqy</a> distributed task queue system.
  <br>
  <a href="https://docs.runqy.com"><strong>Documentation</strong></a> · <a href="https://runqy.com"><strong>Website</strong></a>
</p>

## Overview

runqy-worker is a stateless task executor that:
- Receives all configuration (Redis credentials, code deployment specs, queue routing) from a central runqy-server at startup
- Clones and deploys Python task code from Git repositories
- Supervises Python processes and communicates via stdin/stdout JSON protocol
- Processes tasks from Redis queues with priority-based scheduling
- Maintains heartbeat and health status in Redis

**Architecture:** One worker = One or more queues = One supervised Python process per queue

## Features

- **Server-driven bootstrap** - Workers are stateless; all configuration comes from the central server
- **Automatic code deployment** - Git clone, virtualenv creation, dependency installation
- **Two execution modes:**
  - `long_running` - Single supervised process handles multiple tasks
  - `one_shot` - Fresh process spawned per task
- **Multi-queue support** - Process tasks from multiple queues with priority weighting
- **Sub-queue routing** - Support for `parent:child` queue naming (e.g., `inference:high`, `inference:low`)
- **Health monitoring** - Heartbeat and process health tracked in Redis
- **asynq-compatible** - Uses the same Redis key format as [hibiken/asynq](https://github.com/hibiken/asynq)
- **Graceful shutdown** - Proper cleanup on SIGTERM/SIGINT

## Installation

### From Source

```bash
git clone https://github.com/publikey/runqy-worker.git
cd runqy-worker
go build ./cmd/worker
```

### Build with Version Info

```bash
go build -ldflags "-X main.Version=1.0.0 -X main.Commit=$(git rev-parse HEAD) -X main.BuildDate=$(date -u +%Y-%m-%dT%H:%M:%SZ)" ./cmd/worker
```

## Quick Start

1. **Create configuration file** (`config.yml`):

```yaml
server:
  url: "http://localhost:8081"
  api_key: "your-api-key"

worker:
  queue: "inference"      # Single queue
  # queues:               # Or multiple queues
  #   - inference
  #   - simple
  concurrency: 1
  shutdown_timeout: 30s

bootstrap:
  retries: 3
  retry_delay: "5s"

deployment:
  dir: "./deployment"

retry:
  max_retry: 2
```

2. **Run the worker**:

```bash
./worker -config config.yml
```

3. **Verify configuration** (without starting):

```bash
./worker -config config.yml -validate
```

4. **Check version**:

```bash
./worker -version
```

## Configuration Reference

| Section | Field | Description | Default |
|---------|-------|-------------|---------|
| `server.url` | string | runqy-server URL (required) | - |
| `server.api_key` | string | Authentication key (required) | - |
| `worker.queue` | string | Single queue name | - |
| `worker.queues` | []string | Multiple queue names | - |
| `worker.concurrency` | int | Max parallel tasks | `1` |
| `worker.shutdown_timeout` | duration | Graceful shutdown timeout | `8s` |
| `bootstrap.retries` | int | Server contact retry attempts | `3` |
| `bootstrap.retry_delay` | duration | Delay between retries | `5s` |
| `deployment.dir` | string | Code deployment directory | `./deployment` |
| `git.ssh_key` | string | Path to SSH private key | - |
| `git.token` | string | Git access token | - |
| `retry.max_retry` | int | Max task retry attempts | `25` |

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                runqy-worker (Go)                                │
│                                                                                 │
│  Bootstrap Phase:                                                               │
│    1. POST /worker/register → receive Redis creds + deployment config           │
│    2. git clone repository → deployment directory                               │
│    3. Create virtualenv, pip install dependencies                               │
│    4. Spawn Python process (startup_cmd)                                        │
│    5. Wait for {"status": "ready"} on stdout                                    │
│                                                                                 │
│  Run Phase:                                                                     │
│    ┌──────────────┐    dequeue    ┌───────────────────────────────────────┐     │
│    │    Redis     │ ───────────→  │            Processor                  │     │
│    │              │               │  - Priority-based queue selection     │     │
│    │ - Pending    │  ←─────────── │  - Concurrent task execution          │     │
│    │ - Active     │    complete   │  - Health check per queue             │     │
│    │ - Results    │               └───────────────────────────────────────┘     │
│    └──────────────┘                          │                                  │
│                                              │ stdin/stdout JSON                │
│                                              ↓                                  │
│                                ┌───────────────────────────────────────┐        │
│                                │      Supervised Python Process        │        │
│                                │  - Long-running or one-shot mode      │        │
│                                │  - Handles @task decorated functions  │        │
│                                └───────────────────────────────────────┘        │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Bootstrap Protocol

### Worker Registration Request

```http
POST {server_url}/worker/register
Authorization: Bearer {api_key}
Content-Type: application/json

{
  "queue": "inference",
  "hostname": "worker-node-01",
  "os": "linux",
  "version": "1.0.0"
}
```

### Server Response

```json
{
  "redis": {
    "addr": "redis.example.com:6379",
    "password": "secret",
    "db": 0,
    "use_tls": true
  },
  "queue": {
    "name": "inference",
    "priority": 6
  },
  "sub_queues": [
    {"name": "inference:high", "priority": 9},
    {"name": "inference:low", "priority": 3}
  ],
  "deployment": {
    "git_url": "https://github.com/example/inference-service.git",
    "branch": "main",
    "code_path": "src/tasks",
    "startup_cmd": "python main.py",
    "env_vars": {"MODEL_PATH": "/models/gpt"},
    "startup_timeout_secs": 300,
    "mode": "long_running"
  }
}
```

## Task Protocol (stdin/stdout JSON)

### Task Input (sent to Python via stdin)

```json
{
  "task_id": "abc123",
  "type": "inference:predict",
  "payload": {"input": "Hello world"},
  "retry_count": 0,
  "max_retry": 5,
  "queue": "inference:high"
}
```

### Task Response (received from Python via stdout)

```json
{
  "task_id": "abc123",
  "result": {"output": "Generated text..."},
  "error": null,
  "retry": false
}
```

### Error Response

```json
{
  "task_id": "abc123",
  "result": null,
  "error": "Model failed to generate output",
  "retry": true
}
```

## Redis Key Format

Compatible with [asynq](https://github.com/hibiken/asynq) key format:

| Key | Type | Purpose |
|-----|------|---------|
| `asynq:{queue}:pending` | List | Pending task IDs |
| `asynq:{queue}:active` | Sorted Set | Currently processing tasks |
| `asynq:t:{task_id}` | Hash | Task data (type, payload, retry, etc.) |
| `asynq:result:{task_id}` | String | Task result JSON |
| `asynq:workers` | Set | Active worker IDs |
| `asynq:workers:{worker_id}` | Hash | Worker metadata (includes `healthy` field) |

## Execution Modes

### Long-Running Mode (default)

- Single Python process starts at bootstrap
- Process stays alive handling multiple tasks
- Tasks sent via stdin, responses received via stdout
- Best for: ML inference, stateful services, tasks requiring warm-up

### One-Shot Mode

- Fresh Python process spawned per task
- Process exits after completing one task
- Best for: Isolated execution, memory-intensive tasks, untrusted code

Configure via server response:
```json
{
  "deployment": {
    "mode": "one_shot"
  }
}
```

## Health Monitoring

Workers report health status via Redis heartbeat:

```bash
redis-cli HGETALL asynq:workers:{worker_id}
```

Fields include:
- `started` - Worker start timestamp
- `queues` - Assigned queues
- `concurrency` - Worker concurrency
- `healthy` - Boolean health status

When a supervised Python process crashes:
1. Worker detects crash via process monitor
2. All in-flight tasks fail immediately
3. Worker marks itself as `healthy: false`
4. Worker stops accepting new tasks
5. Manual restart required

## Library Usage

```go
package main

import (
    "context"
    "log"

    worker "github.com/publikey/runqy-worker"
)

func main() {
    // Load configuration
    cfg, err := worker.LoadConfig("config.yml")
    if err != nil {
        log.Fatal(err)
    }

    // Create worker
    w := worker.New(*cfg)

    // Bootstrap: contact server, deploy code, start process
    ctx := context.Background()
    if err := w.Bootstrap(ctx); err != nil {
        log.Fatal(err)
    }

    // Run: process tasks (blocks until shutdown)
    if err := w.Run(); err != nil {
        log.Fatal(err)
    }
}
```

## Project Structure

```
runqy-worker/
├── cmd/worker/
│   ├── main.go           # CLI entry point
│   └── version.go        # Version info (set via ldflags)
├── internal/
│   ├── bootstrap/
│   │   ├── server_client.go        # Server registration
│   │   ├── code_deploy.go          # Git clone, virtualenv setup
│   │   └── process_supervisor.go   # Python process lifecycle
│   ├── handler/
│   │   ├── handler.go              # Handler interface
│   │   ├── handler_factory.go      # Handler creation from config
│   │   ├── stdio_handler.go        # Stdin/stdout JSON communication
│   │   └── oneshot_handler.go      # One-shot process handler
│   └── redis/
│       └── redis.go                # Redis client wrapper
├── worker.go             # Main Worker struct, Bootstrap(), Run()
├── processor.go          # Task dequeue loop, priority scheduling
├── task.go               # Task struct with payload, result writer
├── handler.go            # HandlerFunc, ServeMux, middleware
├── redis.go              # Redis operations (dequeue, complete, retry)
├── heartbeat.go          # Worker health monitoring
├── config.go             # Configuration struct and defaults
├── config_yaml.go        # YAML configuration parsing
├── retry.go              # Retry delay functions
├── logger.go             # Logger interface
└── errors.go             # Error types (SkipRetry, etc.)
```

## Development

### Prerequisites

- Go 1.21+
- Redis (for integration tests)

### Build

```bash
go build ./cmd/worker
```

### Test

```bash
go test ./...
```

### Run with Verbose Logging

```bash
./worker -config config.yml 2>&1 | tee worker.log
```

## Dependencies

- [github.com/redis/go-redis/v9](https://github.com/redis/go-redis) - Redis client
- [gopkg.in/yaml.v3](https://github.com/go-yaml/yaml) - YAML parsing

## Related Projects

- [runqy](https://github.com/publikey/runqy) - Central configuration server with CLI and monitoring
- [runqy-python](https://github.com/publikey/runqy-python) - Python SDK (`runqy-task`)
- [Documentation](https://docs.runqy.com) - Full documentation at docs.runqy.com

## Contributing

Contributions are welcome. Please:

1. Fork the repository
2. Create a feature branch
3. Write tests for new functionality
4. Ensure all tests pass (`go test ./...`)
5. Submit a pull request

## License

MIT License - See [LICENSE](LICENSE) for details.
