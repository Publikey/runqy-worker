<p align="center">
  <img src="assets/logo.svg" alt="runqy logo" width="80" height="80">
</p>

<h1 align="center">runqy-worker</h1>

<p align="center">
  Stateless task processor with server-driven bootstrap.
  <br>
  Part of the <a href="https://github.com/publikey/runqy">runqy</a> distributed task queue system.
  <br>
  <a href="https://docs.runqy.com/worker/"><strong>Documentation</strong></a> · <a href="https://runqy.com"><strong>Website</strong></a>
</p>

## Features

- **Server-driven bootstrap** — Workers receive all config (Redis, code repo, queue routing) from the central server
- **Automatic code deployment** — Git clone, virtualenv creation, pip install on startup
- **Two execution modes** — Long-running process for ML inference, or one-shot per task
- **Multi-queue support** — Process tasks from multiple queues with priority weighting
- **Health monitoring** — Heartbeat and process health tracked in Redis

## Zero-Touch Deployment

Push code to GitHub. Workers deploy themselves. No SSH. No Docker builds. No CI pipelines to maintain.

<p align="center">
  <img src="assets/code_pull.png" alt="Zero-touch deployment flow" width="600">
</p>

## Installation

### Docker

```bash
docker pull ghcr.io/publikey/runqy-worker:latest
```

For GPU/ML workloads:
```bash
docker pull ghcr.io/publikey/runqy-worker:inference
```

### Binary Download

Download from [GitHub Releases](https://github.com/publikey/runqy-worker/releases):

```bash
curl -LO https://github.com/publikey/runqy-worker/releases/latest/download/runqy-worker_latest_linux_amd64.tar.gz
tar -xzf runqy-worker_latest_linux_amd64.tar.gz
```

See [Installation Guide](https://docs.runqy.com/worker/installation/) for all platforms and options.

## Quick Start

1. Create `config.yml`:

```yaml
server:
  url: "http://localhost:3000"
  api_key: "your-api-key"

worker:
  queue: "inference"
```

2. Run the worker:

```bash
./runqy-worker -config config.yml
```

The worker connects to the server, pulls your code from Git, installs dependencies, and starts processing tasks.

See [Configuration Reference](https://docs.runqy.com/worker/configuration/) for all options.

## See Also

- [runqy](https://github.com/Publikey/runqy) — Central server with CLI and monitoring dashboard
- [runqy-python](https://github.com/Publikey/runqy-python) — Python SDK (`runqy-task`)
- [Documentation](https://docs.runqy.com) — Full documentation

## License

MIT License — see [LICENSE](LICENSE) for details.
