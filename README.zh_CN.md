[English](README.md) | 简体中文

# Sluice v4

[![CI](https://github.com/nerdneilsfield/sluice-pipeline/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/nerdneilsfield/sluice-pipeline/actions/workflows/ci.yml)
[![Docker](https://github.com/nerdneilsfield/sluice-pipeline/actions/workflows/docker.yml/badge.svg)](https://github.com/nerdneilsfield/sluice-pipeline/actions/workflows/docker.yml)
[![Publish to PyPI](https://github.com/nerdneilsfield/sluice-pipeline/actions/workflows/publish.yml/badge.svg)](https://github.com/nerdneilsfield/sluice-pipeline/actions/workflows/publish.yml)
[![PyPI](https://img.shields.io/pypi/v/sluice-pipeline)](https://pypi.org/project/sluice-pipeline/)
[![Python 3.12+](https://img.shields.io/badge/python-%3E%3D3.12-blue)](pyproject.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Sluice 采集 RSS 和网页内容，运行由 YAML 定义的任务图，并通过各自独立重试的 Sink 投递结果。SQLite 保存发布记录、执行状态、共享的 Stage 结果和 Delivery 尝试记录。本地命令与可选的 Prefect flow 使用同一套运行时。

典型用途包括监控网页变化、把 RSS 条目转发到 Telegram、邮件或飞书，以及发送由 LLM 撰写的订阅摘要。每种用途都是一份 YAML 定义，用 `sluice` 命令运行。

当前分支是 v4 的 clean-room 实现。请使用独立的数据库，不要指向旧版 Sluice 部署的数据库。运行时使用 `fcntl` 文件锁，面向类 Unix 系统；同一时刻只有一个 App 能持有某个数据库。详见 [Operate Sluice](docs/operations.md)。

## 安装

Sluice 需要 Python 3.12 或更高版本。下面三种方式任选其一：使用 Sluice 选 pip，在容器中运行选 Docker，参与开发选源码安装。

### 使用 pip

推荐普通用户使用这种方式。发行包 `sluice-pipeline` 已发布到 [PyPI](https://pypi.org/project/sluice-pipeline/)，Python 导入名和命令名都是 `sluice`。按需选择 extra 后安装：

```sh
pip install 'sluice-pipeline[web,ui,prefect]'
sluice --help
```

不用的 extra 可以省略。邮件、Telegram 和 LLM 集成需要各自的配置和凭据；console 示例不会用到它们。

### 使用 Docker

已发布的镜像支持 `linux/amd64` 和 `linux/arm64`，可从 GitHub Container Registry（`ghcr.io/nerdneilsfield/sluice-pipeline`）和 Docker Hub（`nerdneils/sluice-pipeline`）获取，两处都有 `0.4.0`、`0.4`、`0` 和 `latest` 标签。拉取镜像并确认它能启动：

```sh
docker pull ghcr.io/nerdneilsfield/sluice-pipeline:latest
docker run --rm ghcr.io/nerdneilsfield/sluice-pipeline:latest --help
```

如果想从检出目录自行构建，请在仓库根目录运行下面的命令，并在后文把已发布的镜像名换成 `sluice-pipeline:local`：

```sh
docker build -f scripts/docker/Dockerfile -t sluice-pipeline:local .
docker run --rm sluice-pipeline:local --help
```

镜像以 `/app` 为工作目录，入口是 `sluice`，并包含 `web`、`ui` 和 `prefect` 这几个运行时 extra。`/app/configs` 和 `/app/data` 两个目录分别用于存放定义和状态。默认的 `database` 路径相对于 YAML 文件，所以如果以只读方式挂载定义，请在 YAML 中把 `database` 设为绝对路径，例如 `/app/data/state.db`：

```sh
docker run --rm \
  -v "$PWD/configs:/app/configs:ro" \
  -v "$PWD/data:/app/data" \
  ghcr.io/nerdneilsfield/sluice-pipeline:latest tick /app/configs/definition.yml \
  --at "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
```

请把 `configs/definition.yml` 换成你自己的定义。镜像没有设置 `USER`，容器会以 root 身份写入挂载的状态目录。如果定义使用自定义 Sink 或其他通过点分路径指定的可调用对象，对应的模块必须能在容器内导入，例如把它挂载到 `/app/configs` 下，并加上 `-e PYTHONPATH=/app/configs`。

### 从源码安装

这种方式用于开发，或运行仓库自带的示例。先安装 [uv](https://docs.astral.sh/uv/)，然后在仓库根目录运行：

```sh
uv sync --locked --extra dev --extra web --extra ui --extra prefect
```

该命令会把 `sluice` 安装为 `.venv/bin/sluice`。下文的示例都使用这个路径。

## 运行网页示例

从源码安装后，在仓库根目录运行：

```sh
PYTHONPATH="$PWD" .venv/bin/sluice tick examples/webpage.yml \
  --at "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
```

该示例抓取 `https://example.com/`，提取可见内容并转换为文本，然后打印一个包含 `delivery_key` 和 `payload` 的 JSON 对象。它不会发送邮件或 Telegram 消息，但需要访问互联网。`PYTHONPATH` 用于让示例的自定义 Sink `examples.console.send` 可被导入；只使用已安装内置组件的定义不需要它。

状态保存在 `examples/webpage.db` 中，路径相对于 YAML 文件。不依赖 CLI 退出状态来查看结果，可以这样做：

```sh
.venv/bin/python - <<'PY'
import sqlite3
with sqlite3.connect('examples/webpage.db') as db:
    print(db.execute('SELECT kind, state, error FROM executions').fetchall())
    print(db.execute('SELECT state FROM deliveries').fetchall())
PY
```

首次运行成功时，Source、Stage 和 Delivery 的状态都是 `succeeded`。记录了执行失败后，CLI 仍可能正常返回，所以排查问题时请检查这些状态。该 Source 的调度间隔为一小时，紧接着再执行一次 tick 时不必重新抓取。内容没有变化时，不会产生新的 delivery。

## Pipeline 概念

[examples/webpage.yml](examples/webpage.yml) 包含显式配置的 HTTPX provider、fetch chain、extractor chain、Source、Stage 和 console Delivery。YAML 中的 `version: 1` 是定义格式的版本，不是软件包的主版本号。

- `sources` 采集不可变的 Item 并发布变化。内置的有 `sluice.sources.rss`、`webpage`、`json_api`、`hacker_news`、`github_trending` 和 `github_activity`。内置 Sink 包括 Telegram、邮件、飞书、`sluice.sinks.webhook` 和 `sluice.sinks.file`。配置项与规则见 [Sources and Sinks 参考](docs/sources-and-sinks.md)（英文）。
- `stages` 运行 `each` 或 `batch` 类型的任务链。
- `pipelines` 通过 `from` 边把 Source 和 Stage 连接起来。每个 Pipeline 都需要一个 Delivery 端点。
- `web` 声明具名 provider 和有序的 fallback chain。HTTPX 与 BeautifulSoup 只需核心依赖；`web` extra 会额外安装 curl_cffi、trafilatura、newspaper4k 和 resiliparse。

为防止 SSRF，直连的 web provider 会拒绝私有地址和回环地址。托管服务的端点适用另一套策略。不要把凭据写进 YAML 的值里；provider 的凭据字段接受环境变量引用，例如 `api_key: env:FIRECRAWL_API_KEY`。

使用 Crawl4AI 时，请参阅 [0.9.x 配置指南](docs/crawl4ai.md)；不支持更早的 API 格式。

任务链与临时产物的规则、定义中的绑定标识，以及扩展契约（包括通过点分路径指定的可调用对象的要求），见 [Write a definition](docs/configuration.md)。

## 场景示例

**网页。** [examples/webpage.yml](examples/webpage.yml) 抓取一个页面，提取其中的文本，并通过 console Sink 打印。上文运行的就是这个示例。

**RSS 通知。** RSS 2.0、Atom 和 RSS 1.0 订阅源可以用内置的发送器投递到 Telegram、SMTP 邮件和飞书自定义机器人。参见[通知配置](docs/notifications.md)和[三通道 RSS 示例](examples/rss-notifications.yml)。该示例会发送真实消息，需要你提供凭据；上面的网页示例只会打印到 console。

**Hacker News 写入文件。** [examples/hn-to-files.yml](examples/hn-to-files.yml) 用 file Sink 为每条热门故事写一个文本文件，不需要凭据。首次运行最多输出 `limit` 条故事。参见 [Sources and Sinks 参考](docs/sources-and-sinks.md)（英文）。

**LLM 摘要。** [examples/rss-llm-digest.yml](examples/rss-llm-digest.yml) 依次运行 RSS → fetch → `llm_extract` → `llm_summary` → console，使用顶层 `llm` 块中配置的一个 OpenAI 兼容 `/chat/completions` 端点。参见 [LLM 摘要配置](docs/llm.md)。

## 常用命令

`sluice` 是一次性执行的运行器，不是后台调度器。执行类命令需要传入带时区的 ISO-8601 时间戳：

```sh
sluice tick definition.yml --at 2026-09-22T00:00:00Z
sluice reconcile definition.yml pipeline_name node_name --at 2026-09-22T00:00:00Z
sluice rebuild definition.yml 42 --at 2026-09-22T00:00:00Z
sluice retry-delivery definition.yml DELIVERY_KEY --at 2026-09-22T00:00:00Z
sluice ui definition.yml --host 127.0.0.1 --port 8000
```

请把 `definition.yml`、Pipeline/Node 名称、执行 ID `42` 和 `DELIVERY_KEY` 换成你部署中的实际值。如果 Sink 没有遵守稳定的 Delivery key，重试 Delivery 可能重复产生外部副作用。管理 UI 带有修改类操作，且没有认证；请只把它绑定在可信的本地接口上。

[Operate Sluice](docs/operations.md) 介绍如何用 cron 调度 `tick`，rebuild 和重试的语义，UI、Prefect、数据库锁，以及如何查看状态。

## 参与贡献

欢迎贡献。[CONTRIBUTING.md](CONTRIBUTING.md) 说明开发环境搭建、验证命令、CI 矩阵和发布流程。[v4 verification](docs/v4-verification.md) 记录了已检查的范围和仍存在的限制。
