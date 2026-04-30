<div align="center">

# 🚰 sluice

**代码即配置的信息流水线 —— 用 RSS、LLM 和 Notion 写就的"代码版 n8n"。**

[![PyPI version](https://img.shields.io/pypi/v/sluice-pipeline.svg?color=blue)](https://pypi.org/project/sluice-pipeline/)
[![Python](https://img.shields.io/pypi/pyversions/sluice-pipeline.svg?color=blue)](https://pypi.org/project/sluice-pipeline/)
[![License](https://img.shields.io/github/license/nerdneilsfield/sluice-pipeline.svg)](https://github.com/nerdneilsfield/sluice-pipeline/blob/master/LICENSE)
[![CI](https://img.shields.io/github/actions/workflow/status/nerdneilsfield/sluice-pipeline/ci.yml?branch=master&label=CI)](https://github.com/nerdneilsfield/sluice-pipeline/actions)
[![Coverage](https://img.shields.io/badge/coverage-81%25-brightgreen.svg)](https://github.com/nerdneilsfield/sluice-pipeline)
[![Tests](https://img.shields.io/badge/tests-303%20passing-brightgreen.svg)](https://github.com/nerdneilsfield/sluice-pipeline/actions)
[![Stars](https://img.shields.io/github/stars/nerdneilsfield/sluice-pipeline.svg?style=social)](https://github.com/nerdneilsfield/sluice-pipeline)

[**English**](./README.md) · [**简体中文**](./README_ZH.md) · [PyPI](https://pypi.org/project/sluice-pipeline/) · [GitHub](https://github.com/nerdneilsfield/sluice-pipeline)

</div>

> **slu·ice** /sluːs/ — *水闸：控制水流通过的闸门。*

这个项目做的就是这件事，只不过流过去的不是水，是**信息**：
你来决定让哪些源头流进来、什么节奏开闸、走哪些处理通道、最后蓄到哪个水库里。

```text
            ┌──────────┐    ┌─────────┐    ┌──────────┐    ┌──────────┐
RSS ───▶── │   源头   │──▶│  阶段   │──▶│  渲染    │──▶│  下游    │──▶ Notion
            └──────────┘    └─────────┘    └──────────┘    └──────────┘
                          去重 / 抓取 /              本地 md / 邮件
                          总结 / 过滤 /              （可插拔）
                          分析（LLM）
```

---

## 目录

- [为什么用 sluice？](#为什么用-sluice)
- [快速上手](#快速上手)
- [核心概念](#核心概念)
- [配置详解](#配置详解)
- [内置插件](#内置插件)
- [运维与可观测性](#运维与可观测性)
- [路线图](#路线图)
- [本地开发](#本地开发)
- [License](#license)

---

## V1.1 新特性

- **推送渠道 Sink**：Telegram（MarkdownV2）、飞书（post/text/interactive）、邮件（fail_fast/best_effort）——均带 `sink_delivery_log` 审计日志。
- **附件镜像**：`mirror_attachments` 阶段将图片/文件下载到本地磁盘，支持 `file://`、`https://` 或相对 URL 前缀。
- **Enricher 协议 + hn_comments**：可插拔的富化器为条目补充外部数据（通过 hckrnws.com 抓取 HN 评论线程）。
- **子日流水线**：`run_key_template` 支持 `{run_hour}`、`{run_minute}`、`{run_iso}`、`{run_epoch}`，适用于小于 24 小时的 cron 间隔。
- **`limit` 阶段**：`sort_by` / `group_by` / `per_group_max` 限制输出数量。
- **`field_filter` 操作**：新增 `lower`、`strip`、`regex_replace`，与已有的 `truncate` / `drop` 并列。
- **抓取器回退**：`on_all_failed = "use_raw_summary"` 在所有抓取器失败时回退到 RSS 原始摘要。
- **URL 缓存容量上限**：可配置 `max_rows`，LRU 式淘汰。
- **GC 命令**：`sluice gc` 回收 `failed_items`、`url_cache`、`attachment_mirror` 的空间 + 孤立文件清理。
- **指标监控**：自定义 Prometheus 采集器 + `sluice stats` + `sluice metrics-server` + `sluice deliveries` 审计查看器。
- **懒加载注册**：插件通过懒加载桩注册，`pip install sluice`（不带 extras）仍然可用。

---

## 为什么用 sluice？

你订阅了一堆 RSS。你想要**每天一份 Notion 上的摘要日报** —— 文章自动抓全文、
LLM 总结每条、汇总写一段综述、推到 Notion 数据库。又想便宜、可观测、**完全自己掌控**。

常见的三种方案：

|                      | n8n / Zapier               | 200 行的 Python 脚本    | **sluice**                                            |
| -------------------- | -------------------------- | ----------------------- | ----------------------------------------------------- |
| 加一个新 feed        | 点 12 下                   | 改代码                  | TOML 加 3 行                                          |
| 换 LLM 厂商          | 看集成是否有                | 看你写的时候有没有抽象  | 改 `providers.toml` 重启                              |
| 单次成本封顶         | 难                          | 自己造                  | 一行 `max_estimated_cost_usd`                         |
| 失败处理             | "整个 workflow 重跑"       | `try: ... except: pass` | 单 item 级 failed_items 生命周期、死信、`--retry`     |
| 自托管可观测         | n8n web UI（重）            | `print` + grep          | Rich 进度、loguru 诊断、Prefect run history            |
| LLM 降级链           | 手画分支                   | 没有                    | 4 层模型链 + 加权路由 + key 冷却                       |
| 幂等                 | "之前跑过没？"             | "脚本中途挂了没？"      | `sink_emissions` 表，重试走 upsert                    |

sluice 是 **n8n 的代码版**：所有业务逻辑都在普通 Python 和 TOML 里。
不依赖 SaaS、没有 GUI 锁定、没有不透明的 webhook。

---

## 快速上手

### 1. 安装

```bash
pip install sluice
```

> 需要 Python 3.11+。准备一个
> [Notion 集成 token](https://developers.notion.com/docs/create-a-notion-integration)
> 和至少一个 OpenAI 兼容的 LLM key（DeepSeek、智谱 GLM、OpenAI、OpenRouter 都行）。

### 2. 项目结构

```
my-digest/
├── configs/
│   ├── sluice.toml           # 全局：state、fetcher chain、运行时
│   ├── providers.toml        # LLM provider 池
│   └── pipelines/
│       └── ai_news.toml      # 一个 pipeline 一个文件
├── prompts/
│   ├── summarize_zh.md       # Jinja2 prompt
│   └── daily_brief_zh.md
└── templates/
    └── daily.md.j2           # 渲染模板
```

### 3. 一个最小 pipeline

```toml
# configs/pipelines/ai_news.toml
id = "ai_news"
window = "24h"
timezone = "Asia/Shanghai"

[[sources]]
type = "rss"
url  = "https://openai.com/blog/rss"

[[stages]]
name = "dedupe"
type = "dedupe"

[[stages]]
name = "summarize"
type = "llm_stage"
mode = "per_item"
input_field  = "raw_summary"
output_field = "summary"
prompt_file  = "prompts/summarize_zh.md"
model        = "openai/gpt-4o-mini"

[[stages]]
name = "render"
type = "render"
template = "templates/daily.md.j2"
output_field = "context.markdown"

[[sinks]]
id   = "notion_main"
type = "notion"
input          = "context.markdown"
parent_id      = "env:NOTION_DB_AI_NEWS"
parent_type    = "database"
token          = "env:NOTION_TOKEN"
title_template = "AI 日报 · {run_date}"
```

### 4. 跑起来

```bash
# Dry-run：不写 DB、不发 sink，只看会发生什么
sluice run ai_news --dry-run

# 真跑
sluice run ai_news

# 带详细诊断
sluice run ai_news --verbose --log-file logs/ai_news.jsonl

# 调度上线（注册成 Prefect deployment + cron）
sluice deploy
```

完事 —— **30 行 TOML 一份完整的每日摘要 pipeline**。

---

## 核心概念

水闸的隐喻直接映射到代码里。五个插件 **Protocol** —— 每个 pipeline 都是它们的组合：

| Protocol      | 做什么                             | 内置实现                                                                    |
| ------------- | ---------------------------------- | --------------------------------------------------------------------------- |
| `Source`      | 把 item 引入水流                   | `rss`                                                                       |
| `Fetcher`     | 把文章 URL → markdown              | `trafilatura`、`firecrawl`、`jina_reader`                                   |
| `Processor`   | 处理水流                           | `dedupe`、`fetcher_apply`、`filter`、`field_filter`、`llm_stage`、`render`、`limit`、`enrich`、`mirror_attachments`  |
| `Sink`        | 把 item 推到下游                   | `file_md`、`notion`、`telegram`、`feishu`、`email`                                  |
| `LLMProvider` | 跟 OpenAI 兼容端点对话             | 加权 base/key 池 + 4 层降级链                                              |

> 每个插件用一个装饰器自注册。要加一个新源（IMAP、Telegram、Reddit 等等），
> **写一个 Python 文件 + TOML 加一行**就完事。

### Item 数据模型

Item 沿 stages 流动。每条带着 provenance、内容、上游 stage 写入的任意 extras：

```python
@dataclass
class Item:
    source_id: str
    pipeline_id: str
    guid: str | None
    url: str                            # 经过规范化（utm_*/fbclid/... 都剥掉）
    title: str
    published_at: datetime | None
    raw_summary: str | None
    fulltext: str | None                # fetcher_apply 写入
    attachments: list[Attachment]       # RSS enclosure
    summary: str | None                 # summarize stage 写入
    extras: dict[str, Any]              # 其他 stage 写的任意字段
    tags: list[str]

    def get(self, path: str, default=None):
        """点路径：'fulltext'、'extras.relevance'、'tags.0'"""
```

### LLM provider 池 —— 真正的*杀手锏*

这就是 sluice 跟 200 行脚本的真正分水岭。配置一个 provider 池、加权 base URL、
加权 API key、自动配额冷却，**每个 stage 一条 4 层降级链**：

<details>
<summary><b>展开完整 provider 配置</b></summary>

```toml
# configs/providers.toml
[[providers]]
name = "openrouter"
type = "openai_compatible"

[[providers.base]]
url    = "https://openrouter.ai/api/v1"
weight = 3

key = [
  { value = "env:OR_KEY_1", weight = 2 },
  { value = "env:OR_KEY_2", weight = 1, quota_duration = 18000,
    quota_error_tokens = ["exceed", "quota"] },
]
active_windows  = ["00:00-08:00"]   # 仅在低峰时段用这个 base
active_timezone = "Asia/Shanghai"

[[providers.models]]
model_name          = "openai/gpt-4o-mini"
input_price_per_1k  = 0.00015
output_price_per_1k = 0.0006

[[providers]]
name = "ollama"
type = "openai_compatible"
[[providers.base]]
url = "http://localhost:11434/v1"
key = [{ value = "local" }]
[[providers.models]]
model_name = "llama3"
# 本地免费，不用配价格
```

然后在任意 `llm_stage`：

```toml
model            = "openrouter/openai/gpt-4o-mini"
retry_model      = "openrouter/openai/gpt-4o-mini"   # 同档，瞬时错误重试
fallback_model   = "openrouter/google/gemini-flash"   # 便宜的 backup
fallback_model_2 = "ollama/llama3"                    # 兜底本地
```

链按 主 → 重试 → fallback → fallback_2 顺序走。每档独立的 worker/concurrency。
配额耗尽的 key 自动冷却。时间窗路由让你把贵的请求推到低峰。

</details>

---

## 配置详解

sluice 有三层 TOML：

1. **`sluice.toml`** —— state DB、运行时、默认 fetcher chain
2. **`providers.toml`** —— LLM provider 池
3. **`pipelines/<id>.toml`** —— 一个 pipeline 一个文件

<details>
<summary><b>展开一个生产级 pipeline 完整 TOML</b></summary>

```toml
id = "ai_news"
description = "每日 AI / infra 新闻摘要"
enabled = true
cron = "0 8 * * *"
timezone = "Asia/Shanghai"
window = "24h"
lookback_overlap = "4h"

# ─── 背压 / 成本封顶 ─────────────────────────────────
[limits]
max_items_per_run    = 50          # 按 Notion upsert 延迟调过的值
item_overflow_policy = "drop_oldest"
max_llm_calls_per_run     = 500
max_estimated_cost_usd    = 5.0    # 超预算前直接 fail-fast

# ─── 失败 item 生命周期 ──────────────────────────────
[failures]
retry_failed  = true
max_retries   = 3                  # 超过进 dead_letter
retry_backoff = "next_run"

# ─── Sources（可多个）────────────────────────────────
[[sources]]
type = "rss"
url  = "https://openai.com/blog/rss"
tag  = "ai"

[[sources]]
type = "rss"
url  = "https://www.anthropic.com/news/rss.xml"
tag  = "ai"

# ─── Stages（按声明顺序执行）─────────────────────────
[[stages]]
name = "dedupe"
type = "dedupe"

[[stages]]
name = "fetch_fulltext"
type = "fetcher_apply"
write_field = "fulltext"
skip_if_field_longer_than = 2000   # feed 自带的内容够长就直接用

[[stages]]
name = "prefilter"
type = "filter"
mode = "keep_if_all"
rules = [
  { field = "fulltext", op = "min_length", value = 300 },
  { field = "title",    op = "not_matches", value = "(?i)广告|赞助" },
]

[[stages]]
name = "summarize"
type = "llm_stage"
mode = "per_item"
input_field    = "fulltext"
output_field   = "summary"
prompt_file    = "prompts/summarize_zh.md"
model          = "openrouter/openai/gpt-4o-mini"
fallback_model = "ollama/llama3"
workers        = 8
max_input_chars   = 20000
truncate_strategy = "head_tail"

[[stages]]
name = "daily_analysis"
type = "llm_stage"
mode = "aggregate"
input_field   = "summary"
output_target = "context.daily_brief"
prompt_file   = "prompts/daily_brief_zh.md"
model         = "openrouter/openai/gpt-4o"

[[stages]]
name = "render"
type = "render"
template     = "templates/daily.md.j2"
output_field = "context.markdown"

# ─── Sinks（可多个，靠 ID 保证幂等）─────────────────
[[sinks]]
id    = "local_archive"
type  = "file_md"
input = "context.markdown"
path  = "./out/ai_news/{run_date}.md"

[[sinks]]
id             = "notion_main"
type           = "notion"
input          = "context.markdown"
parent_id      = "env:NOTION_DB_AI_NEWS"
parent_type    = "database"
token          = "env:NOTION_TOKEN"
title_template = "AI 日报 · {run_date}"
properties     = { Tag = "AI", Source = "sluice" }
mode           = "upsert"          # upsert | create_once | create_new
```

</details>

---

## 内置插件

<details>
<summary><b>📥 Sources（输入源）</b></summary>

| `type` | 说明                              |
| ------ | --------------------------------- |
| `rss`  | 标准 RSS/Atom，走 feedparser。URL 自动规范化（剥 UTM/fbclid/…）、enclosure 抽到 `Item.attachments`、未来日期的 item 自动丢弃。 |

> **v2 计划：** IMAP、Telegram、Reddit、自定义 webhook。

</details>

<details>
<summary><b>🌐 Fetchers（全文抽取链）</b></summary>

| `type`         | 适用场景                                                  |
| -------------- | --------------------------------------------------------- |
| `trafilatura`  | 纯 Python，没额外服务依赖，快。默认首选。                  |
| `firecrawl`    | 自托管 Firecrawl，处理 JS 渲染页。                        |
| `jina_reader`  | 托管 Jina Reader，自托管挂掉时兜底。                       |

Fetcher chain 可全局或每 pipeline 配置。每个请求按链顺序走，带 `min_chars`
校验和可选磁盘缓存：

```toml
[fetcher]
chain         = ["trafilatura", "firecrawl", "jina_reader"]
min_chars     = 500
on_all_failed = "skip"             # 或 "continue_empty"

[fetcher.cache]
enabled = true
ttl     = "7d"
```

内置 SSRF 防护：阻止抓取请求打到内网/loopback IP，防止恶意 feed entry 探测内网。

如果你在 TUN/fake-IP 代理后面跑（比如 Clash/mihomo fake-ip 模式），公网域名
可能会被本地 DNS 解析成 `198.18.0.0/15`。普通环境保持默认严格模式；确认是代理
fake-ip 后再显式打开：

```bash
SLUICE_SSRF_ALLOW_TUN_FAKE_IP=1 sluice run ai_news
```

</details>

<details>
<summary><b>⚙️ Processors（九种 stage）</b></summary>

| `type`                  | 用途                                                                       |
| ----------------------- | -------------------------------------------------------------------------- |
| `dedupe`                | 丢掉本 pipeline `seen_items` 里已有的 item。                               |
| `fetcher_apply`         | 走 fetcher chain 给 `item.fulltext` 灌内容。                              |
| `filter`                | 规则过滤。14 种 op，含正则、长度、时间窗。带 ReDoS 防护。                  |
| `field_filter`          | 修改字段（truncate、drop、lower、strip、regex_replace）—— 比如喂贵的 LLM 之前把 fulltext 截到 20k。 |
| `llm_stage`             | LLM 调用，`per_item`（fan out）或 `aggregate`（一次跑全部）。支持 JSON 解析、最大输入截断、头尾保留、4 层降级链、成本预检。 |
| `render`                | Jinja2 模板 → markdown 写到 `context.<key>`。模板拿到固定 context（items、stats、run_date 等）。 |
| `limit`                 | 排序并截断输出。`sort_by`、`group_by`、`per_group_max`、`top_n`。         |
| `enrich`                | 可插拔的 enricher 协议，用外部数据增强 item（如 HN 评论）。                |
| `mirror_attachments`    | 把 item 的附件/extras 下载到本地磁盘；重写 URL。                           |

</details>

<details>
<summary><b>🎚 <code>filter</code> 操作符参考</b></summary>

`filter` 是便宜、确定、**不调 LLM** 的保留 / 丢弃 stage。每条规则
`{ field, op, value }`，`field` 是 `Item.get()` 解析的点路径
（`extras.relevance`、`tags.0`、`published_at` 都行），用四种 `mode`
之一组合。

**Mode（怎么组合）：**

| `mode`           | 满足下列条件就保留这条 item            |
| ---------------- | -------------------------------------- |
| `keep_if_all`    | 所有规则都命中（逻辑 AND）             |
| `keep_if_any`    | 至少一条命中（逻辑 OR）                |
| `drop_if_all`    | 不是所有规则都命中                     |
| `drop_if_any`    | 一条也不命中                           |

**操作符（共 17 个）：**

| 类别        | `op`            | 检查什么                                                              |
| ----------- | --------------- | --------------------------------------------------------------------- |
| 存在性      | `exists`        | 字段存在且非 None                                                     |
|             | `not_exists`    | 字段为 None 或不存在                                                  |
| 数值        | `gt` / `gte`    | `field > value` / `field >= value`                                    |
|             | `lt` / `lte`    | `field < value` / `field <= value`                                    |
|             | `eq`            | `field == value`                                                      |
| 字符串      | `matches`       | 正则搜索（带 ReDoS 防护）                                             |
|             | `not_matches`   | `matches` 取反                                                        |
|             | `contains`      | 子串 / 元素是否在 field 里                                            |
|             | `not_contains`  | `contains` 取反                                                       |
| 集合        | `in`            | field 值在给定列表里                                                  |
|             | `not_in`        | `in` 取反                                                             |
| 长度        | `min_length`    | `len(field) >= value`                                                 |
|             | `max_length`    | `len(field) <= value`                                                 |
| 时间        | `newer_than`    | `field`（datetime）比 `now - value` 新（如 `"24h"`、`"7d"`）          |
|             | `older_than`    | `field` 比 `now - value` 旧                                          |

**实战示例：**

```toml
# 1. 喂 LLM 之前的便宜预过滤 —— 留下足够长的文章 + 砍标题里的广告
[[stages]]
name = "prefilter"
type = "filter"
mode = "keep_if_all"
rules = [
  { field = "fulltext", op = "min_length", value = 300 },
  { field = "title",    op = "not_matches", value = "(?i)广告|赞助|sponsored|advertisement" },
  { field = "published_at", op = "newer_than", value = "48h" },
]

# 2. LLM 打分 + 规则过滤组合 —— 上游 llm_stage 给相关度 0-10，
#    `filter` 砍掉低于 6 分的。
[[stages]]
name = "rate_relevance"
type = "llm_stage"
mode = "per_item"
input_field    = "summary"
output_field   = "extras.relevance"
prompt_file    = "prompts/rate.md"
output_parser  = "json"
model          = "openrouter/openai/gpt-4o-mini"

[[stages]]
name = "drop_irrelevant"
type = "filter"
mode = "keep_if_all"
rules = [
  { field = "extras.relevance", op = "gte", value = 6 },
]

# 3. 标签白名单 + URL 黑名单组合
[[stages]]
name = "scope"
type = "filter"
mode = "keep_if_all"
rules = [
  { field = "tags",       op = "contains",     value = "ai" },
  { field = "url",        op = "not_matches",  value = "^https?://(twitter|x)\\.com/" },
  { field = "source_id",  op = "not_in",       value = ["spammy_feed_1", "spammy_feed_2"] },
]
```

**为什么"规则过滤 + LLM 打分"比纯 LLM 过滤好：** 打分要花 LLM token，
所以让 `llm_stage` 算一次写到字段里，下游用便宜的规则随便组合。
同样的 prompt 钱，下游过滤器无限组合。

</details>

<details>
<summary><b>📤 Sinks（输出端）</b></summary>

| `type`      | 说明                                                                      |
| ----------- | ------------------------------------------------------------------------- |
| `file_md`   | 确定性的本地 markdown 文件。当审计存档很合适。                           |
| `notion`    | 包 [`notionify`](https://pypi.org/project/notionify/)：markdown → Notion 数据库的页面。 |
| `telegram`  | 通过 Bot API 推送消息到 Telegram 群/私聊。MarkdownV2 渲染、安全截断、超长自动分片。 |
| `feishu`    | 通过 webhook 推送消息到飞书/Lark 群。支持 `post`、`text`、`interactive`（Card V2）模式。 |
| `email`     | 通过 SMTP 发送 HTML 邮件。支持逐收件人发送、`fail_fast`/`best_effort` 策略。 |

**幂等模式：**

| `mode`        | 行为                                                                        |
| ------------- | --------------------------------------------------------------------------- |
| `upsert`      | 同一 `run_key` 重跑会更新已有页面（不重复创建）。                           |
| `create_once` | 第一次创建，之后 no-op。                                                    |
| `create_new`  | 永远新建（故意非幂等，用于"每次都发一份新页面"）。                          |

当 parent 是 database 时，`properties` 可以写友好的 TOML 简写，比如
`{ Tag = "AI", Source = "sluice" }`；sluice 会读取数据库 schema，把它们展开成
Notion API 需要的 `select`、`multi_select`、`rich_text`、`url`、`date` 等结构。
如果你已经写了完整 Notion property dict，则会原样透传。

</details>

---

## 运维与可观测性

<details>
<summary><b>🔧 CLI</b></summary>

```bash
sluice list                                          # 列出所有 pipeline 和它们的 cron
sluice validate                                      # 校验所有 TOML
sluice run <pipeline_id>                             # 跑一次
sluice run <pipeline_id> --dry-run                   # 不写 DB、不发 sink
sluice run <pipeline_id> --verbose                   # 打开 Sluice DEBUG 日志
sluice run <pipeline_id> --log-file logs/run.jsonl   # 写 DEBUG JSONL 诊断日志
sluice deploy                                        # 注册所有 enabled pipeline 到 Prefect
sluice failures <pipeline_id>                        # 列出 failed_items
sluice failures <pipeline_id> --retry <item_key>     # 把 dead_letter 重置回 failed
sluice gc                                            # 回收 failed_items/url_cache/attachment_mirror 的空间
sluice gc --dry-run                                  # 预览会删什么但不实际修改
sluice gc --older-than 90d --pipeline ai_news        # 指定过期时间和 pipeline 范围
sluice stats                                         # 显示 pipeline 运行统计（最近 7 天）
sluice stats ai_news --since 30d --format json       # 单 pipeline 统计，JSON 格式
sluice metrics-server --host 0.0.0.0 --port 9090    # 启动 Prometheus 指标暴露端点
sluice deliveries <pipeline_id>                      # 列出 sink 投递审计日志
sluice deliveries <pipeline_id> --run <run_key>      # 按特定 run 筛选投递记录
```

</details>

<details>
<summary><b>🪵 进度与日志</b></summary>

`sluice run` 执行时会显示 tqdm 进度条，结束后用 Rich 打一张 **Step
Summary** 表，直接列出每个 source、stage、sink 的输入输出：

```text
source  rss_0           -   2   total=2
stage   fetch_fulltext  22  3   fetched=3 failed=19 AllFetchersFailed=19
sink    local:file_md   3   emitted
```

控制台默认是 INFO。加 `--verbose` 后会显示 Sluice 自己的 DEBUG 事件：
单个 fetcher 尝试、cache hit、页面太短、LLM 可重试失败等。第三方库内部日志
（`aiosqlite`、`httpx`、`httpcore`、`feedparser`、`trafilatura`、`prefect`）
统一压到 WARNING，避免 verbose 模式被连接细节刷屏。

需要完整回放时，用 `--log-file` 或 `SLUICE_LOG_FILE` 写 DEBUG JSONL：

```bash
sluice run ai_news --log-file logs/ai_news.jsonl
SLUICE_LOG_FILE=logs/ai_news.jsonl sluice run ai_news --verbose
```

</details>

<details>
<summary><b>📊 持久化的状态（SQLite）</b></summary>

8 张表，无 ORM，靠 `PRAGMA user_version` 做迁移：

| 表                     | 做什么                                                          |
| ---------------------- | --------------------------------------------------------------- |
| `seen_items`           | 每 pipeline 的去重表。带 summary，将来给 RAG 用。              |
| `failed_items`         | 单 item 失败记录，带完整 payload、状态（`failed`/`dead_letter`/`resolved`）、重试次数。 |
| `sink_emissions`       | `(pipeline_id, run_key, sink_id)` → external_id，幂等重试的核心。 |
| `url_cache`            | 文章抽取缓存，带 TTL。重试时不用再敲 Firecrawl。               |
| `run_log`              | 每次 run 的元信息：入/出 item 数、LLM 调用数、估算成本、状态、错误。 |
| `sink_delivery_log`    | 逐消息 push-sink 审计：ordinal、kind、recipient、external_id、status、error。 |
| `attachment_mirror`    | 镜像附件元信息：原始 URL、本地路径、mime type、大小。           |
| `gc_log`               | GC 运行历史：时间戳、清理的表、影响的行数。                     |

</details>

<details>
<summary><b>🔁 失败处理生命周期</b></summary>

```
RSS item ──▶ stage X 失败 ──▶ failed_items（status=failed，存 item_json）
                                          │
                                  下次 run 启动
                                          │
                          ┌───────────────┴──────────────┐
                          ▼                              ▼
                先于 processors 重新入队        成功 → status=resolved
                          │
                  又失败 ──▶ attempts++ ──▶ 达到 max_retries → dead_letter
                                                       │
                                       sluice failures --retry → 回到 failed
```

Item 从 `item_json` 还原，所以重试不依赖 RSS feed 是否还吐出这条。

</details>

<details>
<summary><b>📅 用 Prefect 调度</b></summary>

`sluice deploy` 会把每个 enabled pipeline 注册成一个带 cron 的 Prefect
deployment。Prefect UI 提供按 pipeline 看 run history、重跑、按 task
看可观测性 —— 基本就是你想要的 n8n web UI。

```bash
prefect server start                # 一个终端
sluice deploy                       # 注册 cron deployment
prefect worker start --pool default # 处理调度任务
```

</details>

---

## 路线图

✅ **v1（当前）** —— RSS source、Notion sink、file_md sink、6 种 processor、
4 层 LLM 降级、幂等重试、dry-run、loguru 诊断、Prefect 调度、SSRF 防护。

✅ **v1.1**

- [x] Push-channel sinks：Telegram、Feishu、Email
- [x] Attachment 镜像（`mirror_attachments` stage）
- [x] Enricher 协议 + `hn_comments`
- [x] 亚日级 pipeline（`run_key_template`）
- [x] `limit` stage
- [x] `field_filter` 操作：lower、strip、regex_replace
- [x] Fetcher 降级（`on_all_failed`）
- [x] URL cache 大小上限
- [x] GC 命令 + metrics + CLI 审计查看器
- [x] Lazy registry

🚧 **v1.2**

- 原生 Anthropic Messages API
- 各 tier 的 worker 配置
- Notion 页面 cover image
- 插件 entry-points（第三方插件自动发现）

🔮 **v2**

- IMAP / 邮箱 source
- GitHub repo sink（推 markdown → 触发 build）
- 历史摘要的 RAG（跨日报的语义搜索）

---

## 本地开发

```bash
git clone https://github.com/nerdneilsfield/sluice-pipeline
cd sluice-pipeline
uv sync --all-extras                # 或 pip install -e '.[dev]'
pytest                              # 303 个测试
pytest --cov=sluice                 # 81% 覆盖率
ruff check .
ty check .
```

架构和设计动机在 [`docs/superpowers/specs/`](./docs/superpowers/specs/)，
TDD 实现计划在 [`docs/superpowers/plans/`](./docs/superpowers/plans/)。

---

## License

MIT © [nerdneilsfield](https://github.com/nerdneilsfield)

<div align="center">

— *开闸放行，让对的水流过去* —

</div>
