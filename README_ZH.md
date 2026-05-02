<div align="center">

# 🚰 sluice

**代码即编排的信息流水线。用 TOML 把 RSS、LLM、Notion 串起来 —— 你可以理解为代码版的 n8n。**

[![PyPI version](https://img.shields.io/pypi/v/sluice-pipeline.svg?color=blue)](https://pypi.org/project/sluice-pipeline/)
[![Python](https://img.shields.io/pypi/pyversions/sluice-pipeline.svg?color=blue)](https://pypi.org/project/sluice-pipeline/)
[![License](https://img.shields.io/github/license/nerdneilsfield/sluice-pipeline.svg)](https://github.com/nerdneilsfield/sluice-pipeline/blob/master/LICENSE)
[![CI](https://img.shields.io/github/actions/workflow/status/nerdneilsfield/sluice-pipeline/ci.yml?branch=master&label=CI)](https://github.com/nerdneilsfield/sluice-pipeline/actions)
[![Coverage](https://img.shields.io/badge/coverage-80%25-brightgreen.svg)](https://github.com/nerdneilsfield/sluice-pipeline)
[![Tests](https://img.shields.io/badge/tests-408%20passing-brightgreen.svg)](https://github.com/nerdneilsfield/sluice-pipeline/actions)
[![Stars](https://img.shields.io/github/stars/nerdneilsfield/sluice-pipeline.svg?style=social)](https://github.com/nerdneilsfield/sluice-pipeline)

[**English**](./README.md) · [**简体中文**](./README_ZH.md) · [PyPI](https://pypi.org/project/sluice-pipeline/) · [GitHub](https://github.com/nerdneilsfield/sluice-pipeline)

</div>

> **slu·ice** /sluːs/ — *水闸。开关之间，定乾坤。*

这就是 sluice 的隐喻：你决定从哪些源头取水、闸门何时抬起、水流经过几道工序、
最终汇入哪座水库。只不过经过闸门的不是水，是**信息**，而每一道闸、每一条渠、
每一座库，都是你读得懂的代码。

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
- [Docker 部署](#docker-部署)
- [CI/CD](#cicd)
- [路线图](#路线图)
- [本地开发](#本地开发)
- [License](#license)

---

## V1.1 新特性

自己每天跑了好几个月的真实 feed 之后，这版修了不少东西：

- **多通道输出**：Telegram（MarkdownV2）、飞书（post/text/interactive）、邮件（fail_fast/best_effort）—— 均带 `sink_delivery_log` 审计。
- **附件镜像**：`mirror_attachments` 阶段将图片/文件下载到本地磁盘，支持 `file://`、`https://` 或相对 URL 前缀。
- **Enricher 协议 + hn_comments**：可插拔的富化器为条目注入外部数据（主要通过 HN Firebase API 获取评论线程，官方 HN 页面兜底）。该 stage **必须在 `summarize` 之前运行**，才能让 LLM 汇总时纳入社区讨论。
- **亚日级流水线**：`run_key_template` 支持 `{run_hour}`、`{run_minute}`、`{run_iso}`、`{run_epoch}`，适配小于 24 小时的 cron 间隔。
- **`limit` 阶段**：按 `sort_by` / `group_by` / `per_group_max` 排序分组并截断输出。
- **`field_filter` 操作**：新增 `lower`、`strip`、`regex_replace`，与已有的 `truncate` / `drop` 并列使用。
- **清洗、跨源去重和打分阶段**：新增 `cross_dedupe`、`html_strip`、`score_tag`，用于跨 feed 合并重复文章、清理 RSS 里的 HTML 字段，并在昂贵的总结前先打分和打标签。
- **智能抓取降级**：`on_all_failed = "use_raw_summary"`，全部抓取器失败时优雅回退到 RSS 摘要。
- **URL 缓存上限**：可配置 `max_rows`，LRU 淘汰 —— 数据库保持轻量。
- **GC 命令**：`sluice gc` 回收 `failed_items`、`url_cache`、`attachment_mirror` 的存储空间，并清理孤立文件。
- **可观测性**：自定义 Prometheus 采集器、`sluice stats`、`sluice metrics-server`、`sluice deliveries` 审计查看器。
- **懒加载注册**：插件通过桩注册 —— `pip install sluice-pipeline` 不带 extras 也能跑。

---

## 为什么用 sluice？

你关注了一堆 RSS。你想要**每天一份 Notion 上的摘要日报** —— 每条文章自动抓全文、
LLM 逐条总结、汇总成一篇简报、推到 Notion 数据库。要求便宜、可观测、**完全自己掌控**。

摆在你面前的大致三条路：

|                      | n8n / Zapier               | 200 行的 Python 脚本    | **sluice**                                            |
| -------------------- | -------------------------- | ----------------------- | ----------------------------------------------------- |
| 加一个新 feed        | 点 12 下                   | 改代码                  | TOML 加 3 行                                          |
| 换 LLM 厂商          | 看集成是否有                | 看你写的时候有没有抽象  | 改 `providers.toml` 重启                              |
| 单次成本封顶         | 难                          | 自己造                  | 一行 `max_estimated_cost_usd`                         |
| 失败处理             | "整个 workflow 重跑"       | `try: ... except: pass` | 单 item 级 failed_items 生命周期、死信、`--retry`     |
| 自托管可观测         | n8n web UI（重）             | `print` + grep          | Rich 进度条、loguru 诊断、Prefect 运行历史             |
| LLM 降级链           | 手画分支                   | 没有                    | 模型降级 + 长上下文路由 + key 冷却                     |
| 幂等                 | "之前跑过没？"             | "脚本中途挂了没？"      | `sink_emissions` 表，重试走 upsert                    |

sluice 就是 **代码版 n8n**：业务逻辑全在普通 Python 和 TOML 里
—— 不绑 SaaS、不锁 GUI、没有黑盒 webhook。

---

## 快速上手

### 1. 安装

```bash
# PyPI 包名是 sluice-pipeline（sluice 已被占用）
# import 和 CLI 命令仍用 "sluice"
pip install sluice-pipeline

# 推送渠道 Sink（Telegram / 飞书 / 邮件）
pip install "sluice-pipeline[channels]"

# Prometheus 指标
pip install "sluice-pipeline[metrics]"

# HN 评论富化器
pip install "sluice-pipeline[enrich-hn]"

# 全家桶
pip install "sluice-pipeline[all]"
```

> **先说实话：** Python 3.11+ 是硬要求。你还需要一个
> [Notion 集成 token](https://developers.notion.com/docs/create-a-notion-integration)
> 和至少一个兼容 OpenAI 的 API key（DeepSeek、智谱 GLM、OpenAI、OpenRouter 这些走
> `/v1/chat/completions` 协议的都行）。如果只用 RSS source + file_md sink，
> LLM key 可以不用。

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
└── prompts/ (also contains render templates)
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

# 任意 source 都可选：进入 stages 前先按同一套 mode/rules 过滤。
# `content` 表示 source 阶段可见的正文，如 RSS summary/content。
filter = { mode = "keep_if_any", rules = [
  { field = "title", op = "matches", value = "(?i)gpt|agent|model" },
  { field = "content", op = "matches", value = "(?i)gpt|agent|model" },
] }

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
template = "prompts/daily.md.j2"
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

完事 —— **30 行 TOML，一份完整的每日摘要 pipeline**。

---

## 核心概念

先把心智模型搭好。水闸的隐喻直接落到了代码上 —— 五个插件
**Protocol**，每条 pipeline 就是它们的组合：

| Protocol      | 做什么                             | 内置实现                                                                    |
| ------------- | ---------------------------------- | --------------------------------------------------------------------------- |
| `Source`      | 把 item 引入水流                   | `rss`                                                                       |
| `Fetcher`     | 把文章 URL → markdown              | `trafilatura`、`crawl4ai`、`firecrawl`、`jina_reader`                       |
| `Processor`   | 处理水流                           | `dedupe`、`cross_dedupe`、`fetcher_apply`、`html_strip`、`filter`、`field_filter`、`score_tag`、`llm_stage`、`render`、`limit`、`enrich`、`mirror_attachments` |
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

### LLM provider 池 —— 真正的杀手锏

200 行脚本也能调 LLM，这没问题。但让它同时管理一组加权 provider、自动冷却
用完配额 key、同模型重试瞬时错误、走降级链，并把超长 prompt 直接切到长上下文模型
—— 而且你不用写一行重试逻辑？

这才是 provider 池的威力。

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
  { value = "env:OR_KEY_2", weight = 1, quota_duration = 18000, quota_error_tokens = ["exceed", "quota"] },
]
active_windows  = ["00:00-08:00"]   # 仅在低峰时段用这个 base
active_timezone = "Asia/Shanghai"

[[providers.models]]
model_name          = "openai/gpt-4o-mini"
input_price_per_1k  = 0.00015
output_price_per_1k = 0.0006
max_input_tokens    = 32000
max_output_tokens   = 4096

[[providers.models]]
model_name          = "openai/gpt-4o"
input_price_per_1k  = 0.0025
output_price_per_1k = 0.01
max_input_tokens    = 128000
max_output_tokens   = 16384

[[providers]]
name = "ollama"
type = "openai_compatible"
[[providers.base]]
url = "http://localhost:11434/v1"
key = [{ value = "local" }]
[[providers.models]]
model_name = "llama3"
max_input_tokens  = 8192
max_output_tokens = 2048
# 本地免费，不用配价格
```

然后在任意 `llm_stage`：

```toml
model            = "openrouter/openai/gpt-4o-mini"
retry_model      = "openrouter/openai/gpt-4o-mini"   # 同档，瞬时错误重试
fallback_model   = "openrouter/google/gemini-flash"   # 便宜的 backup
fallback_model_2 = "ollama/llama3"                    # 兜底本地
long_context_model = "openrouter/openai/gpt-4o"        # 大 prompt / overflow 恢复

same_model_retries = 2
overflow_trim_step_tokens = 100000
long_context_threshold_ratio = 0.8
```

链按 主 → 重试 → fallback → fallback_2 依次推进。大 prompt 会直接路由到
`long_context_model`；context overflow 错误也会直接跳过去，不会先尝试中间
fallback。每层独立 worker / concurrency。配额耗尽的 key 自动冷却；时间窗路由把高价请求推到低峰。

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
long_context_model = "openrouter/openai/gpt-4o"
workers        = 8
max_input_chars   = 400000
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
template     = "prompts/daily.md.j2"
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
| `crawl4ai`     | 自托管 Crawl4AI。先 `POST /crawl`，如果服务返回 task id，再轮询 `/task/{task_id}` 或 `/jobs/{task_id}`。 |
| `firecrawl`    | 自托管 Firecrawl，处理 JS 渲染页。                        |
| `jina_reader`  | 托管 Jina Reader，自托管挂掉时兜底。                       |

Fetcher chain 可全局或每 pipeline 配置。每个请求按链顺序走，带 `min_chars`
校验和可选磁盘缓存：

```toml
[fetcher]
chain         = ["trafilatura", "crawl4ai", "firecrawl", "jina_reader"]
min_chars     = 500
on_all_failed = "skip"             # 或 "continue_empty" / "use_raw_summary"

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
<summary><b>⚙️ Processors（十二种 stage）</b></summary>

| `type`                  | 用途                                                                       |
| ----------------------- | -------------------------------------------------------------------------- |
| `dedupe`                | 丢掉本 pipeline `seen_items` 里已有的 item。                               |
| `cross_dedupe`          | 跨 source 合并重复文章：先按 URL，再按标题相似度。可按 source priority 选 winner，并合并 tags。 |
| `fetcher_apply`         | 走 fetcher chain 给 `item.fulltext` 灌内容。                              |
| `html_strip`            | 清理顶层字段或 `extras.<key>` 里的 HTML，保留段落/标题换行，并丢弃 script/style/template 内容。 |
| `filter`                | 规则过滤。17 种 op，含正则、长度、时间窗。带 ReDoS 防护。                  |
| `field_filter`          | 修改字段（truncate、drop、lower、strip、regex_replace）—— 比如喂贵的 LLM 之前把 fulltext 截到 20k。 |
| `score_tag`             | 逐 item 调 LLM 打分并写入 `extras.<score_field>`，同时追加或替换 tags。支持 JSON fence、数字字符串、截断和逐 item 失败处理。 |
| `llm_stage`             | LLM 调用，`per_item`（fan out）或 `aggregate`（一次跑全部）。支持 JSON 解析、最大输入截断、头尾保留、同模型重试、降级链、长上下文路由、overflow 裁剪、成本预检。 |
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

同样的 `mode` 和 `rules` 结构也可以写在任意 `[[sources]]` 里，
作为 `filter = { mode = "...", rules = [...] }`。source 级过滤会在 stages 之前运行，并额外支持
`content` 字段，表示 source 阶段可见的正文，例如 RSS 的 `summary`、
`description` 或 Atom `content`。

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
# 1. source 级正则预过滤 —— 进入 stages 前先跳过不相关 feed item
[[sources]]
type = "rss"
url  = "https://openai.com/blog/rss"

filter = { mode = "keep_if_any", rules = [
  { field = "title", op = "matches", value = "(?i)gpt|agent|model" },
  { field = "content", op = "matches", value = "(?i)gpt|agent|model" },
] }

# 2. 喂 LLM 之前的便宜 stage 过滤 —— 留下足够长的文章 + 砍标题里的广告
[[stages]]
name = "prefilter"
type = "filter"
mode = "keep_if_all"
rules = [
  { field = "fulltext", op = "min_length", value = 300 },
  { field = "title",    op = "not_matches", value = "(?i)广告|赞助|sponsored|advertisement" },
  { field = "published_at", op = "newer_than", value = "48h" },
]

# 3. LLM 打分 + 规则过滤组合 —— score_tag 给相关度 1-10，
#    `filter` 砍掉低于 6 分的。
[[stages]]
name = "score_and_tag"
type = "score_tag"
input_field = "fulltext"
prompt_file = "prompts/score_tag.md"
model       = "openrouter/openai/gpt-4o-mini"
score_field = "relevance"
tags_merge  = "append"

[[stages]]
name = "drop_irrelevant"
type = "filter"
mode = "keep_if_all"
rules = [
  { field = "extras.relevance", op = "gte", value = 6 },
]

# 4. 标签白名单 + URL 黑名单组合
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
所以让 `score_tag` 算一次写到 `extras` 里，下游用便宜的规则随便组合。
同样的 prompt 钱，下游过滤器无限组合。

</details>

<details>
<summary><b>📤 Sinks（输出端）</b></summary>

| `type`      | 说明                                                                      |
| ----------- | ------------------------------------------------------------------------- |
| `file_md`   | 确定性的本地 markdown 文件。当审计存档很合适。                           |
| `notion`    | 包 [`notionify`](https://pypi.org/project/notionify/)：markdown → Notion 数据库的页面。 |
| `telegram`  | 通过 Bot API 推送消息到 Telegram 群/私聊。MarkdownV2 渲染、安全截断、超长自动分片。 |
| `feishu`    | 推送消息到飞书/Lark。两种鉴权模式：`auth_mode = "webhook"`（默认）—— webhook URL + 可选 HMAC 签名；`auth_mode = "bot_api"` —— app_id + app_secret + receive_id，通过 Bot API 发送 Markdown 转换后的 post 消息。支持 `post`、`text`、`interactive`（Card V2）消息类型。 |
| `email`     | 通过 SMTP 发送 HTML 邮件。自动根据端口检测 TLS 模式（465→SSL，587→STARTTLS）。支持逐收件人发送、`fail_fast`/`best_effort` 策略。 |

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
<summary><b>📅 用 Prefect 调度 —— 生产环境升级</b></summary>

直接用 cron 跑 sluice 完全没问题。一行 crontab 就够了：

```bash
0 8 * * * cd /app && sluice run ai_news
```

这能跑。但跑上几周之后，你会开始问 cron 没法回答的问题：今天早上的 run
成功了没？哪条 item 失败了、为啥？能不能只重跑一个 stage 而不是整个 pipeline
重来？

Prefect 就是为这些来的。`sluice deploy` 把每个启用的 pipeline 注册成一个
**Prefect deployment** —— 带独立 cron、运行历史、逐 task 可观测性。你可以把
Prefect 理解为 pipeline 的 web 控制台：不是依赖，不是锁定，只是 cron 不够用
时的一个可选项。

**三步跑起来 —— 三个终端，半分钟：**

```bash
# 终端 1：启动 Prefect server（API + UI）
prefect server start

# 终端 2：把所有 pipeline 注册成定时部署
sluice deploy

# 终端 3：启动 worker 来执行调度任务
prefect worker start --pool default
```

完事。你的 pipeline 会按 cron 准时触发，Prefect UI 在
**http://localhost:4200**，能看到每个 pipeline 的运行时间线、重跑按钮、
per-task 日志 —— 差不多就是你一直想要的 n8n 那种控制台。

**几点值得注意的：**

- `sluice deploy` 是幂等的 —— 改完 pipeline 的 cron 再跑一次，Prefect 自动
  更新。
- worker pool 名（`--pool default`）可以自定义。GPU 密集型和轻量型 pipeline
  分到不同 pool。
- Prefect 挂了也不影响 sluice。Prefect 包着 sluice，不是反过来。
- 运行历史和日志都存本地 SQLite，跟 sluice 的 state DB 放一起，单机不需要
  额外搭 Postgres。

Prefect 完全可选。先拿 cron 试试水，想要可视化的时候加 `sluice deploy`，
等 pipeline 多了再上 Prefect server。一步步来，不用一开始就搞那么重。

</details>

---

## Docker 部署

你不需要 Docker 也能跑 sluice —— 一个 Python venv 就完全够用。但部署到 VPS、
NAS、或者 Kubernetes CronJob 的时候，容器会让事情简单很多。下面两条路。

Docker 配置在 [`scripts/docker/`](./scripts/docker/) 目录。
compose 文件默认使用已经发布的 GHCR 镜像。生产环境建议在
`scripts/docker/.env` 里设置 `SLUICE_IMAGE` 来固定版本。

### 路径 A：单次执行 —— 跑完就退

适合：serverless cron job、CI pipeline、或者"我想用 systemd timer 调 sluice
但不想在宿主机上装 Python 全家桶"。

```bash
cd scripts/docker

# 把配置和 .env 拷进来
cp -r ../../configs ./configs
cp .env.example .env
$EDITOR .env

# 跑一次
docker compose run --rm sluice run ai_news

# 先 dry-run 看效果
docker compose run --rm sluice run ai_news --dry-run

# 校验配置
docker compose run --rm sluice validate
```

compose 文件把 `./configs` 只读挂载、`./data`（SQLite 状态）可写挂载。
在宿主机配好 systemd timer、Kubernetes CronJob、AWS EventBridge —— 随便你
习惯用什么定时器 —— 触发 `docker compose run --rm` 即可。容器启动、跑完
pipeline、退出。不需要常驻进程，不绑端口，没有状态。

### 路径 B：搭配 Prefect —— 常驻调度

适合：多条 pipeline 日程交叉、想看运行历史、需要 UI 做重试和排障。

```bash
cd scripts/docker
cp -r ../../configs ./configs
cp .env.example .env
$EDITOR .env

docker compose -f docker-compose.prefect.yml up
```

这会同时起三个东西：
- **Prefect server** —— API + 控制台，访问 **http://localhost:4200**
- **sluice deploy** —— 容器启动时自动注册所有 cron 启用的 pipeline
- **Prefect worker** —— 到点了拉任务执行

数据（SQLite 状态、运行历史、文章缓存）通过 Docker volume 持久化。把容器
停掉，下周再拉起来，pipeline 状态还在，继续跑。

### 手动构建镜像

如果发布的镜像不符合你的环境，从源码构建：

```bash
# 从仓库根目录构建
docker build -f scripts/docker/Dockerfile -t sluice:local .

# 跑
docker run --rm \
  -v $(pwd)/configs:/app/configs:ro \
  -v $(pwd)/data:/app/data \
  sluice:local run ai_news
```

镜像包含所有可选依赖（`[all]` extras），Telegram、飞书、邮件 sink、
Prometheus 指标、HN enricher 开箱即用。

---

## CI/CD

GitHub Actions 工作流在 [`.github/workflows/`](./.github/workflows/)。
跟我们自己用的一样。

### `ci.yml` —— 每次 push 和 PR 都跑

Python 3.11 到 3.14 矩阵：

1. 安装依赖（`uv sync --all-extras --frozen`）
2. `ruff check .` —— lint
3. `ty check` —— 类型检查（0 错误）
4. `pytest -q`

### `publish.yml` —— 打 `v*.*.*` 标签时触发

推个版本 tag（`git tag v0.2.0 && git push --tags`），剩下全自动：

1. 校验 tag 版本跟 `pyproject.toml` 一致
2. `uv build` → `dist/`
3. 通过 **OIDC 可信发布**推上 PyPI —— 不需要 API token，不用轮换密钥

**一次性 PyPI 配置：**

1. GitHub → Settings → Environments 里建一个 `pypi` 环境
2. PyPI → [sluice-pipeline](https://pypi.org/project/sluice-pipeline/) → Publishing → 添加可信发布方：
   - Owner: `nerdneilsfield`
   - Repository: `sluice-pipeline`
   - Workflow: `publish.yml`
   - Environment: `pypi`

之后每次 `git push --tags` 自动发布。零手动步骤，零过期密钥。

---

## 路线图

✅ **v1（当前）** —— RSS source、Notion sink、file_md sink、核心 processors、
4 层 LLM 降级、幂等重试、dry-run、loguru 诊断、Prefect 调度、SSRF 防护。

✅ **v1.1**

- [x] Push-channel sinks：Telegram、Feishu、Email
- [x] Attachment 镜像（`mirror_attachments` stage）
- [x] Enricher 协议 + `hn_comments`
- [x] 亚日级 pipeline（`run_key_template`）
- [x] `limit` stage
- [x] `field_filter` 操作：lower、strip、regex_replace
- [x] `cross_dedupe`、`html_strip`、`score_tag` stages
- [x] Crawl4AI fetcher 支持
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
uv sync --all-extras                # 或 pip install -e '.[dev,all]'
pytest -q                           # 408 个测试
pytest --cov=sluice                 # 80% 覆盖率
ruff check .
ty check                            # 0 错误
```

架构和设计动机在 [`docs/superpowers/specs/`](./docs/superpowers/specs/)，
TDD 驱动的实现计划在 [`docs/superpowers/plans/`](./docs/superpowers/plans/)。

---

## License

MIT © [nerdneilsfield](https://github.com/nerdneilsfield)

<div align="center">

— *水闸升起，让对的流过去* —

</div>
