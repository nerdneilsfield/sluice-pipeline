# Sluice v4 Web Content Pipeline Design — Adversarial Review

**Document:** `docs/superpowers/specs/2026-08-18-sluice-v4-web-content-pipeline-design.md`
**Branch:** `v4-cleanroom` @ `00259c8` (Design baseline cited as `070e8f8`)
**Mode:** Read-only. No files changed. No implementation plan.

---

## 1. Verdict: **NO-GO**

## 2. Executive summary

核心产品决策（双链、Item 内顺序 fallback、Stage 缓存独占、named instances、终端 empty StageResult、provider/extractor 集合）本身不是问题。文档不能进入 Plan，是因为**唯一写死的 fallback 算法只覆盖 article 质量胜出**，却同时把 list Source 与 RSS raw fallback 绑到同一个 `WebContentClient.resolve`。按 §6.7 第 4 步，Firecrawl/Crawl4AI 一次返回 Markdown+HTML 时，identity 会在 list/RSS 请求上直接胜出，AC6/AC7 无法按字面实现。

第二处阻塞：§6.6 禁止 Stage 以 Artifact/`fetch_webpage` 收尾，合法 Stage 必须跟 `to_markdown`/`to_text`；§10/AC8 却把终端 drop 限定为 “the single built-in `fetch_webpage`”。按严读，合法配置永远走不到 drop；按宽读，又没有规定 `FetchChainExhausted` 在现有 `_fail_execution`（失败+重试）与 `invalid: drop`（`dropped`+empty）之间如何变成 `succeeded`+empty。Plan 必须发明 Stage 状态机。

其余 High 项（snapshot 精确 JSON、Source bind 签名、extractor 输出格式、`extract_content` 二次链、html-to-markdown 被放进 extra、Firecrawl `html`/`rawHtml`/`maxAge`、Crawl4AI 轮询路径、Newspaper4k 默认联网）都是可补丁合同，但应在 B1/B2 修完后再写 Plan。

---

## 3–4. Findings

### Blocker

#### B1 — 唯一 fallback 算法与 list/RSS 胜出条件互斥

**Design**
- §6.5 L683–686：所有解析共用 `resolve(request, fetch_chain, extract_chain) -> DocumentArtifact`
- §6.7 L731–754：第 4 步「If Markdown or text is present, run `identity` first… A passing candidate wins immediately」；第 5 步才用 raw HTML 跑剩余 extractor
- §7 L800–802：article 接受 html/text/markdown 并一次要 Markdown+HTML；list 只接受 raw HTML；RSS 只接受 raw_source
- §13.1 L1325–1333：list 质量是结构而非 `min_chars`；合法无匹配 HTML 必须继续试后续 raw-HTML provider
- §13.2 L1378–1398：RSS 无 extract_chain；首个可解析 feed 胜出，含 0 条 entries

**Code**
- 当前无 `WebContentClient`。`webpage.py` `_list_records` 直接解析 HTML；`rss.py` `_parse` 用 `xml.etree` 扫任意 `item`/`entry`。
- `run_stage_tasks`（`stage_runtime.py` L226–244）把 `NEW.value` 原样传给下一 Task，不区分 Artifact/list/feed。

**Failure**
1. List Source + Firecrawl/Crawl4AI：provider 同时带回长 Markdown 与 HTML。§6.7.4 identity 过质量 → `selected_format=markdown`，list parser 拿不到 raw HTML，或拿到已抽正文、选择器为空。
2. 合法空列表页被当成 article 失败/`too_short`，不会按 §13.1 继续试 browser provider；或反过来，challenge 页的 Markdown 过 `min_chars` 被当成胜者。
3. RSS + Firecrawl（声称 `raw_source`）：若响应里还有 Markdown，§6.7.4 在 `extract_chain is null` 时行为未定义；若走 identity，feed XML 永不解析。

**最小修正**
把 `resolve` 拆成三个显式 mode，并写进算法，禁止混用 article 质量：

| Mode | 资格 | 胜出 | extract_chain | identity |
| --- | --- | --- | --- | --- |
| `article` | html\|text\|markdown | 第一条过 §9 的候选 | 必填 | 仅当 Markdown/text ∈ acceptable |
| `list_html` | 仅 `raw_html` | 合法非 challenge HTML；无匹配则留下 empty candidate 并继续 | 禁止 | 禁止 |
| `raw_source` | 仅 `raw_source` | 首个通过 §13.2 根识别的字节 | 缺省 | 禁止 |

`acceptable` 必须约束胜出表示，而不能只做预调用资格过滤。

---

#### B2 — 终端 drop 与「Stage 必须以 projector 收尾」互相取消

**Design**
- §6.6 L720–726、§8 L1008–1010：含 `fetch_webpage` 的 Stage 必须由 `to_markdown`/`to_text` 结束；loader 拒绝最终值为 Artifact 的 Stage
- §10 L1118–1140：最后一次重试耗尽后，「`each` Stage whose failing operation is the single built-in `fetch_webpage`」把终端 Execution 标为 `succeeded` + empty `succeeded` StageResult
- AC8 L1528–1529：exhaustion 精确遵守 node retry，并只 drop 该 each Item

**Code**
- `run_stage_tasks`：Task 返回 `DROP` → `StageOutcome(outputs=(), status="succeeded")`（L239–240）——**第一次**就 empty，不走 retry
- `_execute_claimed_stage`（L1426–1448）：异常一律 `_fail_execution`
- `_fail_execution`（L1266–1310, 1311+）：未耗尽则 `failed` + 排队 retry；仅 `InvalidStructuredOutput` + `invalid: drop` 在耗尽时发 `dropped` + empty StageResult
- 现成 fixture 已违反新规则：`tests/integration/test_shared_materialization.py:517` 的 Stage 以单独 `fetch_webpage` 结束

**Failure**
- 严读：「single `fetch_webpage`」= Stage 只有这一 Task → 所有合法生产配置都不会 drop，AC8 死代码。
- 宽读：`FetchChainExhausted` 必须在 projector 未运行的情况下，把**最后一次** attempt 从异常改成 `succeeded`+empty。文档未规定：`executions.error` 是否为 NULL、retry_count 何时算耗尽、与 `invalid: drop` 的 `dropped` 路径如何区分、共享 waiter 看见的是 empty `succeeded` 还是仍是 `failed`。

**最小修正**
写死这 6 条，不要改「retry 后再 empty StageResult」这一已批准决策：

1. `FetchChainExhausted` 仅由内置 `fetch_webpage` 抛出；后续 projector 不得执行。
2. 资格：Stage `input=each`，且失败点是该 `fetch_webpage`（允许其后有且仅有内置 projector）。
3. `retry_count < retries`：当前 attempt = `failed` + 净化后的 `FetchChainExhausted` 摘要；claim 转给下一次 queued attempt（沿用现有 `_fail_execution`）。
4. 最后一次 attempt（`retry_count >= retries`）且仍持有同一 claim：`_finalize_stage(StageOutcome((), "succeeded"))`；`error` 为 NULL；empty StageResult 成为 `stage_result_current`。
5. 该异常不得走 `invalid: drop` 的 `dropped` 状态。
6. 其他异常、`extract_content` 耗尽、batch、自定义 Task：保持 `failed`，不发 StageResult。

---

### High

#### H1 — Trafilatura composite 强制 extractor，破坏「两条独立链」

**Design:** §3.1 L66–76；§6.7 L756–762；§7.3 L821–826；provider 表 L297 与 extractor 表 L450 重复 `include_tables`/`include_comments`。
**Code:** 当前 `extract_content` 是独立 Task，fetch 不做抽取。
**Failure:** fetch_chain 里的 `type: trafilatura` 「forced Trafilatura extractor binding」会覆盖 Task/Source 声明的 `extract_chain`（identity→newspaper→…）。同一 Item 内 HTTPX memoize 与「强制换 extractor」被写在同一段。
**修正:** composite 只展开为 HTTPX 传输 + 尝试内 memoize。`include_tables` 等只属于 extractor 实例。禁止 provider 覆盖 `extract_chain`。

#### H2 — Extractor 输出格式 / `selected_format` 未定义

**Design:** §6.2 L612–613、L635–638；§8 L878–885、L950–957（projector 完全依赖 `selected_format`）。
**Failure:** Trafilatura「Markdown」、Newspaper「text」、Resiliparse/BeautifulSoup 未说明产出 html 还是 text。`to_markdown` 对 `text` 会转义为字面量，对 `raw_html` 会走转换器。同一页因不同 extractor 胜出得到完全不同的持久化 payload。AC14 不可证。
**修正:** 为每个 extractor 固定 `selected_format` 与写入字段（建议：trafilatura→`markdown`；newspaper4k/resiliparse/beautifulsoup→`text`，除非显式产出清洗 HTML）。禁止 projector 在 `selected_format` 之外自行猜测。

#### H3 — `extract_content` 接在 `fetch_webpage` 后可被 leftover Markdown 短路

**Design:** §6.6 L721–725；§6.7 L739–740。
**Failure:** fetch 用 chain A 抽出过质量的 Markdown。后接 `extract_content` + 不同 chain B。digest 不同 →「continues only the declared local extractor chain」。identity 仍是 B 的第一项，立刻对已有 Markdown 胜出，B 的 trafilatura/newspaper 永不运行。
**修正:** digest 相同才原样返回。digest 不同则清除 `selected_format` 与先前 extraction，仅保留 fetch 表示（raw_html/text/markdown），再跑 B；identity 只接受**provider 原始** text/markdown，不接受先前 extractor 的输出。

#### H4 — Stage/Source snapshot 精确形状与现有严格 decoder 冲突

**Design:** §5 L473–533（`TaskDef.web_binding` 与 `with` 分开存储；缺省段为 JSON null）。
**Code:** `decode_stage_snapshot`（`config.py` L747–775）要求 snapshot 键恰好 `{name,input,cache_version,tasks,output_schema}`，每个 Task 恰好 `{use,with}`。`canonical_stage_config`（L654–698）只哈希 `use`/`with`。Source digest（`source_runtime.py` L37–46）只哈希 `{source,use,with}`。`collect`（L757）只把 `with_config` 传给 Source。
**Failure:** 加上 `web_binding` 后，旧 retained execution/waiter 无法 decode；rebuild 失败。若不把 semantic `web_binding` 写入 `canonical_stage_config`/`_binding_digest`，改 provider 顺序不会换 Stage key / Source binding，AC11 失败。
**修正:** 给出完整 JSON schema：每个 Task `{use,with,web_binding}`，非 web Task 的 `web_binding` 为 null；Source snapshot/digest 含 semantic `web_binding`。明确：升级后、本功能之前的 retained web/`to_markdown` snapshot 不可执行，rebuild 报错，不得回退到当前 YAML。

#### H5 — 核心 projector 依赖 `web` extra

**Design:** §8 L893–910；§15 L1468–1480（html-to-markdown 在 extra；`markdown-it-py` 在 core）。§6.6 L716–718：`to_markdown`/`to_text` 是固定内置。
**Code:** `pyproject.toml` 核心依赖只有 `PyYAML`。大量 Stage 只有 `to_markdown`（smoke/integration）。
**Failure:** 未装 extra 时，现有 `to_markdown` Stage 无法做 HTML 转换，或 loader 把「未配置的」html-to-markdown 当成缺失 optional dep。与「不要无 blocker 就加实验依赖」及 AC15 冲突。
**修正:** 二选一并写死：(a) html-to-markdown 升为核心依赖；(b) 核心 `to_markdown` 仅用 BeautifulSoup + markdown-it-py，html-to-markdown 只在 `web` extra 且仅当声明了 web fetch/extract 时启用。

#### H6 — Firecrawl：`html`≠`rawHtml`，且默认 2 天 URL 缓存

**Design:** §3.4 L105–114；§4.2 L299；§7.5 L849–851；AC5 L1524。
**Vendor（2026-08-18 Firecrawl v2 scrape）：** `formats` 含 `html`（清洗后，受 `onlyMainContent` 影响，API 默认 true）与 `rawHtml`（未修改）。`maxAge` 默认 `172800000`（2 天）。`write` 无 selector；`scroll` 是 up/down，不是 x/y。
**Failure:** 若请求 `html` 而非 `rawHtml`，list CSS 跑在被剥掉的 DOM 上（B1 的放大）。未设 `maxAge=0` 时，Stage cache miss 仍可能打到 Firecrawl 的 URL TTL，违反「只有 Stage 缓存」。
**修正:** article：一次请求 `markdown` + `rawHtml`（不要用清洗 `html` 充当 `raw_html`）。list/RSS：只要 `rawHtml`。所有 Firecrawl 调用 `maxAge=0`（并写明 `storeInCache`）。给出 Sluice action → Firecrawl action 的一一映射（write = click + write）。

#### H7 — Crawl4AI 默认 poll path 与现行 API 不符

**Design:** §4.2 L298：`poll_paths` 默认 `[/task/{id}, /jobs/{id}]`。
**Vendor:** 同步 `POST /crawl`；异步 `POST /crawl/job` + `GET /job/{task_id}`。0.9.0 默认要认证。
**Failure:** 按默认 path 轮询会 404，然后 `provider_error` 落到后续付费 provider。
**修正:** 默认改为文档路径：优先同步 `/crawl`；若用 job API，则 `GET /job/{id}`。规定 `api_key` 如何变成 Authorization；0.9 无 key 的实例为 startup error。

#### H8 — Newspaper4k / Trafilatura 默认可出网

**Design:** §6.4 L676–678：「Extractors … do not perform network access。」§4.6 L451。
**Vendor:** Newspaper4k `fetch_images` 默认 True；须 `set_html`/`input_html` 且关闭 download。Trafilatura 有 `fetch_url`。
**Failure:** extractor「本地、确定性」合同被默认实现打破；SSRF/付费边界被绕过。
**修正:** 写死离线 API：Newspaper `fetch_images=False`，只 `set_html`；Trafilatura 只用 `extract()`；任何出网 = `extract_failed`，不算 provider 尝试。

#### H9 — Source 拿不到 `WebContentClient` / `web_binding`

**Design:** §5 L520–525；§6.5 L693–699；§13.1 L1300。
**Code:** `bind_builtin_source(path, http_client)`（`resolution.py` L129–135）；`BoundaryAdapters` 只有 `http_client`/`llm_client`/`sink_clients`；`collect_source` 传 `with_config`。
**Failure:** webpage/RSS 无法解析 named chain。Plan 会发明 bind 签名与 invocation mapping。
**修正:** `BoundaryAdapters.web_content_client`；`bind_builtin_source(path, adapters)`；内置 Source `collect` 收到 `{**with_config, "web_binding": ...}`；自定义 Source 仍只收 authored `with`。RSS：有注入 `HTTPClient` 则 `__rss_direct__` 走它，否则走绑定里的 HTTPX。

#### H10 — Firecrawl/Crawl4AI 的 `raw_source` 能力未定义

**Design:** §7 表 L789–790；§13.2 L1385–1398。
**Failure:** 这些服务默认返回 Markdown/清洗 HTML。对 `feed.xml` 的 scrape 通常不是可解析 RSS/Atom 字节。AC7 在「声称 raw-capable」的 provider 上不可证。
**修正:** `raw_source` 仅当响应体（或 Firecrawl `rawHtml` / Crawl4AI 原始 body）通过 §13.2 `defusedxml` 根识别时才设置。Markdown/article HTML 不得当作 feed。不能提供未转换字节的 provider 从 `raw_source` 静态能力里删除（至少 Jina、Bright Data markdown 模式；Firecrawl 除非证明 `rawHtml` 是 XML）。

---

### Medium

#### M1 — `WebBinding.version` 是否进入每一个 Stage key

§5 L539–540：「participates in every Stage key」。若 `dedup`/`filter` 也注入 `web_binding.version=1`，升级会刷掉全部 Stage 缓存。应改为：仅当该 Stage 至少有一个内置 web Task 时才写入 binding。

#### M2 — Provider deadline 与 Stage timeout 的合成

§6.1 `deadline`、§11 L1174–1176 未定义 `deadline = min(now+instance.timeout, stage_deadline?)`。N 个 60s provider 顺序执行可远超 Stage timeout；probe 只在调用之间检查。应规定：instance timeout 被剩余 Stage watchdog 截断；截断后当前调用可完成但不能启动下一个 provider。

#### M3 — 两个 `max_body_bytes`

§4.1 provider 字段 vs §9 quality `max_body_bytes 10 MiB`。取 min 还是谁覆盖，未写。应：有效上限 = min(provider, 10 MiB)；quality 项不可配置。

#### M4 — List/RSS 仍物化并哈希 article quality

§4.5 允许 quality 字段；§13.1/13.2 说 `min_chars` 不适用。改无效果的 `min_chars` 仍会换 Source binding。应：list/RSS loader 拒绝 article quality，或在 binding 里显式标 `unused` 且不进 digest。

#### M5 — List/single-page `metadata.web.extractor`

§13.1 L1318、L1355 要求与 projector 相同的 bounded metadata。list 没有 article extractor。应固定：list → `list_selector`；RSS → `feed_parser`（已有）；单页 → 与 Stage projector 同一函数、同一 pruning。

#### M6 — `published_at` / `authors` 形状

§8 L974–975 未规定类型。非确定或异构 JSON 会改变 `make_item_id`（`canonical.py` L200–225，id 含 metadata）。应：`published_at` 为 RFC3339 UTC 或 null；`authors` 为去重后的字符串列表。

#### M7 — Zyte `automatic_content` 与 action 翻译

§4.2 L301 的 `automatic_content` 不是 Zyte 字段。现行 API：`POST https://api.zyte.com/v1/extract`，`browserHtml`/`httpResponseBody`/`article`。Zyte action 为 `{action: waitForTimeout|click|scrollBottom,...}`，selector 是 `{type,value}`。应给出 mode→请求字段表，以及 Sluice action→Zyte action 表；无法翻译则 loader 拒绝。

#### M8 — Bright Data 字段名

Unlocker 用 `data_format: markdown` + `format: raw`，不是 Sluice 的 `output`。`async` 禁止是对的。应写请求 JSON 映射。

#### M9 — `startup_http` / 注入 `HTTPClient` 绕过 SSRF

§6.2 L650–657、§13.2 L1378–1380。测试与「trusted」部署下，direct RSS/webpage 不做 DNS pin。应：非 test 的 `trusted: true` 必须同时有显式 allowlist，或仅允许测试。

#### M10 — 升级后 `to_markdown` 缓存分区

所有现有 `to_markdown` Stage 会因 `WebBinding` 换 lookup key。应明确这是一次有意的 cache break，并更新 AC15/fixture 预期。

---

### Low

#### L1 — `no_match` 类型未定义（§6.4）。修正：模块级 sentinel，不要用异常或 None。

#### L2 — `warnings` 稳定码未列举（§8 L976）。修正：闭集（`metadata_truncated`, `sanitizer_rejected_link`, …）。

#### L3 — Challenge 短语/DOM 启发式（§9 L1041–1051）会误伤短 SPA。已规定不可关。修正：把启发式钉在 `WebBinding.version` 附录表，避免 Plan 自由发挥。

#### L4 — `App.close()`（§11 L1198）当前不存在。作为新合同可以，但要写清与 worker join 的顺序。

#### L5 — Design baseline `070e8f8` 不是当前 HEAD `00259c8`。修正：改 baseline。非功能缺陷。

---

## 5. Claims checked and rejected

| Claim | Why rejected |
| --- | --- |
| 「`succeeded`+empty 与 cleanroom `dropped`+empty 矛盾，应改成 `dropped`」 | 已批准决策。Task `DROP` 已经是 `succeeded`+empty（`stage_runtime.py` L239–240）。缺陷是 B2 的适用条件/转换路径，不是状态名。 |
| 「Bright Data Unlocker 不能出 Markdown」 | `data_format: markdown` 存在。问题只是字段映射（M8）。 |
| 「核心新增 HTTPX/BS4/defusedxml/markdown-it-py 违反依赖约束」 | §15 已列为 core。约束针对的是未证明必要的实验依赖。H5 只针对被放进 extra 的 **html-to-markdown**。 |
| 「provider concurrency/cooldown 不应进 Stage key」 | §3.4/§11 已说明它们改变谁胜出。 |
| 「应保留 `extract_content.mode` / webpage `selector` / 无 chain 的 fetch」 | Design 明确改 fixture/config；非缺陷。 |
| 「AC15 与改 fixture 矛盾」 | §6.5 L701–706：green = 行为断言在新显式配置下成立，不是旧最小 YAML 仍合法。 |
| 「应引入 URL TTL 缓存 / 并行 provider 竞速」 | 已否决，不重开。 |
| 「必须用 vendor SDK」 | §15 已选 Sluice HTTP adapter。 |
| 「`.superpowers` 会被提交」 | `.gitignore` 已有 `.superpowers/`。§18 与现状一致。 |
| 「list logical_id / 单页不再持久化 raw HTML 破坏兼容」 | §13.1 显式更新；允许。 |
| 「RSS 改用 `defusedxml` + 根元素识别改变 record 集合」 | 相对当前 `root.iter()` 任意 `item`/`entry` 是有意收紧（§13.2 L1400–1404）。 |

---

## 6. Plan-readiness checklist

| Gate | Status |
| --- | --- |
| 单一、无歧义的 Item fallback 算法覆盖 article / list / RSS | **FAIL** (B1) |
| 终端 drop 与强制 projector 共存，且钉在现有 Stage 状态机上 | **FAIL** (B2) |
| Snapshot / canonical key / Source digest 的精确 JSON | **FAIL** (H4) |
| Source/Task 如何获得 `WebContentClient` 与 `web_binding` | **FAIL** (H9) |
| Extractor → `selected_format` → projector | **FAIL** (H2, H3) |
| 双链独立（无 composite 覆盖） | **FAIL** (H1) |
| 核心 `to_markdown` 的依赖闭合 | **FAIL** (H5) |
| Vendor 格式/缓存/路径与 AC5–7 可证明 | **FAIL** (H6, H7, H10) |
| Extractor 零网络 | **FAIL** (H8) |
| 双链、顺序 fallback、named instances、Stage-only cache（Sluice 侧）、provider 集合 | PASS（决策本身） |
| `.superpowers` 不提交 | PASS |
| 允许更新 fixture | PASS |

在 B1、B2 以及 H1–H10 补进同一份 Design 之前，不要开 Plan。

---

## 7. Final prioritized patch list

1. **B1** — 三个 `resolve` mode；`acceptable` 约束胜出表示；list 继续试 empty HTML；RSS 禁止 identity。
2. **B2** — 合法 `fetch_webpage`+projector Stage 的终端 `succeeded`+empty 路径；6 条状态机规则。
3. **H1** — Trafilatura profile = HTTPX + memoize only。
4. **H2+H3** — extractor 输出表；二次 `extract_content` 重置规则。
5. **H4+H9+M1+M10** — snapshot/digest/bind/collect JSON 与升级行为。
6. **H5** — html-to-markdown 核心 vs extra 二选一。
7. **H6+H7+H10+M7+M8** — 每个 adapter 的请求字段、禁用 vendor URL cache、真实 poll/auth 路径、`raw_source` 填充规则。
8. **H8** — Newspaper/Trafilatura 离线 API 钉死。
9. **M2–M6, M9** — deadline 截断、body 上限、list/RSS quality、metadata 形状、trusted SSRF。
10. 更新 Design baseline commit；把 challenge 启发式收到 versioned 附录。

补完并再审通过之前，本 Design **不能**进入 Superpowers Plan。
