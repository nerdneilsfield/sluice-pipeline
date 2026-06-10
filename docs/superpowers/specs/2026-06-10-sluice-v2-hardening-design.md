# sluice v2 加固 — 设计文档

- 日期：2026-06-10
- 状态：待实现
- 作者：dengqi（+ Claude）

## 1. 摘要

对 v1→v2 迁移收尾后遗留的健壮性与部署安全缺口做一次聚焦加固，分三块：

1. **loader 结构校验**：配置加载时对必填字段做 collect-all 校验，手改 TOML 漏字段给出清晰 `ConfigError` 而非裸 `KeyError`。
2. **部署安全**：移除 `docker-compose.yml` 中数据库凭证的弱默认回退，未设即启动失败。
3. **文档 badge 修正**：README 测试数/覆盖率 badge 与实际对齐。

## 2. 背景与动机

2026-05-13 代码审核 + 2026-06-10 复审确认以下问题（均已核实为真）：

| # | 位置 | 严重度 | 问题 |
|---|------|--------|------|
| 1 | `loader.py:100-109` | 🟡 中 | 必填 key 直接下标取值（`task_doc["name"]`、`["window"]["anchor"]`、`["deployments"]`），漏字段→裸 `KeyError` |
| 2 | `loader.py:68-80` | 🟡 中 | source/sink/stage 的 `head["name"]/["type"]` 同样无保护 |
| 4 | `docker-compose.yml:6,16` | 🟡 中（安全） | `POSTGRES_PASSWORD:-prefect` 弱默认回退 |
| 5 | `docker-compose.yml:40` | 🟡 中（安全） | `PREFECT_API_AUTH_STRING:-` 默认空，worker 可能静默无认证连接 |

根因：loader 把 TOML 文档直接映射到 `TaskConfig`，语义校验（`validator.py`）作用于**已构建对象**，运行在 KeyError 之后，太晚。部署文件为开箱即用埋了弱默认。

## 3. 范围

- loader 结构校验层（#1 #2）
- 部署凭证默认值处理（#4 #5）
- README / README_ZH badge 修正

## 4. 非目标

- `scaffold_from_v1` 的 `_toml_value` 对 None/dict 输出非法 TOML（#3）— 单列 backlog，scaffold 是一次性迁移助手、产物人工复核，影响低。
- `ty` 报的 33 个类型 warning（集中在 `executors.py`/`stores/*` None 处理）— 单列 backlog。
- `validator.py` 重构为 collect-all — 不动。它校验**语义**、作用于已构建对象，职责与本次结构校验不同。

## 5. 设计决策

### 5.1 loader 校验策略：方案 A — 累加器函数

在 `sluice/dag/loader.py` 内新增私有函数组（与 `_split_named`/`_read_toml` 同文件，符合现有组织，不另起文件）：

```
_require(errors, doc, key, ctx)         # 单 key 存在检查，缺则 append 到 errors
_require_path(errors, doc, keys, ctx)   # 嵌套路径如 ["window","anchor"]
_validate_structure(docs) -> list[str]  # 总入口，遍历全部文件，返回全部错误消息
```

否决方案：声明式 schema 模块（5 文件十余 key，过度工程）；pydantic（现为纯 dataclass，引依赖+大改，违 YAGNI）。

### 5.2 校验在构建前运行

```
load_task(task_dir)
  ├─ 现有：目录/文件存在性检查（保留不动）
  ├─ 读 5 个 toml → docs（注意 task.toml 顶层 [task] 表本身也需先校验存在）
  ├─ ★ errors = _validate_structure(docs)
  │     if errors:
  │         raise ConfigError(
  │             f"invalid config in {task_dir}:\n  - " + "\n  - ".join(errors))
  ├─ 构建 TaskConfig（此后所有 [key] 取值已保证安全）
  └─ 返回
```

错误类型复用 `sluice.core.errors.ConfigError`（loader 已 import，与 `_pipeline_concurrency_strategy` 一致）。**不**用 `validator.ValidationError`。

### 5.3 concurrency_strategy 值校验并入 collect-all

现有 `_pipeline_concurrency_strategy`（loader.py:40-47）是 fail-fast 立抛。为统一行为，把「值非法」也改为 append 到 `_validate_structure` 的 errors 列表，而非单独抛出。保持单一报错出口。

### 5.4 部署安全：compose 原生 `:?` 而非 wrapper 脚本

现状：`Dockerfile.worker` 的 ENTRYPOINT 直接是 `prefect worker start`，**无启动脚本**可插入校验。

决策：用 docker-compose 原生 `${VAR:?message}` 插值——变量未设或为空时 `docker compose up`/`config` 立即失败并打印 message。无需新增脚本、不改 Dockerfile。

- `POSTGRES_USER` → `${POSTGRES_USER:?POSTGRES_USER must be set}`
- `POSTGRES_PASSWORD` → `${POSTGRES_PASSWORD:?POSTGRES_PASSWORD must be set (no weak default)}`
- `POSTGRES_DB` → `${POSTGRES_DB:?POSTGRES_DB must be set}`
- 连接串（line 16）三处插值同步改为 `:?` 形式

### 5.5 子决策：PREFECT_API_AUTH_STRING 保持可选

`PREFECT_API_AUTH_STRING` 空值对**自托管无认证** Prefect 服务器是合法配置；强制 `:?` 必填会破坏常见单机无认证部署。

决策：**保持** `${PREFECT_API_AUTH_STRING:-}` 默认空，但在 `docker-compose.yml` 加注释说明「设此值须与 server 端 auth 配置一致；留空 = 无认证，仅限可信网络」。`.env.example` 同步注释。

权衡：牺牲「默认强认证」换取无认证部署的可用性。真正的弱凭证风险在 DB 密码（已由 5.4 关闭）。

### 5.6 .env.example 收紧

移除弱默认值，改为占位提示，强调生产必改：

```
SLUICE_IMAGE=sluice/worker:0.1.0

# 必填：数据库凭证。生产环境务必设强密码，勿用示例值。
POSTGRES_USER=changeme
POSTGRES_PASSWORD=changeme-use-a-strong-secret
POSTGRES_DB=prefect

# 可选：Prefect API 认证串。须与 server 端一致；留空 = 无认证（仅限可信网络）。
PREFECT_API_AUTH_STRING=
```

## 6. 校验清单（块 1 完整覆盖）

collect-all，全扫完再统一报：

- `task.toml`
  - 顶层 `[task]` 表存在
  - `task.name`、`task.db_path`、`task.seen_db` 必填
  - `task.window.anchor`、`task.window.length` 必填（嵌套路径）
  - `task.deployments` 必填（表）
- `sources.toml` / `sinks.toml` / `stages.toml` 每个 entry
  - `name`、`type` 必填，错误带索引定位（如 `sources[2]: missing 'type'`）
- `pipelines.toml` 每个 entry
  - `name`、`stages` 必填，带索引定位
  - `concurrency_strategy`（若有）值合法（并入 5.3）

错误消息样例：

```
ConfigError: invalid config in configs/ai_news:
  - task.toml: missing 'task.window.anchor'
  - sources[1]: missing 'type'
  - pipelines[0]: missing 'stages'
  - pipelines[1]: invalid concurrency_strategy 'foo' (must be queue/enqueue/cancel_new)
```

## 7. 错误处理

- 单一报错出口：`_validate_structure` 收集所有问题，`load_task` 一次性 `raise ConfigError`。
- 文件级缺失（5 个必需 toml 不存在）仍由现有 `FileNotFoundError` 处理，不改。
- 校验顺序：文件存在 → 结构完整（本次）→ 语义合法（validator，已有）。

## 8. 测试策略（TDD）

### 块 1（loader）
失败用例先行，每类一个：
- 缺 `task.name` → ConfigError，message 含该项
- 缺嵌套 `window.anchor` → message 含 `task.window.anchor`
- source entry 缺 `type` → message 含 `sources[i]`
- pipeline entry 缺 `stages`
- 非法 `concurrency_strategy` 值并入聚合报错
- **多错聚合**：一次构造多个缺失，断言 message 同时含全部
- 回归：完整合法配置不报错、正常构建 `TaskConfig`

### 块 2（部署）
- `docker compose config` 在变量未设时**失败**（断言非零退出）
- 变量齐备时 `docker compose config` 通过
- （无 Python 逻辑，纯声明式，最小化测试）

### 块 3（badge）
- 无需测试。数字以最终 `pytest` 输出 + `--cov` 结果为准。

## 9. 文档 badge（块 3）

- `README.md` + `README_ZH.md`：
  - `tests-408%20passing` → 实现完跑 `pytest` 取实际数
  - `coverage-80%` → 取实际 `--cov` 值
- 两语言版同步。

## 10. 迁移影响

- 纯加固，无破坏性变更。合法既有配置行为不变。
- 部署侧：已设环境变量的用户无影响；依赖弱默认的用户会在 `compose up` 时被强制设值——这是预期的安全改进，需在 CHANGELOG 注明。

## 11. 文件清单

| 文件 | 变更 |
|------|------|
| `sluice/dag/loader.py` | 新增 `_require`/`_require_path`/`_validate_structure`；`load_task` 接入；`_pipeline_concurrency_strategy` 改 append |
| `tests/dag/test_loader.py`（或新建） | 块 1 全部用例 |
| `deploy/docker-compose.yml` | DB 凭证改 `:?`；auth string 加注释 |
| `deploy/.env.example` | 收紧默认值、补注释 |
| `tests/deploy/`（若有/新建） | `docker compose config` 校验测试 |
| `README.md` / `README_ZH.md` | badge 数字 |
| `CHANGELOG.md` | 记部署侧行为变更 |
