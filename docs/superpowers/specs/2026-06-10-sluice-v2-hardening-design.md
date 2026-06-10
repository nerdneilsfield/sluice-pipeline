# sluice v2 加固 — 设计文档

- 日期：2026-06-10
- 状态：待实现（已过 codex 对抗审核，12 项发现全部折入 v2）
- 作者：dengqi（+ Claude）

## 1. 摘要

对 v1→v2 迁移收尾后遗留的健壮性与部署安全缺口做一次聚焦加固，分四块：

1. **loader 结构校验**：配置加载时对**缺失字段、类型形状、值域**做 collect-all 校验，并把 TOML 解析错纳入同一 `ConfigError` 出口；杜绝裸 `KeyError`/`TypeError`/`AttributeError`/`TOMLDecodeError`。
2. **部署安全**：移除 `docker-compose.yml` 弱凭证默认、把 Prefect server 默认绑回环、约束 DB 密码字符集。
3. **scaffold 防护**：`_toml_value` 遇不支持类型（None/dict）抛 `ConfigError`，不再吐非法 TOML。
4. **文档 badge 修正**：README 测试数/覆盖率与实际对齐。

## 2. 背景与动机

2026-05-13 审核 + 2026-06-10 复审 + codex 对抗审核确认问题（均核实为真）：

| 来源 | 位置 | 严重度 | 问题 |
|---|------|--------|------|
| 复审 #1 | `loader.py:100-109` | 🟡 中 | 必填 key 直接下标取值 → 裸 `KeyError` |
| 复审 #2 | `loader.py:68-80` | 🟡 中 | entry 的 `name/type` 无保护 |
| codex F1 | `loader.py:61,106,109` | 🔴 高 | 只查 key 存在不查类型；`window="24h"`（标量非表）→ `TypeError` |
| codex F2 | `loader.py:35,68-82` | 🔴 高 | 不查 list/entry 形状；`sources="bad"` 或非 dict 项 → `AttributeError` |
| codex F3 | `loader.py:28` | 🟠 高 | `_read_toml` 在 ConfigError 边界外，语法/编码/IO 错裸抛 |
| codex F4 | `loader.py:93`,`config.py:40,59,66` | 🟠 高 | 构造期 `ValueError`（`int(max_attempts)`、`DeploymentsDef`、`input_mode`、`concurrency_strategy`）绕过 collect-all |
| 复审 #4 | `docker-compose.yml:5-7,16` | 🟡 中（安全） | DB 凭证弱默认回退 |
| codex F7 | `docker-compose.yml:17,20,40` | 🟠 高（安全） | server 绑 `0.0.0.0` + 发布 `4200:4200`，auth 可选 = 无认证 API 暴露宿主 |
| codex F8 | `docker-compose.yml:16` | 🟠 高 | 密码特殊字符（`@:/#%$`）破坏 DSN 拼接 |
| codex F11 | `admin.py:158,173` | 🟡 中 | `_toml_value` 对 None/dict 吐 `repr` → 非法 TOML |

根因：loader 把 TOML 文档直接映射到 `TaskConfig`，语义校验（`validator.py`）作用于**已构建对象**，运行在崩溃之后，太晚。部署文件为开箱即用埋了弱默认且把 server 暴露到宿主。

## 3. 范围

- loader 结构校验层（缺失 + 类型 + 形状 + 值域 + 解析错包装）
- 部署：凭证默认、server 绑定、密码字符集
- `scaffold_from_v1` 的 `_toml_value` 类型防护
- README / README_ZH badge 修正

## 4. 非目标

- `ty` 报的 33 个类型 warning（`executors.py`/`stores/*` None 处理）— 单列 backlog。
- `validator.py` 重构 — 不动。它管**语义**（交叉引用、唯一性、无环、registry），职责与本次**结构**校验互补，见 5.7 边界。
- `deploy.py:49-55` 的 concurrency_strategy 校验 — **保留不动**（见 5.8）。

## 5. 设计决策

### 5.1 loader 校验策略：方案 A — 累加器函数

`sluice/dag/loader.py` 内新增私有函数组（同文件，符合现有组织）：

```
_require(errors, doc, key, ctx)              # key 存在
_require_table(errors, doc, key, ctx)        # key 存在且值为 dict（F1）
_require_list(errors, doc, key, ctx)         # key 缺省或值为 list（F2）
_require_path(errors, doc, keys, ctx)        # 逐段断言中间节点是 dict（F1）
_validate_structure(docs) -> list[str]       # 总入口，返回全部错误消息
```

否决：声明式 schema 模块（过度工程）、pydantic（引依赖+大改，违 YAGNI）。

### 5.2 校验在构建前运行 + 解析错包装（F3）

```
load_task(task_dir)
  ├─ 目录/文件存在性检查（保留）
  ├─ docs = {f: _read_toml(path)}   # _read_toml 内 try/except 包 TOMLDecodeError /
  │                                  #   UnicodeDecodeError / OSError → ConfigError(带文件名)
  ├─ ★ errors = _validate_structure(docs)
  │     if errors:
  │         raise ConfigError(f"invalid config in {task_dir}:\n  - " + "\n  - ".join(errors))
  ├─ try: 构建 TaskConfig
  │  except (ValueError, TypeError) as exc:   # F4 兜底：残留构造期错统一包装
  │      raise ConfigError(f"invalid config in {task_dir}: {exc}") from exc
  └─ 返回
```

错误类型统一 `sluice.core.errors.ConfigError`。**不**用 `validator.ValidationError`。

### 5.3 类型 + 形状校验（F1 / F2）

`_validate_structure` 对每段都先断言类型再下钻：

- `task.toml`：顶层 `[task]` 是表；`window`、`deployments` 是表（非标量）；嵌套取值前逐段确认是 dict。
- `sources/sinks/stages/pipelines`：根 key 缺省或为 **list**；每个 element 为 **table**，否则报 `sources[2]: expected table, got str`。

### 5.4 值域校验并入 collect-all（F4）

从原始 docs 直接校验已知枚举/类型，避免落到构造器才崩：

- `pipeline.concurrency_strategy`（若有）∈ `CONCURRENCY_STRATEGIES`
- `pipeline.input_mode`（若有）∈ 合法集
- `pipeline.max_attempts`（若有）可转 int
- `task.deployments.pipelines` 为 `"all"` 或 list

构造器的 `ValueError`/`TypeError` 仍由 5.2 的 try 兜底包装为 ConfigError（belt-and-suspenders，覆盖未来新增字段）。

### 5.5 部署安全：compose `:?` + server 绑回环（F7 / 复审 #4 #5）

`Dockerfile.worker` ENTRYPOINT 直接是 `prefect worker start`，**无脚本**可插校验 → 用 compose 原生 `${VAR:?msg}`（变量未设/空则 `docker compose up`/`config` 立即失败）。

- DB 凭证去默认、强制必填：
  - `${POSTGRES_USER:?POSTGRES_USER must be set}`
  - `${POSTGRES_PASSWORD:?POSTGRES_PASSWORD must be set (no weak default)}`
  - `${POSTGRES_DB:?POSTGRES_DB must be set}`
  - 连接串（line 16）三处插值同步改 `:?`
- **server 绑回环**（F7 决策）：端口发布改 `127.0.0.1:4200:4200`，默认仅本机可达。单机部署下 auth 可选即站得住。
- `PREFECT_API_AUTH_STRING` **保持** `${...:-}` 可选默认空，但 compose + `.env.example` 加注释：
  > 留空 = 无认证，仅当 server 绑回环（默认）时可接受。要对外暴露（改 ports 为 `0.0.0.0`），**必须**设非空 auth 并同步 server 端 `PREFECT_SERVER_API_AUTH_STRING`。

### 5.6 DB 密码字符集约束（F8）

`${POSTGRES_PASSWORD}` 直插 DSN URL，含 `@ : / # % $` 会破坏拼接。决策：**不**加 entrypoint 脚本（与「无脚本」初衷一致），改在 `.env.example` 注释约束字符集：

```
# 必填：数据库密码。直接拼入连接 URL，须为 URL 安全字符。
# 推荐：openssl rand -hex 32   （hex 输出无特殊字符；勿用 base64，其 +/= 会破坏 DSN）
POSTGRES_PASSWORD=
```

### 5.7 loader / validator 职责边界（F5，写死）

- **loader** 负责：结构形状、原始类型、值域枚举、TOML 解析错 → 全部 `ConfigError`，在构建 `TaskConfig` 前。
- **validator.py** 负责：交叉引用、名字唯一、安全名、形状语义、DAG 无环、registry → `ValidationError`，在已构建对象上。
- 校验顺序：文件存在 → **结构（本次）** → 构建 → 语义（validator）。

### 5.8 保留 deploy.py 校验（F12）

`deploy.py:49-55` 的 effective concurrency_strategy 校验**保留**。它也服务**程序化构造**的 `TaskConfig`（不经文件 loader）。5.4 的「collect-all」仅限文件加载路径；deploy.py 那份是 defense-in-depth。

### 5.9 load_all_tasks 静默跳过（F6）

`loader.py:121` 仅处理含 `task.toml` 的目录。决策：**保留**「无 task.toml 的目录非任务、跳过」为有意行为，但加一行 `logger.warning(f"skipping {dir}: no task.toml")` 便于排查 typo。spec「必需 5 文件」校验仅对已进入的任务目录生效——措辞已收紧。

### 5.10 scaffold _toml_value 防护（F11）

`_toml_value` 遇 `None`/`dict`/其他不支持类型，**抛 `ConfigError`**（带字段定位），不再 `repr` 吐非法 TOML。避免 scaffold 产物日后被新 loader 报错却无源头线索。

### 5.11 .env.example 收紧（F10）

弱默认 `prefect` 与占位 `changeme` 均能过 `:?` 却是已知值。决策：必填密钥**留空**（让 `:?` 拦住），仅给生成指引：

```
SLUICE_IMAGE=sluice/worker:0.1.0

# 必填：数据库凭证。留空会让 compose 启动失败——这是预期的安全护栏。
POSTGRES_USER=
POSTGRES_PASSWORD=    # openssl rand -hex 32
POSTGRES_DB=prefect

# 可选：Prefect API 认证串。须与 server 端一致。
# 留空 = 无认证；仅当 server 绑回环（默认 127.0.0.1）时可接受。对外暴露必须设值。
PREFECT_API_AUTH_STRING=
```

## 6. 校验清单（块 1 完整覆盖）

collect-all，全扫完再统一报。每项含「存在 + 类型 + 值域」三维：

- `task.toml`
  - 顶层 `[task]` 存在且为表
  - `task.name`、`task.db_path`、`task.seen_db` 存在
  - `task.window` 为表，且 `window.anchor`、`window.length` 存在
  - `task.deployments` 为表；`deployments.pipelines`（若有）为 `"all"` 或 list
- `sources/sinks/stages`：根为 list；每 entry 为表，`name`、`type` 存在（错误带索引）
- `pipelines`：根为 list；每 entry 为表，`name`、`stages` 存在；`concurrency_strategy`/`input_mode` 值合法；`max_attempts` 可转 int

样例：
```
ConfigError: invalid config in configs/ai_news:
  - task.toml: 'task.window' expected table, got str
  - sources[1]: missing 'type'
  - pipelines[0]: missing 'stages'
  - pipelines[1]: invalid concurrency_strategy 'foo' (must be queue/enqueue/cancel_new)
```

## 7. 错误处理

- 单一报错出口：`_validate_structure` 收集全部 → `load_task` 一次性 `raise ConfigError`；构造期残留错由 5.2 try 兜底包装。
- TOML 解析/编码/IO 错由 `_read_toml` 包装（F3）。
- 文件级缺失仍由现有 `FileNotFoundError` 处理。

## 8. 测试策略（TDD）

### 块 1（loader）失败用例先行
- 缺 `task.name` / 缺嵌套 `window.anchor`
- **类型错**：`task = "x"`、`window = "24h"`（标量非表）→ ConfigError 非 TypeError（F1）
- **形状错**：`sources = "bad"`、`sources = ["str_not_table"]` → ConfigError 非 AttributeError（F2）
- entry 缺 `type`（带索引）、pipeline 缺 `stages`
- 值域：非法 `concurrency_strategy`/`input_mode`、`max_attempts="x"`（F4）
- **TOML 语法错** → ConfigError 含文件名（F3）
- **多错聚合**：一次断言 message 同时含全部
- 回归：完整合法配置正常构建

### 块 2（部署）
- `env -i docker compose --env-file /dev/null config` 在变量未设时**失败**（F9：清环境避免 CI 假绿）
- 变量齐备时 `config` 通过；断言 ports 为 `127.0.0.1:4200:4200`
- 仅需 Compose CLI，无需 daemon

### 块 3（scaffold）
- `_toml_value(None)` / `_toml_value({...})` → ConfigError（F11）
- 含特殊字符的字符串仍正确转义（回归旧 #3）

### 块 4（badge）
- 无需测试，数字以最终 `pytest --cov` 为准

## 9. 文档 badge（块 4）

- `README.md` + `README_ZH.md`：`tests-408` → 实际数；`coverage-80%` → 实际值（修完重测取准）

## 10. 迁移影响

- loader/scaffold：纯加固，合法既有配置行为不变。
- 部署：依赖弱默认的用户会在 `compose up` 被强制设值，server 默认仅本机可达——**破坏性**，CHANGELOG 必须注明「升级后需设 `POSTGRES_*`，对外暴露需改 ports + 设 auth」。

## 11. 文件清单

| 文件 | 变更 |
|------|------|
| `sluice/dag/loader.py` | `_require*`/`_validate_structure`；`_read_toml` 包装；`load_task` 接入 + 构造兜底；`load_all_tasks` 加 warning |
| `sluice/dag/admin.py` | `_toml_value` 不支持类型抛 ConfigError |
| `tests/dag/test_loader.py` | 块 1 全部用例 |
| `tests/dag/test_admin.py` | `_toml_value` 防护用例 |
| `deploy/docker-compose.yml` | DB 凭证 `:?`；ports 绑 `127.0.0.1`；auth 注释 |
| `deploy/.env.example` | 留空必填密钥、补字符集与 auth 注释 |
| `tests/deploy/test_compose.py` | `compose config` 清环境校验 |
| `README.md` / `README_ZH.md` | badge 数字 |
| `CHANGELOG.md` | 记部署侧破坏性变更 |
