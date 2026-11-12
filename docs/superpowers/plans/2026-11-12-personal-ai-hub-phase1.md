# Personal AI Hub V2.0 第一阶段实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 完成 OMP/模型可用性验证，并在验证通过后实现可取消、可超时、可持久化的 Windows 本机任务执行器最小版本。

**Architecture:** FastAPI 后端与 Windows Agent 通过统一任务协议交互；Agent 通过 Provider Registry 解析逻辑角色，再由 OMP Adapter 启动外部进程。聊天、开发任务、工具、记忆和扩展模块使用隔离的数据边界与统一权限接口，第一阶段只实现任务执行所需子集。

**Tech Stack:** Python 3.13、FastAPI、Pydantic、SQLite、SQLAlchemy、pytest、Ruff、PowerShell、Git；不新增第三方依赖，直到验证确有需要。

**Spec:** 用户提供的《项目启动指令：个人 AI 聊天与远程开发系统 V2.0》及本仓库根目录 AGENTS.md（当前未发现项目级文件）。

## Global Constraints

- 实际代码编写、修改、测试优先交给 OMP 的 DeepSeek；OMP 不可用时停止实际开发。
- 模型名称只能出现在配置/适配器层，工作流使用逻辑角色。
- 单 AI 与多 AI 共用任务、日志、权限、项目和 Git 记录模块。
- 任务必须支持唯一 ID、幂等键、持久化状态、超时、取消、失败记录和异常恢复。
- 不得覆盖用户已有修改；API 密钥不得硬编码或提交。
- 未来扩展通过 Provider Registry、Tool Registry、Plugin、Memory、Event、Identity 和 Extension API 接入，第一版不实现未来功能。

## Review Focus

- OMP 原生扩展缺失或权限异常时，必须返回可诊断错误且不回退到 Codex 编码。
- 子进程超时/取消/异常退出时，任务状态、退出码和日志必须一致。
- 重复幂等键不得重复执行。
- 未授权项目路径和高风险命令必须在执行前拒绝。
- 任务/聊天数据必须隔离，迁移和备份不能破坏已有记录。

### Task 0: OMP 环境恢复与验证（当前阻断）

**Files:**
- Read: `%USERPROFILE%\\.omp\\agent\\config.yml`
- Read: `%USERPROFILE%\\.omp\\agent\\agents\\reviewer.md`
- Create: `docs/omp-validation-report.md`

**Interfaces:**
- Produces: 可复现的 OMP 命令、退出码、stdout/stderr、模型/角色结果和阻断原因。

- [ ] 记录 `python/node/git/omp/codex` 版本与路径。
- [ ] 修复或升级 OMP 原生扩展加载问题；不得绕过错误切换编码模型。
- [ ] 用 DeepSeek 完成无副作用文本测试，保存完整输出、退出状态和错误信息。
- [ ] 用隔离测试目录完成一次最小代码修改、测试和 Git diff 获取。
- [ ] 用 Codex `@slow` Reviewer 审查该 diff；移除 reviewer 的 DeepSeek 覆盖配置或明确记录例外。
- [ ] 运行 `git diff --check`，把结果写入验证报告。

**验收:** 同一 PowerShell 命令可重复得到成功退出码；DeepSeek 修改、测试、diff 和错误路径均可观测；Reviewer 实际使用 Codex 角色。

### Task 1: 执行协议与状态模型

**Files:**
- Create: `src/agent_protocol/models.py`
- Create: `src/agent_protocol/status.py`
- Test: `tests/test_protocol.py`

**Interfaces:**
- `TaskRequest(task_id, idempotency_key, project_path, instruction, timeout_seconds)`
- `TaskResult(task_id, status, exit_code, stdout, stderr, diff, error_code)`
- 状态枚举：`queued/running/succeeded/failed/cancelled/timed_out`。

- [ ] 先写状态转换、非法请求、幂等键校验测试。
- [ ] 用 Pydantic 实现请求/结果模型和序列化。
- [ ] 运行 `pytest tests/test_protocol.py -q` 与 `ruff check src tests`。

### Task 2: OMP Adapter 与进程生命周期

**Files:**
- Create: `src/agent_runtime/adapter.py`
- Create: `src/agent_runtime/process.py`
- Test: `tests/test_runtime.py`

**Interfaces:**
- `ProviderAdapter.run(request, on_event) -> TaskResult`
- `ProcessRunner.run(argv, cwd, timeout, cancel_event) -> ProcessResult`

- [ ] 先写成功、无效任务、超时、取消、异常退出、日志完整性测试。
- [ ] 通过配置解析逻辑角色，不在代码中硬编码模型名。
- [ ] 使用 Windows 进程组终止策略，保留 stdout/stderr 和退出码。
- [ ] 运行完整测试并检查 diff。

### Task 3: 持久化、幂等与项目边界

**Files:**
- Create: `src/storage/database.py`
- Create: `src/storage/task_store.py`
- Create: `tests/test_storage.py`

**Interfaces:**
- `TaskStore.create_or_get(request) -> TaskRecord`
- `TaskStore.append_event(task_id, event) -> None`
- `TaskStore.complete(task_id, result) -> None`

- [ ] 测试重复请求去重、状态持久化、恢复和项目路径拒绝。
- [ ] SQLite schema 预留 owner/agent/provider/tool/plugin 关联字段，但不实现未使用功能。
- [ ] 提供备份接口与迁移版本号。

### Task 4: 最小集成验收

**Files:**
- Create: `src/agent_runtime/service.py`
- Create: `tests/test_integration.py`
- Modify: `README.md`
- Create: `docs/development-log.md`

- [ ] 在隔离测试项目执行一次真实 DeepSeek 代码修改与测试。
- [ ] 获取执行日志、结果、Git diff 和审查结果。
- [ ] 运行 `pytest -q`、`ruff check .`、`git diff --check`。
- [ ] 由 Codex Reviewer 做一次关键节点审查，按反馈最多交给 DeepSeek 修复两轮。

## 第一批文件清单

仅在 Task 0 通过后创建：`src/agent_protocol/models.py`、`src/agent_protocol/status.py`、`src/agent_runtime/adapter.py`、`src/agent_runtime/process.py`、`tests/test_protocol.py`、`tests/test_runtime.py`、`docs/omp-validation-report.md`。数据库和集成文件延后到前两项验收通过后创建。
