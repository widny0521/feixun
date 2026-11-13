# Feixun（飞讯）

Feixun 是一个面向个人开发者的 AI 聊天与远程开发系统。目前项目处于第一阶段，聚焦可扩展的任务协议、Windows Agent 运行时、Provider Registry，以及安全的 SQLite 任务持久化。

## 当前状态

已完成第一阶段核心基础设施：

- 统一任务请求、结果、事件和状态模型
- Provider Registry 与 OMP Adapter 接口
- Windows 子进程执行、超时和取消处理
- SQLite 持久化、幂等、事件日志、恢复与备份
- 项目路径授权边界和高风险命令检查

聊天界面、手机推送和更完整的多模型工作流仍在规划中，尚未作为可用功能发布。

## 技术架构

- Python 3.13+
- Pydantic 数据模型
- SQLite 持久化
- Windows Agent 运行时
- pytest 测试，Ruff 代码检查

代码按职责划分为 `src/agent_protocol`、`src/agent_runtime` 和 `src/storage`。设计记录见 [`docs/superpowers/plans/2026-11-12-personal-ai-hub-phase1.md`](docs/superpowers/plans/2026-11-12-personal-ai-hub-phase1.md)。

## 本地开发

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .
Copy-Item .env.example .env
python -m pytest -q
python -m ruff check .
```

当前仓库主要提供可复用的协议、运行时和存储模块；完整应用入口与前端仍在开发中。

## 路线图

1. 完善 Agent 服务入口和配置管理
2. 增加聊天与任务编排 API
3. 开发 Web/PWA 界面与通知能力
4. 扩展 Provider、工具和记忆模块

## 贡献

欢迎提交 Issue 和 Pull Request。请先阅读 [CONTRIBUTING.md](CONTRIBUTING.md)，并确保测试及 Ruff 检查通过。

## 许可证

本项目采用 [MIT License](LICENSE)。
