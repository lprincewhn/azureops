# AGENTS.md

本仓库（`azureops`）是一组相互独立的 **Azure 运维（AzureOps）工具集**，每个顶层目录为一个自成体系的子项目，覆盖成本报表、指标告警治理、慢查询日志分析等场景。本文档面向在此仓库中工作的 AI/自动化 Agent，说明结构、约定与验证方式。

## 仓库结构

```
azureops/
├── alert-policies/       # VM/MySQL/Redis 指标告警的 Azure Policy（DINE）定义与部署文档
├── amr-slowquery-log/    # Azure Managed Redis 慢查询日志采集 Exporter + Workbook
└── daily-cost-summary/    # 每日出网流量费用日报（Azure Logic App，低代码）
```

三个子项目**互不依赖**，可单独部署与验证。改动应限定在相关子目录内，勿跨子项目做无关修改。

## 子项目概览

### `alert-policies/` — 指标告警策略（纯 JSON + CLI）
- **内容**：三套 `DeployIfNotExists` 自定义策略，为 VM / MySQL 弹性服务器 / Azure Managed Redis 自动部署 `Microsoft.Insights/metricAlerts` 并绑定 Action Group。
- **文件**：每套策略含 `*.rules.json`（策略规则）与 `*.params.json`（参数定义）。部署说明见 `DEPLOYMENT.md`。
- **无构建/测试**：这是声明式配置，通过 `az policy definition/assignment create` 部署。修改后应校验 JSON 合法性并保持 rules/params 参数一致。
- **告警命名规则**：`<前缀>-<资源组名>-<资源名>`（前缀 `vm-` / `mysql-` / `redis-`）。

### `amr-slowquery-log/` — AMR 慢查询 Exporter（Python + K8s）
- **主程序**：`exporter.py`，轮询 Redis SLOWLOG 并上报 Azure Monitor Log Analytics（表 `AMRSlowQuery_CL`），同时追加本地 JSONL 备份。
- **多集群**：以 Kubernetes StatefulSet 管理，每个 Pod 按序号从 `clusters.json` 索引一个集群；配置见 `k8s/base/` 与 `k8s/overlays/template/`。
- **认证**：Redis 用 Access Key 或 Entra ID；上报 Log Analytics 用 AKS Workload Identity。
- **辅助**：`deploy-workbook.py` 部署 Azure Monitor Workbook；`Dockerfile` 基于 `python:3.12-slim`。
- **部署文档**：`readme.md` 与 `docs/deployment.md`。

### `daily-cost-summary/` — 出网费用日报（Logic App，低代码）
- **工作流定义**：Logic App JSON 定义为项目文件 `workflow-definition-egress-cost.json`（含 `${...}` 占位符），部署时用 `envsubst` 渲染，**不再内联在文档中**。
- **功能**：每日拉取前一天 `MeterCategory=Bandwidth` 成本，按**订阅（`SubscriptionName`）**出明细并计算环比，经 ACS Email 发送 HTML 日报。查询范围由 `COST_SCOPE` 决定（管理组可跨订阅出明细，或单订阅）。
- **文档**：`README.md`（概览）+ `DEPLOY-CLI.md`（分步手册，2.2 用 envsubst 渲染定义文件）。

## 开发与验证

### `amr-slowquery-log`（唯一含代码与测试的子项目）
在该子目录下操作：

```bash
cd amr-slowquery-log

# 安装依赖（含测试依赖）
pip install -r requirements-test.txt

# 运行单元测试（默认跳过需真实 Redis 的 live 测试）
pytest

# 仅跑非 live 测试（显式）
pytest -m "not live"

# 本地直接运行 Exporter（读取 .env，参考 .env.example）
python exporter.py
```

- 测试位于 `tests/`，用例编号见 `tests/test_exporter.py` 顶部注释（TC-01~TC-49）。
- 标记 `live` 的测试需真实 Azure Managed Redis 实例，缺少凭据时自动跳过（见 `pytest.ini`）。
- 本地运行需 `.env`：复制 `.env.example` 后填写；K8s 部署时集群参数改由 `clusters-config.yaml` 提供。

### `alert-policies` / `daily-cost-summary`
无自动化测试/构建。改动 JSON 或 CLI 文档后，至少校验 JSON 合法性（如 `python -m json.tool <file>`）并确保文档命令自洽。

## 约定

- **语言**：面向用户的文档为中文；代码、注释、日志、标识符为英文。保持这一风格。
- **改动范围**：只修改与任务直接相关的文件；勿跨子项目改动无关内容。
- **文档同步**：修改 `exporter.py` 的配置项、字段或 K8s 清单时，同步更新对应 `readme.md` / `docs/deployment.md` / `.env.example`。
- **密钥安全**：切勿提交任何真实的 Access Key、连接串、订阅 ID、UAMI/租户 ID 等；占位符沿用 `<...>` 形式。
- **图片资源**：`docs/*.png|jpg` 为文档配图，勿删除或重命名，README 通过绝对 raw 链接引用。

## 提交规范

- 提交信息使用清晰的祈使句，注明所影响的子项目（如 `amr-slowquery-log: fix OSS shard dedup`）。
- 一次提交尽量只涉及一个子项目。
