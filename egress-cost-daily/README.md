# 每日出网流量费用采集（Azure Functions · Python）

> 说明：**Azure Logic App 不支持 Python 运行时**（Logic App 是可视化工作流）。
> Python 定时任务在 Azure 上对应 **Azure Functions**，本项目即以 Python Function
> （Timer 触发）实现你的需求：每天拉取「前一天」的出网流量费用并发送到 Action Group。

## 工作流程

1. **Timer 触发**：默认每天 UTC 02:00（`SCHEDULE` = `0 0 2 * * *`）。
2. **查询成本**：调用 Cost Management Query API，取前一天 `MeterCategory=Bandwidth`
   （出网流量）的 `PreTaxCost`，按 `MeterSubCategory` 分组汇总。
3. **发送通知**：
   - **优先(推荐)**：若配置 `ACS_CONNECTION_STRING` + `EMAIL_SENDER` + `EMAIL_RECIPIENTS`，
     通过 **Azure Communication Services** 直接发送邮件,正文含**真实金额与分项明细**(HTML 表格)。
   - 若配置 `ACTION_GROUP_WEBHOOK_URL`：直接 POST 富文本 JSON 报告（Teams/Slack/Logic App）。
   - 若配置 `ACTION_GROUP_ID`：通过 `createNotifications` API 触发 Action Group(注意:测试
     通知模板**不含**真实金额,仅验证通道连通)。

## 目录结构

```
egress-cost-daily/
├── function_app.py        # Python v2 编程模型，Timer 触发主逻辑
├── requirements.txt
├── host.json
├── local.settings.json    # 本地调试配置（勿提交真实密钥）
├── infra/
│   ├── main.bicep         # Function App + 存储 + App Insights + ACS(邮件)
│   └── roles.bicep        # 订阅级角色授权（托管标识）
└── logicapp/              # 备选实现：纯低代码 Logic App（无 Python/无邮箱账号）
    ├── azuredeploy.json           # Recurrence -> HTTP(Cost, MI) -> ACS Email 发邮件
    └── azuredeploy.parameters.json
```

## 两种实现对比

| | **Function + ACS**（当前默认） | **Logic App + ACS**（`logicapp/`） |
|---|---|---|
| 运行时 | Python | 低代码工作流(无代码) |
| 发邮件 | ACS SDK | ACS Email 连接器(`acsemail`,密钥) |
| 是否需要 ACS | 是 | 是(复用同一个 ACS) |
| 发件人 | `DoNotReply@…azurecomm.net` | `DoNotReply@…azurecomm.net` |
| 鉴权 | 托管标识查成本 | 托管标识查成本 + ACS 连接串(密钥) |
| 需授予角色 | Cost Management Reader | Cost Management Reader |
| 额外手动步骤 | 无 | **无**(连接用密钥,部署即配好) |

### Logic App 部署

```bash
CS=$(az communication list-key --name <acs名> -g <rg> --query primaryConnectionString -o tsv)
az deployment group create -g <rg> \
  --template-file logicapp/azuredeploy.json \
  --parameters logicapp/azuredeploy.parameters.json \
  --parameters acsConnectionString="$CS"
```
部署后仅需一步：给输出的 `principalId` 授予 `Cost Management Reader`。

> **不想用 Bicep/ARM？** 两套方案的**纯 Azure CLI** 逐步部署手册见
> [`DEPLOY-CLI.md`](DEPLOY-CLI.md)（用 `az cli` 创建全部基础设施资源）。

## 关键配置（App Settings / 环境变量）

| 名称 | 说明 |
|------|------|
| `SUBSCRIPTION_ID` | 要统计费用的订阅 ID |
| `ACS_CONNECTION_STRING` | Azure Communication Services 连接字符串(直接发邮件) |
| `EMAIL_SENDER` | 发件地址,如 `DoNotReply@<域>.azurecomm.net` |
| `EMAIL_RECIPIENTS` | 收件人,多个用逗号分隔 |
| `ACTION_GROUP_ID` | (可选) Action Group 资源 ID,走测试通知(不含金额) |
| `ACTION_GROUP_WEBHOOK_URL` | (可选) 直接推送富文本报告的 Webhook |
| `METER_CATEGORY` | 计费类别过滤，默认 `Bandwidth` |
| `SCHEDULE` | NCRONTAB 表达式，默认 `0 0 2 * * *` |

## 部署步骤

> 两种方式二选一：
> - **Bicep/ARM**（本节）：一条 `az deployment` 命令建全部资源。
> - **纯 Azure CLI**：逐步用 `az cli` 创建资源，见 [`DEPLOY-CLI.md`](DEPLOY-CLI.md)。

```bash
# 1. 部署 Function App 基础设施
az deployment group create \
  --resource-group <rg> \
  --template-file infra/main.bicep \
  --parameters functionAppName=<name> \
               actionGroupId=/subscriptions/<sub>/resourceGroups/<rg>/providers/microsoft.insights/actionGroups/<ag>

# 2. 记录输出的 principalId，授予订阅级角色
az deployment sub create \
  --location <region> \
  --template-file infra/roles.bicep \
  --parameters principalId=<principalId>

# 3. 发布函数代码
cd egress-cost-daily
func azure functionapp publish <name>
```

## 本地调试

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# 填好 local.settings.json 后
func start
```

## 说明与前提

- **鉴权**：使用 Function App 系统托管标识（`DefaultAzureCredential`），需
  `Cost Management Reader` + `Monitoring Contributor` 角色（roles.bicep 已包含）。
- **出网口径**：Azure 计费中出网流量归于 `MeterCategory=Bandwidth`
  （如 Data Transfer Out、Inter-Region）。如需更细口径，可改用
  `MeterSubCategory` 过滤或调整 `METER_CATEGORY`。
- **成本数据延迟**：Cost Management 数据通常有数小时延迟，02:00 拉取前一天数据可覆盖。
- `createNotifications` 为 Action Group 测试通知 API，会真实投递；若需在邮件正文中
  包含完整费用明细，建议给 Action Group 配置 Webhook 接收方并使用
  `ACTION_GROUP_WEBHOOK_URL`。
