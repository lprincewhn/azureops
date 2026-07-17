# 每日出网流量费用日报（Azure Logic App · 低代码）

用 **Azure Logic App（Consumption）** 实现的定时任务：每天拉取「前一天」的出网流量费用，
计算与前一天的**环比差异**并按 `MeterSubCategory` 生成**分项明细表**，通过
**Azure Communication Services（ACS）Email** 连接器发送富文本（HTML）邮件。
无需写代码，无需邮箱账号。

## 工作流程

1. **Recurrence 触发**：默认每天 UTC 02:00（`scheduleHour` 可调）。
2. **查询成本**（3 次 Cost Management Query，托管标识鉴权）：
   - `Query_detail`：昨日 `MeterCategory=Bandwidth`，按 `MeterSubCategory` 分组的明细；
   - `Query_yesterday_total`：昨日合计；
   - `Query_prev_total`：前天合计（用于环比）。
   - HTTP 动作配置了指数退避重试，以应对 Cost API 429 限流。
3. **计算与拼装**：算出合计、货币、环比百分比（`▲ +x%` / `▼ -x%`，含除零保护），
   用 `Select`+`join` 拼出 HTML 明细表。
4. **发送邮件**：ACS Email 连接器（`acsemail`，密钥）发送富文本日报。

## 目录结构

```
egress-cost-daily/
├── DEPLOY-CLI.md                 # 纯 Azure CLI 部署手册（推荐）
├── azuredeploy.json              # 工作流 ARM 定义（参考；CLI 部署以 DEPLOY-CLI.md 为准）
└── azuredeploy.parameters.json   # 参数示例（发件人/收件人/计费类别/触发时刻）
```

## 部署

所有资源（ACS 三资源、`acsemail` 连接、Logic App 工作流）统一使用 **Azure CLI** 部署，
完整分步手册见 [`DEPLOY-CLI.md`](DEPLOY-CLI.md)：

1. 公共前置：登录、变量、资源组、CLI 扩展。
2. 创建共用 ACS（Email Service / 托管发件域 / Communication Service），取发件域与连接串。
3. 建 `acsemail` API 连接（密钥）。
4. `az logic workflow create` 建工作流（`--mi-system-assigned true` 开系统托管标识）。
5. 给工作流托管标识授予 `Cost Management Reader`（`az role assignment create`）。

发件域获取与收件人填写方式详见 `DEPLOY-CLI.md`（收件人多个用**分号 `;`** 分隔）。

## 关键参数（`azuredeploy.parameters.json` / 工作流参数）

| 名称 | 说明 |
|------|------|
| `emailSender` | ACS 发件地址，如 `DoNotReply@<发件域>.azurecomm.net`（由 ACS 托管域自动生成） |
| `emailRecipients` | 收件人，多个用分号 `;` 分隔 |
| `meterCategory` | 计费类别过滤，默认 `Bandwidth`（捕获出网流量） |
| `scheduleHour` | 每天触发的小时（UTC，0-23），默认 `2` |
| `acsConnectionString` | ACS 连接字符串（`endpoint=…;accesskey=…`），用于 `acsemail` 连接 |

## 说明与前提

- **鉴权**：Logic App 系统托管标识查成本，需订阅级 `Cost Management Reader`
  （部署后用 `az role assignment create` 授予，需执行者具备角色分配权限）。
- **发邮件**：ACS Email 连接器用密钥（连接串），无需邮箱账号或额外授权。
- **出网口径**：Azure 计费中出网流量归于 `MeterCategory=Bandwidth`
  （如 Data Transfer Out、Inter-Region）。如需更细口径，可调整 `meterCategory`。
- **成本数据延迟**：Cost Management 数据通常有数小时延迟，02:00 拉取前一天数据可覆盖。
- **ACS 邮件连接器 404**：`Send_email` 动作必须带 `api-version=2023-03-31` 查询参数（模板已含）。
