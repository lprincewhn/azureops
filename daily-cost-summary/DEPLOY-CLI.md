# 每日出网流量费用日报 · 部署手册（Azure CLI 版）

本文提供 **Logic App（低代码）+ ACS 邮件** 方案的完整部署步骤，所有资源均使用 **Azure CLI** 创建（不使用 Bicep/ARM 建资源）。

可视化工作流，无需写代码、无需邮箱账号：每天拉取前一天出网费用，计算环比差异并**按订阅
（`SubscriptionName`）**生成明细表，通过 **ACS（Azure Communication Services）Email** 连接器发信，
靠**系统托管标识**查成本。

> 工作流的 JSON 定义已作为项目文件保存在 [`workflow-definition-egress-cost.json`](workflow-definition-egress-cost.json)，
> 本手册在 2.2 用 `envsubst` 将其中的占位符渲染成实际值，不再内联生成定义。

---

## 0. 公共前置

```bash
# 登录并选择订阅
az login
az account set --subscription <SUBSCRIPTION_ID>

# 公共变量（按需修改）
SUB=$(az account show --query id -o tsv)
RG=rg-daily-cost-summary
LOCATION=southeastasia
METER_CATEGORY=Bandwidth          # 出网流量归属的计费类别
RECIPIENTS="you@example.com"      # 收件人邮箱，多个用分号(;)分隔（在 2.2 填入 LA_RECIPIENTS）

# 成本查询范围：明细「按订阅」区分，需覆盖多个订阅时用管理组；也可用单订阅
#   管理组（跨订阅按订阅出明细）： providers/Microsoft.Management/managementGroups/<MG_ID>
#   单订阅（明细仅一行本订阅）：     subscriptions/$SUB
COST_SCOPE="providers/Microsoft.Management/managementGroups/<MG_ID>"

# 创建资源组
az group create -n $RG -l $LOCATION
```

> 需要的 CLI 扩展（首次执行会自动提示安装，也可手动装）：
> ```bash
> az extension add --name communication   # ACS
> az extension add --name logic           # Logic App workflow
> ```
>
> 渲染工作流定义用到 `envsubst`（来自 `gettext`，Cloud Shell 已内置；本地缺失可 `apt-get install gettext-base`）。

---

## 1. 创建 ACS 邮件资源

ACS 邮件由 3 个资源组成：Email Service → 托管发件域（AzureManagedDomain）→ Communication Service（关联发件域）。
Email 服务用 `az resource create` 创建；发件域用 `az communication email domain` 专用命令；Communication Service 用 `az communication create`。

```bash
ACS_NAME=egresscost-acs
EMAIL_SVC=egresscost-email
DATA_LOCATION="United States"     # ACS 数据驻留地

# 1a. Email Communication Service（location 固定 global）
az resource create \
  -g $RG --name $EMAIL_SVC \
  --resource-type Microsoft.Communication/emailServices \
  --api-version 2023-04-01 \
  --location global \
  --properties "{\"dataLocation\":\"$DATA_LOCATION\"}"

# 1b. Azure 托管发件域（用 ACS 专用命令；az resource create 对该子资源类型解析会报错）
az communication email domain create \
  -g $RG --email-service-name $EMAIL_SVC \
  --name AzureManagedDomain --location global \
  --domain-management AzureManaged --user-engmnt-tracking Disabled

# 取回发件域，拼出发件地址
SENDER_DOMAIN=$(az communication email domain show \
  -g $RG --email-service-name $EMAIL_SVC --name AzureManagedDomain \
  --query fromSenderDomain -o tsv)
EMAIL_SENDER="DoNotReply@${SENDER_DOMAIN}"
echo "发件人: $EMAIL_SENDER"

DOMAIN_ID=$(az communication email domain show \
  -g $RG --email-service-name $EMAIL_SVC --name AzureManagedDomain \
  --query id -o tsv)

# 1c. Communication Service，并关联发件域
az communication create \
  -g $RG --name $ACS_NAME \
  --location global --data-location "$DATA_LOCATION"

az resource update \
  -g $RG --name $ACS_NAME \
  --resource-type Microsoft.Communication/communicationServices \
  --api-version 2023-04-01 \
  --set properties.linkedDomains="['$DOMAIN_ID']"

# 取回 ACS 连接字符串（发邮件用）
ACS_CONN=$(az communication list-key -g $RG --name $ACS_NAME \
  --query primaryConnectionString -o tsv)
```

> **发件域与收件人从哪来？**
> - **发件人 `$EMAIL_SENDER`**：由步骤 1b 查询托管发件域 `fromSenderDomain` 自动拼成
>   `DoNotReply@<发件域>.azurecomm.net`，无需手填。若已部署过 ACS，可单独查回：
>   ```bash
>   az communication email domain show -g $RG \
>     --email-service-name $EMAIL_SVC --name AzureManagedDomain \
>     --query fromSenderDomain -o tsv
>   ```
> - **收件人**：在下方 2.2 的 `LA_RECIPIENTS` 填真实邮箱，多个用**分号 `;`** 分隔。

---

## 2. Logic App 工作流部署

Logic App 工作流本身是一段 JSON 定义（项目文件 [`workflow-definition-egress-cost.json`](workflow-definition-egress-cost.json)）。
这里用 **az cli** 建资源：先建 `acsemail` API 连接，再用 `az logic workflow create` 建工作流。

该工作流每天拉取「前一天」出网费用，并额外产出：
- **分项明细**：在 `COST_SCOPE` 范围内按订阅（`SubscriptionName`）分组，拼成 HTML 表格；
- **环比差异**：与「前两天」合计对比，计算百分比升降（`▲ +x%` / `▼ -x%`）。
共 3 个成本查询（昨日明细 / 昨日合计 / 前天合计），HTTP 动作已配置指数退避重试以应对 Cost API 429 限流。

## 2.1 创建 ACS Email API 连接（`acsemail`，密钥，无需邮箱账号）

```bash
CONN_NAME=egress-cost-logic-acsemail
API_ID="/subscriptions/$SUB/providers/Microsoft.Web/locations/$LOCATION/managedApis/acsemail"

az resource create \
  -g $RG --name $CONN_NAME \
  --resource-type Microsoft.Web/connections \
  --api-version 2016-06-01 \
  --location $LOCATION \
  --properties "{\"displayName\":\"acsemail\",\"api\":{\"id\":\"$API_ID\"},\"parameterValues\":{\"api_key\":\"$ACS_CONN\"}}"

CONN_ID=$(az resource show -g $RG --name $CONN_NAME \
  --resource-type Microsoft.Web/connections --api-version 2016-06-01 --query id -o tsv)
```

## 2.2 渲染工作流定义文件

工作流定义保存在项目文件 [`workflow-definition-egress-cost.json`](workflow-definition-egress-cost.json)（含 `$connections` 绑定），
其中 `${...}` 为占位符。用 `envsubst` 把前面步骤准备好的变量填入，生成可部署的 `/tmp/workflow-def.json`
（Logic App 收件人用**分号**分隔）：

```bash
LOGIC_APP=egress-cost-logic
LA_RECIPIENTS="you@example.com"          # 多个用分号(;)分隔

# 导出占位符对应的变量（COST_SCOPE 见 0. 公共前置；EMAIL_SENDER/CONN_ID/CONN_NAME/API_ID 见步骤 1、2.1）
export COST_SCOPE METER_CATEGORY EMAIL_SENDER LA_RECIPIENTS CONN_ID CONN_NAME API_ID

# 只替换列出的变量，保留定义中的 $schema / $connections 与 @{...} 表达式
envsubst '${COST_SCOPE} ${METER_CATEGORY} ${EMAIL_SENDER} ${LA_RECIPIENTS} ${CONN_ID} ${CONN_NAME} ${API_ID}' \
  < workflow-definition-egress-cost.json > /tmp/workflow-def.json
```

> **明细按订阅区分**：`Query_detail` 按 `SubscriptionName` 分组。要让明细出现多行，`COST_SCOPE`
> 需为覆盖多个订阅的**管理组**（`providers/Microsoft.Management/managementGroups/<MG_ID>`）；
> 若设为单订阅（`subscriptions/$SUB`），明细仅有该订阅一行。

> `api-version=2023-03-31` 是 `Send_email` 必需的查询参数；缺失会导致连接器返回 `404 Resource not found`。

## 2.3 创建 Logic App 工作流（az cli）

`az logic workflow create` 的 `--definition` 需要**完整的工作流属性对象**（即包含 `definition`
与 `parameters` 两个键，正是 2.2 生成的 `workflow-def.json`），并用 `--mi-system-assigned true`
一并开启系统托管标识（该命令没有独立的 `--parameters` 参数）：

```bash
az logic workflow create \
  -g $RG --name $LOGIC_APP --location $LOCATION \
  --mi-system-assigned true \
  --definition @/tmp/workflow-def.json
```

## 2.4 授予托管标识查成本的角色

在成本查询范围 `COST_SCOPE` 上授予角色（管理组范围可覆盖其下所有订阅，明细才能按订阅列出多行）：

```bash
LA_PRINCIPAL=$(az resource show -g $RG --name $LOGIC_APP \
  --resource-type Microsoft.Logic/workflows --api-version 2019-05-01 \
  --query identity.principalId -o tsv)

az role assignment create \
  --assignee-object-id $LA_PRINCIPAL \
  --assignee-principal-type ServicePrincipal \
  --role "Cost Management Reader" \
  --scope "/$COST_SCOPE"
```

## 2.5 手动触发验证（Recurrence 触发器）

```bash
# 手动触发一次（Recurrence 触发器用 run API）
az rest --method post --url \
  "https://management.azure.com/subscriptions/$SUB/resourceGroups/$RG/providers/Microsoft.Logic/workflows/$LOGIC_APP/triggers/Recurrence/run?api-version=2019-05-01"

# 查看最近一次运行状态
az rest --method get --url \
  "https://management.azure.com/subscriptions/$SUB/resourceGroups/$RG/providers/Microsoft.Logic/workflows/$LOGIC_APP/runs?api-version=2019-05-01&\$top=1" \
  --query "value[0].properties.status" -o tsv
```

工作流每天 **UTC 02:00** 自动运行：查前一天出网费用 → 计算真实金额 → 通过 ACS Email 发信。

验证成功后收到的日报邮件效果如下（含合计、环比差异、分项明细表）：

![出网流量费用日报邮件效果](docs/email-sample.jpg)

## 2.6 更新工作流（`workflow-definition-egress-cost.json` 变更后）

修改了 `workflow-definition-egress-cost.json`（或调整了 `COST_SCOPE` / `METER_CATEGORY` / 收件人等变量）后，
**重新渲染并下发**即可，无需重建资源，也不影响已有的托管标识与角色授权：

```bash
# 1. 重新导出变量（同 2.2；若同一 shell 会话已 export 可跳过）
export COST_SCOPE METER_CATEGORY EMAIL_SENDER LA_RECIPIENTS CONN_ID CONN_NAME API_ID

# 2. 重新渲染定义文件
envsubst '${COST_SCOPE} ${METER_CATEGORY} ${EMAIL_SENDER} ${LA_RECIPIENTS} ${CONN_ID} ${CONN_NAME} ${API_ID}' \
  < workflow-definition-egress-cost.json > /tmp/workflow-def.json

# 3. 下发更新（az logic workflow create 为 upsert：同名工作流会被整体覆盖更新）
az logic workflow create \
  -g $RG --name $LOGIC_APP --location $LOCATION \
  --mi-system-assigned true \
  --definition @/tmp/workflow-def.json
```

> - `az logic workflow create` 对已存在的工作流是**幂等覆盖**（upsert），因此更新与初次创建用同一条命令；
>   已开启的系统托管标识与其 `Cost Management Reader` 角色分配会保留，无需重复 2.4。
> - 只想改**收件人 / 发件人**时也走同样流程（改 `LA_RECIPIENTS` / `EMAIL_SENDER` 后重渲染下发）。
> - 更新后可用 2.5 的手动触发命令验证新定义是否生效。

---

## 常见问题与要点

- **Cost Management 限流（429）**：反复手动测试易触发限流；工作流的 3 个 HTTP 查询已配置指数退避重试（`count=4, PT20S`）。生产每天仅触发 1 次，无碍。
- **ACS 邮件连接器 404**：`Send_email` 必须带 `api-version=2023-03-31` 查询参数（模板已含）。
- **环比除零保护**：前一天费用为 0 时显示「无法计算百分比」，不会报错。
- **明细按订阅**：`Query_detail` 按 `SubscriptionName` 分组；`COST_SCOPE` 设为管理组时明细覆盖其下各订阅，设为单订阅时仅一行。
- **出网口径**：Azure 出网流量计费归属 `MeterCategory=Bandwidth`；如需更细口径可改用 `MeterSubCategory` 过滤或调整 `meterCategory`。
- **成本数据延迟**：Cost Management 数据通常有数小时延迟，02:00 拉前一天数据可覆盖。
- **角色分配权限**：`az role assignment create` 需执行者在 `COST_SCOPE` 范围具备 `Microsoft.Authorization/roleAssignments/write`（Owner / User Access Administrator；管理组范围需在该管理组授权）。
