# 每日出网流量费用日报 · 部署手册（Azure CLI 版）

本文提供**两套方案**的完整部署步骤，基础设施资源全部使用 **Azure CLI** 创建（不使用 Bicep/ARM 建资源）。

- **方案 A：Azure Functions（Python）+ ACS 邮件** —— 代码定时任务，正文含真实金额与分项明细。
- **方案 B：Logic App（低代码）+ ACS 邮件** —— 可视化工作流，无需写代码、无需邮箱账号。

两套方案**共用同一个 ACS（Azure Communication Services）资源**发邮件，都靠**系统托管标识**查成本。

---

## 0. 公共前置

```bash
# 登录并选择订阅
az login
az account set --subscription <SUBSCRIPTION_ID>

# 公共变量（按需修改）
SUB=$(az account show --query id -o tsv)
RG=rg-egress-cost-daily
LOCATION=southeastasia
METER_CATEGORY=Bandwidth          # 出网流量归属的计费类别
RECIPIENTS="you@example.com"      # 收件人，多个用逗号分隔（Logic App 用分号，见下文）

# 创建资源组
az group create -n $RG -l $LOCATION
```

> 需要的 CLI 扩展（首次执行会自动提示安装，也可手动装）：
> ```bash
> az extension add --name communication   # ACS
> az extension add --name logic           # 方案 B 的 Logic App workflow
> ```

---

## 1. 创建共用的 ACS 邮件资源（两套方案都需要）

ACS 邮件由 3 个资源组成：Email Service → 托管发件域（AzureManagedDomain）→ Communication Service（关联发件域）。
Email 服务和发件域只有 ARM 资源类型，用 `az resource create` 创建；Communication Service 用 `az communication create`。

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

# 1b. Azure 托管发件域
az resource create \
  -g $RG --name "$EMAIL_SVC/AzureManagedDomain" \
  --resource-type Microsoft.Communication/emailServices/domains \
  --api-version 2023-04-01 \
  --location global \
  --properties '{"domainManagement":"AzureManaged","userEngagementTracking":"Disabled"}'

# 取回发件域，拼出发件地址
SENDER_DOMAIN=$(az resource show \
  -g $RG --name "$EMAIL_SVC/AzureManagedDomain" \
  --resource-type Microsoft.Communication/emailServices/domains \
  --api-version 2023-04-01 \
  --query properties.fromSenderDomain -o tsv)
EMAIL_SENDER="DoNotReply@${SENDER_DOMAIN}"
echo "发件人: $EMAIL_SENDER"

DOMAIN_ID=$(az resource show \
  -g $RG --name "$EMAIL_SVC/AzureManagedDomain" \
  --resource-type Microsoft.Communication/emailServices/domains \
  --api-version 2023-04-01 --query id -o tsv)

# 1c. Communication Service，并关联发件域
az communication create \
  -g $RG --name $ACS_NAME \
  --location global --data-location "$DATA_LOCATION"

az resource update \
  -g $RG --name $ACS_NAME \
  --resource-type Microsoft.Communication/communicationServices \
  --api-version 2023-04-01 \
  --set properties.linkedDomains="['$DOMAIN_ID']"

# 取回 ACS 连接字符串（两套方案发邮件都要用）
ACS_CONN=$(az communication list-key -g $RG --name $ACS_NAME \
  --query primaryConnectionString -o tsv)
```

> **发件域与收件人从哪来？**
> - **发件人 `$EMAIL_SENDER`**：由步骤 1b 查询托管发件域 `properties.fromSenderDomain` 自动拼成
>   `DoNotReply@<发件域>.azurecomm.net`，无需手填。若已部署过 ACS，可单独查回：
>   ```bash
>   az resource show -g $RG --name "$EMAIL_SVC/AzureManagedDomain" \
>     --resource-type Microsoft.Communication/emailServices/domains \
>     --api-version 2023-04-01 --query properties.fromSenderDomain -o tsv
>   ```
> - **收件人 `$RECIPIENTS`**：在第 0 节按需修改成真实邮箱。
>   **方案 A（Function）多个用逗号 `,`；方案 B（Logic App）多个用分号 `;`**。

---

# 方案 A：Azure Functions（Python）+ ACS

## A1. 创建基础设施（az cli）

```bash
FUNC_APP=func-egresscost-$RANDOM        # 全局唯一
STORAGE=stegresscost$RANDOM             # 3-24 位小写字母数字，全局唯一
AI_NAME=${FUNC_APP}-ai

# 存储账户（Functions 宿主必需：代码包/Timer 锁/运行时协调，不存业务数据）
az storage account create \
  -g $RG -n $STORAGE -l $LOCATION \
  --sku Standard_LRS --kind StorageV2 --min-tls-version TLS1_2

# Application Insights
az monitor app-insights component create \
  -g $RG --app $AI_NAME -l $LOCATION --application-type web
AI_CONN=$(az monitor app-insights component show \
  -g $RG --app $AI_NAME --query connectionString -o tsv)

# Function App（Linux 消费计划 + Python 3.11 + 系统托管标识）
az functionapp create \
  -g $RG -n $FUNC_APP \
  --storage-account $STORAGE \
  --consumption-plan-location $LOCATION \
  --runtime python --runtime-version 3.11 \
  --functions-version 4 \
  --os-type Linux \
  --app-insights $AI_NAME \
  --assign-identity '[system]'
```

> ⚠️ **存储账户策略注意**：若订阅有策略强制 `allowSharedKeyAccess=false` 或 `publicNetworkAccess=Disabled`，
> 新建存储账户无法作为 Functions 宿主存储。此时改用一个**允许共享密钥+公网**的已有存储账户，
> 用其连接串覆盖 `AzureWebJobsStorage`（见下）。

## A2. 配置应用设置

```bash
az functionapp config appsettings set -g $RG -n $FUNC_APP --settings \
  AzureWebJobsFeatureFlags=EnableWorkerIndexing \
  SCM_DO_BUILD_DURING_DEPLOYMENT=true \
  ENABLE_ORYX_BUILD=true \
  SUBSCRIPTION_ID="$SUB" \
  METER_CATEGORY="$METER_CATEGORY" \
  SCHEDULE="0 0 2 * * *" \
  ACS_CONNECTION_STRING="$ACS_CONN" \
  EMAIL_SENDER="$EMAIL_SENDER" \
  EMAIL_RECIPIENTS="$RECIPIENTS" \
  APPLICATIONINSIGHTS_CONNECTION_STRING="$AI_CONN"

# 如需改用已有存储账户绕过策略：
# EXIST_CONN=$(az storage account show-connection-string -g <rg> -n <storage> --query connectionString -o tsv)
# az functionapp config appsettings set -g $RG -n $FUNC_APP --settings AzureWebJobsStorage="$EXIST_CONN"
```

## A3. 授予托管标识查成本的角色

```bash
PRINCIPAL_ID=$(az functionapp identity show -g $RG -n $FUNC_APP --query principalId -o tsv)

# 订阅级 Cost Management Reader（需执行者具备角色分配权限）
az role assignment create \
  --assignee-object-id $PRINCIPAL_ID \
  --assignee-principal-type ServicePrincipal \
  --role "Cost Management Reader" \
  --scope "/subscriptions/$SUB"
```

## A4. 发布函数代码（远程构建装依赖）

```bash
cd /home/azureuser/copilot/egress-cost-daily
func azure functionapp publish $FUNC_APP --build remote
```

> Python v2 编程模型在 Linux 消费计划上依赖 `EnableWorkerIndexing`（A2 已设），
> 且**必须远程构建**（`--build remote`）才会安装 `requirements.txt`，否则运行时报 `No module named 'requests'`。

## A5. 验证

```bash
# 查看函数已索引
az functionapp function list -g $RG -n $FUNC_APP -o table
# 触发日志 / App Insights 里应看到成本查询与 ACS 邮件发送成功
```

Timer 每天 **UTC 02:00** 自动运行，向收件人发送含真实金额与明细表的 HTML 邮件。

---

# 方案 B：Logic App（低代码）+ ACS

Logic App 工作流本身是一段 JSON 定义（本仓库 `logicapp/azuredeploy.json` 内含）。这里用 **az cli** 建资源：
先建 `acsemail` API 连接，再用 `az logic workflow create` 建工作流。

## B1. 创建 ACS Email API 连接（`acsemail`，密钥，无需邮箱账号）

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

## B2. 生成工作流定义文件

Logic App 收件人用**分号**分隔。用下面命令生成一份 `workflow-def.json`（工作流定义 + `$connections` 绑定）：

```bash
LOGIC_APP=egress-cost-logic
LA_RECIPIENTS="you@example.com"          # 多个用分号(;)分隔

cat > /tmp/workflow-def.json <<JSON
{
  "definition": {
    "\$schema": "https://schema.management.azure.com/providers/Microsoft.Logic/schemas/2016-06-01/workflowdefinition.json#",
    "contentVersion": "1.0.0.0",
    "parameters": {
      "\$connections": { "type": "Object", "defaultValue": {} },
      "sender": { "type": "String", "defaultValue": "$EMAIL_SENDER" },
      "recipients": { "type": "String", "defaultValue": "$LA_RECIPIENTS" }
    },
    "triggers": {
      "Recurrence": {
        "type": "Recurrence",
        "recurrence": { "frequency": "Day", "interval": 1, "timeZone": "UTC",
          "schedule": { "hours": [ 2 ], "minutes": [ 0 ] } }
      }
    },
    "actions": {
      "Query_cost": {
        "type": "Http",
        "inputs": {
          "method": "POST",
          "uri": "https://management.azure.com/subscriptions/$SUB/providers/Microsoft.CostManagement/query?api-version=2023-11-01",
          "authentication": { "type": "ManagedServiceIdentity", "audience": "https://management.azure.com/" },
          "body": {
            "type": "ActualCost", "timeframe": "Custom",
            "timePeriod": {
              "from": "@{formatDateTime(addDays(utcNow(), -1), 'yyyy-MM-ddT00:00:00Z')}",
              "to": "@{formatDateTime(addDays(utcNow(), -1), 'yyyy-MM-ddT23:59:59Z')}"
            },
            "dataset": {
              "granularity": "None",
              "aggregation": { "totalCost": { "name": "PreTaxCost", "function": "Sum" } },
              "filter": { "dimensions": { "name": "MeterCategory", "operator": "In", "values": [ "$METER_CATEGORY" ] } }
            }
          }
        }
      },
      "Compose_cost": {
        "type": "Compose", "runAfter": { "Query_cost": [ "Succeeded" ] },
        "inputs": "@if(greater(length(body('Query_cost')?['properties']?['rows']), 0), first(body('Query_cost')?['properties']?['rows'])?[0], 0)"
      },
      "Compose_currency": {
        "type": "Compose", "runAfter": { "Compose_cost": [ "Succeeded" ] },
        "inputs": "@if(greater(length(body('Query_cost')?['properties']?['rows']), 0), first(body('Query_cost')?['properties']?['rows'])?[1], 'USD')"
      },
      "Select_recipients": {
        "type": "Select", "runAfter": { "Compose_currency": [ "Succeeded" ] },
        "inputs": { "from": "@split(parameters('recipients'), ';')", "select": { "address": "@trim(item())" } }
      },
      "Send_email": {
        "type": "ApiConnection", "runAfter": { "Select_recipients": [ "Succeeded" ] },
        "inputs": {
          "host": { "connection": { "name": "@parameters('\$connections')['acsemail']['connectionId']" } },
          "method": "post",
          "path": "/emails:sendGAVersion",
          "queries": { "api-version": "2023-03-31" },
          "body": {
            "senderAddress": "@parameters('sender')",
            "recipients": { "to": "@body('Select_recipients')" },
            "content": {
              "subject": "@{concat('[出网费用] ', formatDateTime(addDays(utcNow(), -1), 'yyyy-MM-dd'), ' 合计 ', string(outputs('Compose_cost')), ' ', string(outputs('Compose_currency')))}",
              "html": "<div style='font-family:Segoe UI,Arial,sans-serif;font-size:14px'><h3>出网流量费用日报 · @{formatDateTime(addDays(utcNow(), -1), 'yyyy-MM-dd')}</h3><p>合计：<b>@{string(outputs('Compose_cost'))} @{string(outputs('Compose_currency'))}</b></p></div>"
            },
            "importance": "Normal"
          }
        }
      }
    },
    "outputs": {}
  },
  "parameters": {
    "\$connections": {
      "value": {
        "acsemail": { "connectionId": "$CONN_ID", "connectionName": "$CONN_NAME", "id": "$API_ID" }
      }
    }
  }
}
JSON
```

> `api-version=2023-03-31` 是 `Send_email` 必需的查询参数；缺失会导致连接器返回 `404 Resource not found`。

## B3. 创建 Logic App 工作流（az cli）

```bash
az logic workflow create \
  -g $RG --name $LOGIC_APP --location $LOCATION \
  --definition "$(python3 -c "import json;print(json.dumps(json.load(open('/tmp/workflow-def.json'))['definition']))")" \
  --parameters "$(python3 -c "import json;print(json.dumps(json.load(open('/tmp/workflow-def.json'))['parameters']))")"

# 开启系统托管标识（查成本用）
az resource update -g $RG --name $LOGIC_APP \
  --resource-type Microsoft.Logic/workflows --api-version 2019-05-01 \
  --set identity.type=SystemAssigned
```

## B4. 授予托管标识查成本的角色

```bash
LA_PRINCIPAL=$(az resource show -g $RG --name $LOGIC_APP \
  --resource-type Microsoft.Logic/workflows --api-version 2019-05-01 \
  --query identity.principalId -o tsv)

az role assignment create \
  --assignee-object-id $LA_PRINCIPAL \
  --assignee-principal-type ServicePrincipal \
  --role "Cost Management Reader" \
  --scope "/subscriptions/$SUB"
```

## B5. 手动触发验证（Recurrence 触发器）

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

---

## 两套方案对比

| | **方案 A：Function + ACS** | **方案 B：Logic App + ACS** |
|---|---|---|
| 运行时 | Python 3.11 | 低代码工作流（无代码） |
| 发邮件 | ACS SDK（HTML 含分项明细表） | ACS Email 连接器（`acsemail`，密钥） |
| 邮件正文 | 真实金额 + 明细 | 真实金额（合计） |
| 收件人分隔 | 逗号 `,` | 分号 `;` |
| 查成本鉴权 | 系统托管标识 | 系统托管标识 |
| 需授予角色 | Cost Management Reader | Cost Management Reader |
| 发邮件鉴权 | ACS 连接串 | ACS 连接串（连接资源密钥） |
| 邮箱账号 | 不需要 | 不需要 |

---

## 常见问题与要点

- **存储账户策略**：新建存储被策略禁用共享密钥/公网时，改用允许密钥+公网的已有存储覆盖 `AzureWebJobsStorage`。
- **Python v2 部署**：需 `AzureWebJobsFeatureFlags=EnableWorkerIndexing`，且 `func azure functionapp publish --build remote` 才装依赖。
- **Cost Management 限流（429）**：反复手动测试易触发限流；Function 代码已内置 `Retry-After` 退避重试。
- **ACS 邮件连接器 404**：`Send_email` 必须带 `api-version=2023-03-31` 查询参数。
- **出网口径**：Azure 出网流量计费归属 `MeterCategory=Bandwidth`；如需更细口径可改用 `MeterSubCategory` 过滤或调整 `METER_CATEGORY`。
- **成本数据延迟**：Cost Management 数据通常有数小时延迟，02:00 拉前一天数据可覆盖。
- **角色分配权限**：`az role assignment create` 需执行者具备 `Microsoft.Authorization/roleAssignments/write`（订阅 Owner / User Access Administrator）。
