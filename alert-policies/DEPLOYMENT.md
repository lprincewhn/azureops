# 虚拟机 / MySQL / Redis 指标告警策略 —— Azure CLI 部署文档

本文档说明如何使用 Azure CLI 部署三套 **DeployIfNotExists（DINE）** 自定义策略。
策略会自动为对应资源部署指标告警规则（`Microsoft.Insights/metricAlerts`），并绑定指定的 Action Group。
存量资源通过**修复任务（Remediation）**补齐，新增资源在合规评估后自动部署。

## 一、策略概览

| 策略名称 | 目标资源类型 | 告警指标 | 本地文件 |
|----------|--------------|----------|----------|
| `auto-vm-metric-alert` | `Microsoft.Compute/virtualMachines` | CPU（Percentage CPU > 阈值）、可用内存（Available Memory Percentage < 阈值） | `auto-vm-metric-alert.rules.json` / `auto-vm-metric-alert.params.json` |
| `auto-mysql-metric-alert` | `Microsoft.DBforMySQL/flexibleServers` | CPU（cpu_percent）、内存（memory_percent）、存储（storage_percent） | `auto-mysql-metric-alert.rules.json` / `auto-mysql-metric-alert.params.json` |
| `auto-redis-metric-alert` | `Microsoft.Cache/redisEnterprise`（Azure Managed Redis） | CPU（percentProcessorTime）、内存（usedmemorypercentage）、负载（serverLoad） | `auto-redis-metric-alert.rules.json` / `auto-redis-metric-alert.params.json` |

**告警命名规则**：`<前缀>-<资源组名>-<资源名>`，例如 `vm-cpu-jump-server_group-jump-server`。

## 二、前置条件

1. 在 [Azure 门户](https://portal.azure.com) 右上角点击 **Cloud Shell** 图标（或访问 [https://shell.azure.com](https://shell.azure.com)），选择 **Bash** 环境。Cloud Shell 已预装 Azure CLI 并自动完成登录，无需 `az login`。首次使用需按提示创建或选择一个存储账户。可用 `az account set --subscription "<订阅ID>"` 切换到目标订阅。
2. 部署账号需具备以下权限（在目标订阅范围）：
   - **Resource Policy Contributor**：创建策略定义与分配。
   - **User Access Administrator** / **Owner** / **Role Based Access Control Administrator**：为策略托管标识授予 `Monitoring Contributor`（DINE 修复所需）。
3. 已有一个 **Action Group**（用于接收告警通知），并准备好其资源 ID。
4. 本地已存在各策略的 `*.rules.json` 与 `*.params.json` 文件。

## 三、公共变量

```bash
# 目标订阅
SUBSCRIPTION_ID="<your-subscription-id>"
az account set --subscription "$SUBSCRIPTION_ID"

# 通知用的 Action Group 资源 ID
ACTION_GROUP_ID="/subscriptions/$SUBSCRIPTION_ID/resourceGroups/<ag-rg>/providers/microsoft.insights/actionGroups/<ag-name>"

# 分配时托管标识的位置（DINE 系统分配标识必填）
ASSIGNMENT_LOCATION="eastus"

# Monitoring Contributor 内置角色 ID（DINE 部署 metricAlerts 所需）
MONITORING_CONTRIBUTOR="/providers/Microsoft.Authorization/roleDefinitions/749f88d5-cbae-40b8-bcfc-e573ddc772fa"
```

## 四、部署步骤

以下以 **VM 策略** 为例，MySQL / Redis 只需替换名称、文件与参数（见第五节）。

### 步骤 1：创建策略定义

```bash
az policy definition create \
  --name "auto-vm-metric-alert" \
  --display-name "自动为虚拟机创建指标告警" \
  --description "为策略范围内的 Azure 虚拟机自动部署指标告警（CPU 与可用内存），并绑定指定的 Action Group。" \
  --mode Indexed \
  --rules @auto-vm-metric-alert.rules.json \
  --params @auto-vm-metric-alert.params.json \
  --subscription "$SUBSCRIPTION_ID"
```

### 步骤 2：创建策略分配（系统分配托管标识）

DINE 策略需要托管标识来部署告警资源，因此必须指定 `--mi-system-assigned` 与 `--location`。

```bash
az policy assignment create \
  --name "auto-vm-metric-alert" \
  --display-name "自动为虚拟机创建指标告警" \
  --policy "auto-vm-metric-alert" \
  --scope "/subscriptions/$SUBSCRIPTION_ID" \
  --mi-system-assigned \
  --location "$ASSIGNMENT_LOCATION" \
  --params "{\"actionGroupId\": {\"value\": \"$ACTION_GROUP_ID\"}}"
```

获取分配自动生成的托管标识 principalId：

```bash
PRINCIPAL_ID=$(az policy assignment show \
  --name "auto-vm-metric-alert" \
  --scope "/subscriptions/$SUBSCRIPTION_ID" \
  --query identity.principalId -o tsv)
echo "Managed Identity principalId: $PRINCIPAL_ID"
```

### 步骤 3：为托管标识授予 Monitoring Contributor

> 需 **User Access Administrator / Owner** 权限。RBAC 传播可能需要数分钟。

```bash
az role assignment create \
  --assignee-object-id "$PRINCIPAL_ID" \
  --assignee-principal-type ServicePrincipal \
  --role "Monitoring Contributor" \
  --scope "/subscriptions/$SUBSCRIPTION_ID"
```

### 步骤 4：触发合规评估（新分配必需）

新建分配的合规状态尚未评估，直接修复会显示“0 个资源”。先触发一次按需扫描并等待完成：

```bash
# 全订阅扫描（较慢）；也可缩小到具体资源组以加速：--resource-group <rg>
az policy state trigger-scan --subscription "$SUBSCRIPTION_ID"
```

### 步骤 5：创建修复任务（为存量资源补齐告警）

```bash
az policy remediation create \
  --name "remediate-vm-metric-alert-$(date +%Y%m%d%H%M%S)" \
  --policy-assignment "auto-vm-metric-alert" \
  --resource-discovery-mode ExistingNonCompliant \
  --subscription "$SUBSCRIPTION_ID"
```

> 若尚未完成合规评估，可改用 `--resource-discovery-mode ReEvaluateCompliance`，它会在修复前重新评估（耗时更长）。

### 步骤 6：验证

```bash
# 查看修复任务状态
az policy remediation show \
  --name "<remediation-name>" \
  --policy-assignment "auto-vm-metric-alert" \
  --query "{state:provisioningState, total:deploymentStatus.totalDeployments, success:deploymentStatus.successfulDeployments, failed:deploymentStatus.failedDeployments}" -o json

# 查看已部署的告警规则
az monitor metrics alert list \
  --query "[?starts_with(name,'vm-')].{name:name, rg:resourceGroup, enabled:enabled}" -o table
```

## 五、MySQL 与 Redis 的差异化命令

### MySQL（`auto-mysql-metric-alert`）

```bash
# 步骤 1：定义
az policy definition create \
  --name "auto-mysql-metric-alert" \
  --display-name "自动为 MySQL 弹性服务器创建指标告警" \
  --description "为 Azure Database for MySQL 弹性服务器自动部署 CPU、内存与存储使用率指标告警。" \
  --mode Indexed \
  --rules @auto-mysql-metric-alert.rules.json \
  --params @auto-mysql-metric-alert.params.json \
  --subscription "$SUBSCRIPTION_ID"

# 步骤 2：分配
az policy assignment create \
  --name "auto-mysql-metric-alert" \
  --display-name "自动为 MySQL 弹性服务器创建指标告警" \
  --policy "auto-mysql-metric-alert" \
  --scope "/subscriptions/$SUBSCRIPTION_ID" \
  --mi-system-assigned --location "$ASSIGNMENT_LOCATION" \
  --params "{\"actionGroupId\": {\"value\": \"$ACTION_GROUP_ID\"}}"

# 步骤 3~6 同 VM，替换名称为 auto-mysql-metric-alert，告警前缀为 mysql-
```

### Redis（`auto-redis-metric-alert`）

```bash
# 步骤 1：定义
az policy definition create \
  --name "auto-redis-metric-alert" \
  --display-name "自动为 Azure Managed Redis 创建指标告警" \
  --description "为 Azure Managed Redis (Microsoft.Cache/redisEnterprise) 自动部署 CPU、内存与服务器负载指标告警。" \
  --mode Indexed \
  --rules @auto-redis-metric-alert.rules.json \
  --params @auto-redis-metric-alert.params.json \
  --subscription "$SUBSCRIPTION_ID"

# 步骤 2：分配
az policy assignment create \
  --name "auto-redis-metric-alert" \
  --display-name "自动为 Azure Managed Redis 创建指标告警" \
  --policy "auto-redis-metric-alert" \
  --scope "/subscriptions/$SUBSCRIPTION_ID" \
  --mi-system-assigned --location "$ASSIGNMENT_LOCATION" \
  --params "{\"actionGroupId\": {\"value\": \"$ACTION_GROUP_ID\"}}"

# 步骤 3~6 同 VM，替换名称为 auto-redis-metric-alert，告警前缀为 redis-
```

## 六、参数说明

| 参数 | 适用策略 | 默认值 | 说明 |
|------|----------|--------|------|
| `actionGroupId` | 全部 | 无（必填） | 接收告警的 Action Group 资源 ID |
| `cpuThreshold` | 全部 | 80 | CPU 使用率百分比阈值（`GreaterThan`） |
| `memoryAvailablePercentThreshold` | VM | 20 | 可用内存百分比阈值（`LessThan`，可用低于该值触发） |
| `memoryThreshold` | MySQL / Redis | 80 | 内存使用率百分比阈值（`GreaterThan`） |
| `storageThreshold` | MySQL | 85 | 存储使用率百分比阈值（`GreaterThan`） |
| `serverLoadThreshold` | Redis | 80 | 服务器负载百分比阈值（`GreaterThan`） |
| `effect` | 全部 | DeployIfNotExists | `DeployIfNotExists` 或 `Disabled` |

覆盖阈值示例（分配时）：

```bash
--params "{\"actionGroupId\": {\"value\": \"$ACTION_GROUP_ID\"}, \"cpuThreshold\": {\"value\": 90}}"
```

## 七、设计要点与注意事项

- **不要在 DINE `deployment` 中写 `location`**：本策略 `deploymentScope` 为 `resourceGroup`，资源组级部署不允许 `location` 属性，否则修复会报 `InvalidDeployment`。
- **告警命名使用 `resourceGroup().name`**：告警名与存在性检查名两侧都使用 `resourceGroup().name`，保证幂等（已存在则不重复部署）。同一订阅内避免跨资源组同名资源导致告警名冲突。
- **存在性检查基于名称**：修改分配的 `actionGroupId` 后，**存量告警不会被修复任务自动更新**（因名称已存在即视为合规）。如需更新存量告警的 Action Group，请直接更新告警资源，或先删除对应告警再修复重建。
- **新分配需先评估再修复**：合规评估完成后，`ExistingNonCompliant` 才能识别到不合规资源。
- **告警内容**：短信为 Azure 固定精简格式，无法自定义；邮件 / Webhook / Logic App 可获得完整告警上下文。

## 八、卸载（回滚）

```bash
NAME="auto-vm-metric-alert"   # 或 auto-mysql-metric-alert / auto-redis-metric-alert

# 删除分配
az policy assignment delete --name "$NAME" --scope "/subscriptions/$SUBSCRIPTION_ID"

# 删除定义
az policy definition delete --name "$NAME" --subscription "$SUBSCRIPTION_ID"

# （可选）删除已部署的告警规则
az monitor metrics alert list \
  --query "[?starts_with(name,'vm-')].id" -o tsv \
  | xargs -r -I{} az monitor metrics alert delete --ids {}
```

> 删除策略分配不会自动删除已部署的告警规则，需按需手动清理。
