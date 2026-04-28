# 部署指南：AMR 慢查询日志分析方案

## 前置条件

| 资源 | 要求 |
|---|---|
| Azure 订阅 | 对目标资源组具有 Owner 或 Contributor + User Access Administrator 权限 |
| AKS 集群 | 已启用 OIDC Issuer 和 Workload Identity Webhook |
| Container Registry | 可推送镜像的 ACR 实例 |
| Azure CLI | 已登录（`az login`），本地已安装 `kubectl`、`docker` |
| Python | 3.10+，用于执行 Workbook 部署脚本 |

---

## 步骤一：准备 Azure Monitor 资源

> 以下资源仅需创建一次，所有集群共用。

### 1.1 创建自定义日志表

```bash
SUBSCRIPTION_ID="<your-subscription-id>"
RESOURCE_GROUP="<your-resource-group>"
WORKSPACE_NAME="<your-workspace-name>"

az monitor log-analytics workspace table create \
  --subscription "$SUBSCRIPTION_ID" \
  --resource-group "$RESOURCE_GROUP" \
  --workspace-name "$WORKSPACE_NAME" \
  --name "AMRSlowQuery_CL" \
  --columns '[
    {"name":"TimeGenerated",  "type":"datetime"},
    {"name":"SlowlogId",      "type":"int"},
    {"name":"Duration_us",    "type":"long"},
    {"name":"Duration_ms",    "type":"real"},
    {"name":"Command",        "type":"string"},
    {"name":"RedisHost",      "type":"string"},
    {"name":"ClusterName",    "type":"string"},
    {"name":"Node",           "type":"string"},
    {"name":"ExportedAt",     "type":"datetime"}
  ]'
```

### 1.2 创建 Data Collection Endpoint（DCE）

```bash
az monitor data-collection endpoint create \
  --subscription "$SUBSCRIPTION_ID" \
  --resource-group "$RESOURCE_GROUP" \
  --name "amr-slowquery-dce" \
  --location "<location>" \
  --public-network-access Enabled
```

记录输出中的 `logsIngestion.endpoint`，后续配置为 `DCE_ENDPOINT`。

### 1.3 创建 Data Collection Rule（DCR）

```bash
DCE_RESOURCE_ID=$(az monitor data-collection endpoint show \
  --name amr-slowquery-dce \
  --resource-group "$RESOURCE_GROUP" \
  --query id -o tsv)

WORKSPACE_RESOURCE_ID="/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.OperationalInsights/workspaces/${WORKSPACE_NAME}"

az monitor data-collection rule create \
  --subscription "$SUBSCRIPTION_ID" \
  --resource-group "$RESOURCE_GROUP" \
  --name "amr-slowquery-dcr" \
  --location "$LOCATION" \
  --data-collection-endpoint-id "$DCE_RESOURCE_ID" \
  --stream-declarations '{
    "Custom-AMRSlowQuery_CL": {
      "columns": [
        {"name":"TimeGenerated",  "type":"datetime"},
        {"name":"SlowlogId",      "type":"int"},
        {"name":"Duration_us",    "type":"long"},
        {"name":"Duration_ms",    "type":"real"},
        {"name":"Command",        "type":"string"},
        {"name":"RedisHost",      "type":"string"},
        {"name":"ClusterName",    "type":"string"},
        {"name":"Node",           "type":"string"},
        {"name":"ExportedAt",     "type":"datetime"}
      ]
    }
  }' \
  --destinations "{\"logAnalytics\":[{\"workspaceResourceId\":\"${WORKSPACE_RESOURCE_ID}\",\"name\":\"defaultWorkspace\"}]}" \
  --data-flows '[{
    "streams":      ["Custom-AMRSlowQuery_CL"],
    "destinations": ["defaultWorkspace"],
    "transformKql": "source | project TimeGenerated, SlowlogId, Duration_us, Duration_ms, Command, ClientAddress, ClientName, RedisHost, ClusterName, Node, ExportedAt",
    "outputStream": "Custom-AMRSlowQuery_CL"
  }]'
```

记录输出中的 `immutableId`（格式：`dcr-xxxxxxxx`），配置为 `DCR_RULE_ID`。

---

## 步骤二：配置 AKS Workload Identity

> UAMI 和 DCR 角色授权仅需配置一次，所有 Pod 共用同一 UAMI。

### 2.1 创建 User-Assigned Managed Identity（UAMI）

```bash
az identity create \
  --subscription "$SUBSCRIPTION_ID" \
  --resource-group "$RESOURCE_GROUP" \
  --name "amr-slowquery-identity"

PRINCIPAL_ID=$(az identity show \
  --name amr-slowquery-identity \
  --resource-group "$RESOURCE_GROUP" \
  --query principalId -o tsv)
```

### 2.2 授予 DCR 上报权限

```bash
DCR_RESOURCE_ID=$(az monitor data-collection rule show \
  --name amr-slowquery-dcr \
  --resource-group "$RESOURCE_GROUP" \
  --query id -o tsv)

az role assignment create \
  --assignee-object-id "$PRINCIPAL_ID" \
  --assignee-principal-type ServicePrincipal \
  --role "Monitoring Metrics Publisher" \
  --scope "$DCR_RESOURCE_ID"
```

### 2.3 创建 Federated Credential

StatefulSet 中所有 Pod 共用同一个 ServiceAccount，只需创建一个 Federated Credential：

```bash
AKS_CLUSTER_NAME="<your-aks-cluster-name>"
AKS_RESOURCE_GROUP="<aks-resource-group>"
K8S_NAMESPACE="amr-exporter"

OIDC_ISSUER=$(az aks show \
  --name "$AKS_CLUSTER_NAME" \
  --resource-group "$AKS_RESOURCE_GROUP" \
  --query oidcIssuerProfile.issuerUrl -o tsv)

az identity federated-credential create \
  --name "amr-slowquery-federated" \
  --identity-name "amr-slowquery-identity" \
  --resource-group "$RESOURCE_GROUP" \
  --issuer "$OIDC_ISSUER" \
  --subject "system:serviceaccount:${K8S_NAMESPACE}:amr-slowquery-exporter" \
  --audience api://AzureADTokenExchange
```

记录 UAMI 的 `clientId`，填入 `k8s/overlays/prod/serviceaccount.yaml` 的 `azure.workload.identity/client-id` 注解。

---

## 步骤三：构建并推送 Docker 镜像

```bash
ACR_NAME="<your-acr-name>"
IMAGE="${ACR_NAME}.azurecr.io/amr-slowquery-exporter:latest"

az acr login --name "$ACR_NAME"
docker build -t "$IMAGE" .
docker push "$IMAGE"
```

更新 `k8s/base/statefulset.yaml` 和 `k8s.yaml` 中的 `image` 字段为实际 ACR 地址。

---

## 步骤四：配置集群列表并部署

`k8s/overlays/template/` 为模板目录，使用 `${VAR}` 占位符；实际部署目录 `k8s/overlays/prod/` 由 `envsubst` 生成，不纳入版本管理。

### 4.1 确认环境变量已加载

确保以下变量已在当前 Shell 中导出（可通过 `.env` 文件加载）：

```bash
set -a && source .env && set +a
```

涉及变量：`DCE_ENDPOINT`、`DCR_RULE_ID`、`IMAGE_REPO`、`IMAGE_TAG`、`UAMI_CLIENT_ID`、`POLL_INTERVAL_SECONDS`、`SLOWLOG_BATCH_SIZE`。

### 4.2 编辑集群配置

用 envsubst 将模板渲染为实际配置

```bash
mkdir -p k8s/overlays/prod
for f in k8s/overlays/template/*.yaml; do
  envsubst < "$f" > "k8s/overlays/prod/$(basename $f)"
done
```

编辑 `k8s/overlays/template/clusters-config.yaml`，在 `clusters.json` 数组中为每个 AMR 集群添加一个对象：

```yaml
stringData:
  clusters.json: |
    [
      {
        "AMR_CLUSTER_NAME": "prod-cache-sea",
        "AMR_HOST": "prod-cache-sea.southeastasia.redis.azure.net",
        "AMR_PORT": 10000,
        "AMR_ACCESS_KEY": "<access-key>",
        "AMR_CLUSTER_POLICY": "oss",
        "AMR_SSL_VERIFY": "false"
      },
      {
        "AMR_CLUSTER_NAME": "prod-cache-eus",
        "AMR_HOST": "prod-cache-eus.eastus.redis.azure.net",
        "AMR_PORT": 10000,
        "AMR_ACCESS_KEY": "<access-key>",
        "AMR_CLUSTER_POLICY": "enterprise",
        "AMR_SSL_VERIFY": "true"
      }
    ]
```

同步更新 `k8s/overlays/template/replicas-patch.yaml` 中的 `replicas` 值，使其等于数组长度。

### 4.3 生成部署目录并部署至 AKS

```bash
az aks get-credentials \
  --name "$AKS_CLUSTER_NAME" \
  --resource-group "$AKS_RESOURCE_GROUP"

kubectl apply -k k8s/overlays/prod
```

### 4.3 验证运行状态

```bash
kubectl -n amr-exporter get pods
# 预期输出：
# amr-slowquery-exporter-0   1/1   Running
# amr-slowquery-exporter-1   1/1   Running

# 查看各 Pod 日志确认集群绑定
kubectl -n amr-exporter logs amr-slowquery-exporter-0
kubectl -n amr-exporter logs amr-slowquery-exporter-1
```

正常启动日志示例：
```
2025-01-01T00:00:00 INFO     Pod amr-slowquery-exporter-0 (ordinal=0) → cluster 'prod-cache-sea' (prod-cache-sea.redis.azure.net:10000)
2025-01-01T00:01:00 INFO     Sent 5 entries to Log Analytics
```

---

## 步骤五：部署 Workbook

安装 Python 依赖：

```bash
pip install azure-mgmt-applicationinsights azure-identity
```

编辑 `deploy-workbook.py`，确认以下常量与实际环境一致：

```python
SUBSCRIPTION_ID = "<your-subscription-id>"
RESOURCE_GROUP  = "<your-resource-group>"
WORKSPACE_ID    = "/subscriptions/.../workspaces/<workspace-name>"
LOCATION        = "<location>"
```

执行部署：

```bash
az login
python deploy-workbook.py
```

---

## 新增 AMR 集群

1. 在 `clusters-config.yaml` 的 `clusters.json` **末尾**追加新集群对象
2. 将 `replicas-patch.yaml` 的 `replicas` 值加 1
3. 重新部署：`kubectl apply -k k8s/overlays/prod`

StatefulSet 仅创建新增的 Pod（最高序号），已有 Pod 不重启。

## 移除 AMR 集群

1. 从 `clusters.json` **末尾**删除对应对象（只能安全移除最后一个集群）
2. 将 `replicas` 值减 1
3. 重新部署：`kubectl apply -k k8s/overlays/prod`

> **注意**：若需移除中间某个集群，建议先将其 `AMR_CLUSTER_NAME` 改为空或标记为停用，待后续调整序号规划后再处理。

---

## 验证数据接入

```kql
AMRSlowQuery_CL
| where TimeGenerated > ago(1h)
| summarize count() by ClusterName, bin(TimeGenerated, 5m)
| render timechart
```

---

## 常见问题

| 现象 | 排查方向 |
|---|---|
| Pod 处于 Pending | 检查节点资源；`kubectl describe pod` 查看事件 |
| `Cannot parse ordinal from POD_NAME` | 确认 StatefulSet 中配置了 Downward API `POD_NAME` 环境变量 |
| `Pod ordinal N out of range` | `clusters.json` 条目数少于 `replicas`，两者必须相等 |
| `Permission denied` 写 `/data` | 确认 `securityContext.fsGroup: 65534` 已设置 |
| `SSL: CERTIFICATE_VERIFY_FAILED` | OSS 模式下该集群条目设置 `"AMR_SSL_VERIFY": "false"` |
| Log Analytics 无数据 | 检查 DCR 角色授予；查看 Pod 日志中是否有 `Failed to send` 错误 |
| Workbook 集群下拉为空 | 确认 `ClusterName` 列已加入表结构；等待首批数据写入（约 5~10 分钟） |

---

## 环境变量参考

以下变量通过 `clusters.json` 中的字段设置（StatefulSet 模式），或直接通过环境变量设置（单集群模式）：

| 键名 | 默认值 | 说明 |
|---|---|---|
| `AMR_CLUSTER_NAME` | `AMR_HOST` 的值 | 集群友好名称，显示在 Workbook 下拉框 |
| `AMR_HOST` | — | Redis 实例主机名（必填） |
| `AMR_PORT` | `10000` | Redis 端口 |
| `AMR_ACCESS_KEY` | — | Redis 访问密钥 |
| `AMR_CLUSTER_POLICY` | `enterprise` | `oss` 或 `enterprise` |
| `AMR_SSL_VERIFY` | `true` | `false` 跳过证书验证（OSS 模式需设为 false） |
| `POLL_INTERVAL_SECONDS` | `60` | 轮询间隔（秒），可在 shared-secret 中全局设置 |
| `SLOWLOG_BATCH_SIZE` | `128` | 每次 SLOWLOG GET 的最大条数 |
| `DCE_ENDPOINT` | — | Data Collection Endpoint URL（必填，在 shared-secret 中设置） |
| `DCR_RULE_ID` | — | DCR immutableId（必填，在 shared-secret 中设置） |
