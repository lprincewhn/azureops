# 部署指南：AMR 慢查询日志分析方案

## 前置条件

| 资源 | 要求 |
|---|---|
| Azure 订阅 | 见下方「权限」说明 —— 步骤 2.2 需要能写 roleAssignments |
| AKS 集群 | 已启用 OIDC Issuer 和 Workload Identity Webhook |
| Container Registry | 可推送镜像的 ACR 实例，且 AKS 能从中拉取（见步骤 3.2） |
| Azure CLI | 已登录（`az login`），本地已安装 `kubectl`、`docker`、`envsubst` |
| Python | 3.10+，用于执行 Workbook 部署脚本 |

### 权限

步骤 2.2 要给 UAMI 授予 DCR 上的 `Monitoring Metrics Publisher` 角色，这需要
`Microsoft.Authorization/roleAssignments/write`——**Contributor 单独不够**，还需要
`User Access Administrator`（或 Owner）。

这一条是整条链路的硬依赖，不是可选优化：Contributor 身份可以创建 DCE / DCR / UAMI /
联合凭据，Pod 也能正常拿到 Workload Identity 令牌，但上报会被数据面拒绝：

```
(OperationFailed) The authentication token provided does not have access to ingest data
for the data collection rule with immutable Id 'dcr-xxxxxxxx'.
```

若执行者没有该权限，请把步骤 2.2 的单条命令交给订阅 Owner 执行，其余步骤不受影响。

---

## 步骤一：准备 Azure Monitor 资源

> 以下资源仅需创建一次，所有集群共用。

先导出后续各步骤共用的变量：

```bash
export SUBSCRIPTION_ID="<your-subscription-id>"
export RESOURCE_GROUP="<your-resource-group>"
export WORKSPACE_NAME="<your-workspace-name>"
export LOCATION="<location>"          # 例如 southeastasia
```

### 1.1 创建自定义日志表

`--columns` 接受的是**空格分隔的 `name=type`** 列表，不是 JSON 数组：

```bash
az monitor log-analytics workspace table create \
  --subscription "$SUBSCRIPTION_ID" \
  --resource-group "$RESOURCE_GROUP" \
  --workspace-name "$WORKSPACE_NAME" \
  --name "AMRSlowQuery_CL" \
  --columns TimeGenerated=datetime \
            SlowlogId=int \
            Duration_us=long \
            Duration_ms=real \
            Command=string \
            RedisHost=string \
            ClusterName=string \
            Node=string \
            ExportedAt=datetime
```

### 1.2 创建 Data Collection Endpoint（DCE）

```bash
az monitor data-collection endpoint create \
  --subscription "$SUBSCRIPTION_ID" \
  --resource-group "$RESOURCE_GROUP" \
  --name "amr-slowquery-dce" \
  --location "$LOCATION" \
  --public-network-access Enabled
```

记录输出中的 `logsIngestion.endpoint`，后续配置为 `DCE_ENDPOINT`。

### 1.3 创建 Data Collection Rule（DCR）

> `transformKql` 里 project 的列必须与 `--stream-declarations` 以及 1.1 建的表结构
> 三者完全一致，多一列都会让创建失败（`InvalidTransformQuery: Undefined symbol: <列名>`）。

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
    "transformKql": "source | project TimeGenerated, SlowlogId, Duration_us, Duration_ms, Command, RedisHost, ClusterName, Node, ExportedAt",
    "outputStream": "Custom-AMRSlowQuery_CL"
  }]'
```

> 命令会打印一条 `WARNING: Use extended value 'Custom-AMRSlowQuery_CL' outside choices [...]`。
> 自定义流名本就不在 CLI 的内置枚举里，这条警告可以忽略。

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

同时记录 `clientId`，步骤四要作为 `UAMI_CLIENT_ID` 写进 `.env`：

```bash
az identity show --name amr-slowquery-identity \
  --resource-group "$RESOURCE_GROUP" --query clientId -o tsv
```

### 2.2 授予 DCR 上报权限

> 需要 `roleAssignments/write` 权限，见「前置条件 → 权限」。没有权限时这条命令会报
> `AuthorizationFailed`，请把它整条交给订阅 Owner 执行。

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

角色授予到数据面生效通常需要 1～5 分钟。在此期间 Pod 日志里会出现
`The authentication token provided does not have access to ingest data`，属正常现象。
exporter 在上报失败时**不会推进持久游标**，等权限生效后积压条目会自动补报，不会丢数据。

验证角色已生效：

```bash
az role assignment list --assignee-object-id "$PRINCIPAL_ID" \
  --scope "$DCR_RESOURCE_ID" --query "[].roleDefinitionName" -o tsv
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

> UAMI 的 `clientId` **不要**手改进 `k8s/overlays/prod/serviceaccount.yaml` —— 那个目录是
> envsubst 的输出，下次渲染会被覆盖。填进 `.env` 的 `UAMI_CLIENT_ID` 即可（步骤 4.1）。

---

## 步骤三：构建镜像并让 AKS 能拉取

### 3.1 构建并推送

```bash
ACR_NAME="<your-acr-name>"
IMAGE="${ACR_NAME}.azurecr.io/amr-slowquery-exporter:latest"

az acr login --name "$ACR_NAME"
docker build -t "$IMAGE" .
docker push "$IMAGE"
```

> **不要**手改 `k8s/base/statefulset.yaml` 里的 `image` 字段。那里的
> `image: amr-slowquery-exporter:latest` 是 overlay 中 kustomize `images:` 变换的**匹配键**，
> 改掉反而会让 `newName`/`newTag` 失配。镜像地址通过 `.env` 的
> `IMAGE_REPO` / `IMAGE_TAG` 注入（步骤 4.1）。

### 3.2 确认 AKS 能拉取该 ACR

推送成功并不代表集群拉得到，必须二选一。

**方案 A（推荐）**：给 AKS 的 kubelet identity 授 `AcrPull`。同样需要
`roleAssignments/write` 权限：

```bash
az aks update -n "$AKS_CLUSTER_NAME" -g "$AKS_RESOURCE_GROUP" --attach-acr "$ACR_NAME"
```

没有权限时会失败在最后一步：
`ERROR: Could not create a role assignment for ACR. Are you an Owner on this subscription?`

**方案 B（无 RBAC 权限时的回退）**：在 namespace 内建 imagePullSecret。仓库里的
`k8s/overlays/template/imagepullsecret-patch.yaml` 已经把
`imagePullSecrets: [{name: acr-pull}]` 打进 StatefulSet：

```bash
kubectl create namespace amr-exporter --dry-run=client -o yaml | kubectl apply -f -

ACR_USER=$(az acr credential show -n "$ACR_NAME" --query username -o tsv)
ACR_PASS=$(az acr credential show -n "$ACR_NAME" --query "passwords[0].value" -o tsv)

kubectl -n amr-exporter create secret docker-registry acr-pull \
  --docker-server="${ACR_NAME}.azurecr.io" \
  --docker-username="$ACR_USER" \
  --docker-password="$ACR_PASS"
```

> 方案 B 依赖 ACR 的 admin user（`az acr update -n <acr> --admin-enabled true`），
> 属长期保存注册表口令的做法，能用方案 A 就用方案 A。
> 走方案 A 时，请把 `imagepullsecret-patch.yaml` 从 `kustomization.yaml` 的
> `patches:` 列表里删掉。

---

## 步骤四：配置集群列表并部署

`k8s/overlays/template/` 是纳入版本管理的模板目录，使用 `${VAR}` 占位符；
实际部署目录 `k8s/overlays/prod/` 由 `envsubst` 生成，被 `.gitignore` 排除。

> **顺序很重要**：先写好 `.env` 和集群列表，**再**渲染。反过来做，渲染出的是仍带占位符的
> Secret，Pod 会去连一个字面量主机名。

### 4.1 准备 .env

以 `.env.example` 为模板创建 `.env`（该文件被 .gitignore 排除，access key 只存在这里）：

```bash
cp .env.example .env
chmod 600 .env
```

至少需要填写：

| 变量 | 来源 |
|---|---|
| `DCE_ENDPOINT` | 步骤 1.2 的 `logsIngestion.endpoint` |
| `DCR_RULE_ID` | 步骤 1.3 的 `immutableId` |
| `IMAGE_REPO` / `IMAGE_TAG` | 步骤 3.1 推送的镜像 |
| `UAMI_CLIENT_ID` | 步骤 2.1 的 `clientId` |
| `POLL_INTERVAL_SECONDS` / `SLOWLOG_BATCH_SIZE` | 采集行为，可用默认值 |
| `AMR_CLUSTER_NAME` / `AMR_HOST` / `AMR_PORT` | 目标 AMR 集群 |
| `AMR_ACCESS_KEY` | `az redisenterprise database list-keys --cluster-name <name> -g <rg> --query primaryKey -o tsv` |
| `AMR_CLUSTER_POLICY` | `oss` 或 `enterprise`，必须与集群实际的 clustering policy 一致 |
| `AMR_SSL_VERIFY` | 见步骤 4.2 |

确认集群的 clustering policy：

```bash
az redisenterprise database show --cluster-name <name> -g <rg> \
  --database-name default --query clusteringPolicy -o tsv
# OSSCluster  → AMR_CLUSTER_POLICY=oss
# EnterpriseCluster → AMR_CLUSTER_POLICY=enterprise
```

### 4.2 编辑集群列表

`k8s/overlays/template/clusters-config.yaml` 的 `clusters.json` 数组，每个 AMR 集群一个对象，
字段值全部来自 `.env`（**不要把真实 access key 写进这个受版本管理的文件**）：

```yaml
stringData:
  clusters.json: |
    [
      {
        "AMR_CLUSTER_NAME": "${AMR_CLUSTER_NAME}",
        "AMR_HOST": "${AMR_HOST}",
        "AMR_PORT": ${AMR_PORT},
        "AMR_ACCESS_KEY": "${AMR_ACCESS_KEY}",
        "AMR_CLUSTER_POLICY": "${AMR_CLUSTER_POLICY}",
        "AMR_SSL_VERIFY": "${AMR_SSL_VERIFY}"
      }
    ]
```

多集群时为每个集群在 `.env` 里定义一组带后缀的变量（`AMR_HOST_B`、`AMR_ACCESS_KEY_B`……），
在数组里追加对应对象。

同步更新 `k8s/overlays/template/replicas-patch.yaml` 中的 `replicas`，使其等于数组长度。

#### 关于 AMR_SSL_VERIFY

**OSS cluster policy 下目前只能设为 `"false"`，这是临时绕过，不是推荐配置。**

原因：OSS 模式下 redis-py 先连 `AMR_HOST` 拿 `CLUSTER SLOTS`，再按返回的**分片 IP**
直连各主分片。AMR 的服务端证书只对主机名有效，对 IP 无效，于是 `AMR_SSL_VERIFY=true`
会失败在主机名校验：

```
redis.exceptions.RedisClusterException: Redis Cluster cannot be connected.
Please provide at least one reachable node: Error 1 connecting to <shard-ip>:<port>.
[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: IP address mismatch,
certificate is not valid for '<shard-ip>'.
```

需要放宽的其实只有**主机名校验**这一项；`AMR_SSL_VERIFY=false` 走的是
`ssl_cert_reqs="none"`，把**证书链校验也一并关掉**了，等于接受任意证书——这是实质性的
安全降级。实测在保留证书链校验的同时只关闭主机名校验（`ssl_check_hostname=False`）
可以正常连上两个主分片，但 exporter 目前没有暴露这个开关。

Enterprise cluster policy 单端点连接不受影响，应保持 `"true"`。

### 4.3 渲染部署目录

```bash
set -a && source .env && set +a

mkdir -p k8s/overlays/prod
for f in k8s/overlays/template/*.yaml; do
  envsubst < "$f" > "k8s/overlays/prod/$(basename $f)"
done
```

渲染后务必确认没有残留占位符：

```bash
grep -n '\${' k8s/overlays/prod/*.yaml   # 应无输出
kubectl kustomize k8s/overlays/prod | grep -E 'image:|replicas:'
```

### 4.4 部署至 AKS

```bash
az aks get-credentials \
  --name "$AKS_CLUSTER_NAME" \
  --resource-group "$AKS_RESOURCE_GROUP"

kubectl apply -k k8s/overlays/prod
```

### 4.5 验证运行状态

```bash
kubectl -n amr-exporter get pods
# 预期输出：
# amr-slowquery-exporter-0   1/1   Running
# amr-slowquery-exporter-1   1/1   Running

# 查看各 Pod 日志确认集群绑定
kubectl -n amr-exporter logs amr-slowquery-exporter-0
```

正常启动日志示例（OSS 模式，单集群）：
```
2026-01-01T00:00:00 INFO     Pod amr-slowquery-exporter-0 (ordinal=0) → cluster '<cluster-name>' (<amr-host>:10000)
2026-01-01T00:00:00 INFO     Slow query exporter starting — host=<amr-host>:10000  poll=60s  output=/data/slowquery.jsonl
2026-01-01T00:00:00 INFO     Connecting to <amr-host>:10000 (OSS cluster policy, SSL)
2026-01-01T00:00:00 INFO     Resuming from per-node state: {}
2026-01-01T00:01:00 INFO     Sent 18 entries to Log Analytics
```

---

## 步骤五：部署 Workbook

安装依赖：

```bash
pip install azure-mgmt-applicationinsights azure-identity python-dotenv
```

`deploy-workbook.py` **不需要编辑**——它从环境变量读取配置（`load_dotenv()` 会自动加载 `.env`）。
确认 `.env` 中这四项已填：

```
SUBSCRIPTION_ID=<your-subscription-id>
RESOURCE_GROUP=<your-resource-group>
WORKSPACE_NAME=<your-workspace-name>
LOCATION=<location>
```

执行部署：

```bash
python deploy-workbook.py
```

脚本用固定 GUID，重复执行是更新而非新建。输出会给出 Workbook 的直达 Portal URL。

---

## 新增 AMR 集群

1. 在 `.env` 中为新集群添加一组带后缀的变量
2. 在 `clusters-config.yaml` 的 `clusters.json` **末尾**追加对应对象
3. 将 `replicas-patch.yaml` 的 `replicas` 值加 1
4. 重新渲染并部署：

```bash
set -a && source .env && set +a
for f in k8s/overlays/template/*.yaml; do
  envsubst < "$f" > "k8s/overlays/prod/$(basename $f)"
done
kubectl apply -k k8s/overlays/prod
```

StatefulSet 仅创建新增的 Pod（最高序号），已有 Pod 不重启。

## 移除 AMR 集群

1. 从 `clusters.json` **末尾**删除对应对象（只能安全移除最后一个集群）
2. 将 `replicas` 值减 1
3. 重新渲染并部署（同上）

> **注意**：若需移除中间某个集群，建议先将其 `AMR_CLUSTER_NAME` 改为空或标记为停用，待后续调整序号规划后再处理。

---

## 验证数据接入

```kql
AMRSlowQuery_CL
| where TimeGenerated > ago(1h)
| summarize count() by ClusterName, bin(TimeGenerated, 5m)
| render timechart
```

制造真实慢查询用于验证（会把该分片的慢查询阈值降到 0，验证后记得改回去）：

```bash
# 原值一般是 10000（微秒）
redis-cli -h <amr-host> -p 10000 --tls --insecure -a <access-key> \
  config set slowlog-log-slower-than 0
```

> OSS 模式下 `CONFIG SET` 只作用于连上的那个分片。要覆盖全部主分片，需对
> `CLUSTER SLOTS` 返回的每个主分片分别执行。

同时确认 PVC 上的本地备份：

```bash
kubectl -n amr-exporter exec amr-slowquery-exporter-0 -- wc -l /data/slowquery.jsonl
kubectl -n amr-exporter exec amr-slowquery-exporter-0 -- cat /data/.slowquery_state.json
```

---

## 常见问题

| 现象 | 排查方向 |
|---|---|
| Pod 处于 Pending | 检查节点资源；`kubectl describe pod` 查看事件 |
| `ImagePullBackOff` / `401 Unauthorized` 拉镜像 | 步骤 3.2 未做：kubelet identity 没有 `AcrPull`，且没建 imagePullSecret |
| `kubectl` 报 API server 域名 `no such host` | 集群可能被停机：`az aks show -n <aks> -g <rg> --query powerState.code`，停机时用 `az aks start` |
| `Cannot parse ordinal from POD_NAME` | 确认 StatefulSet 中配置了 Downward API `POD_NAME` 环境变量 |
| `Pod ordinal N out of range` | `clusters.json` 条目数少于 `replicas`，两者必须相等 |
| Pod 连的是 `<cluster-a>...` 之类字面主机名 | 渲染顺序错了：先改模板再 envsubst；`grep '\${' k8s/overlays/prod/*.yaml` 自查 |
| `Permission denied` 写 `/data` | 确认 `securityContext.fsGroup: 65534` 已设置 |
| `IP address mismatch, certificate is not valid for '<ip>'` | OSS 模式的已知限制，见步骤 4.2「关于 AMR_SSL_VERIFY」 |
| `does not have access to ingest data for the data collection rule` | 步骤 2.2 的 `Monitoring Metrics Publisher` 未授予或未生效（等 1～5 分钟）；Contributor 不隐含该权限 |
| `InvalidTransformQuery: Undefined symbol: <列名>` | 步骤 1.3 的 transformKql 与 stream-declarations / 表结构不一致 |
| Log Analytics 无数据但 Pod 无报错 | 先看 `/data/slowquery.jsonl` 有没有行：有行说明采集正常，问题在上报侧 |
| Workbook 集群下拉为空 | 确认 `ClusterName` 列已加入表结构；等待首批数据写入（约 5~10 分钟） |

---

## 环境变量参考

以下变量通过 `clusters.json` 中的字段设置（StatefulSet 模式），或直接通过环境变量设置（单集群本地运行）：

| 键名 | 默认值 | 说明 |
|---|---|---|
| `AMR_CLUSTER_NAME` | `AMR_HOST` 的值 | 集群友好名称，显示在 Workbook 下拉框 |
| `AMR_HOST` | — | Redis 实例主机名（必填） |
| `AMR_PORT` | `10000` | Redis 端口 |
| `AMR_ACCESS_KEY` | — | Redis 访问密钥 |
| `AMR_CLUSTER_POLICY` | `enterprise` | `oss` 或 `enterprise`，须与集群实际 clustering policy 一致 |
| `AMR_SSL_VERIFY` | `true` | `false` 跳过证书验证；OSS 模式下目前只能设为 `false`，见步骤 4.2 |
| `POLL_INTERVAL_SECONDS` | `60` | 轮询间隔（秒），可在 shared-secret 中全局设置 |
| `SLOWLOG_BATCH_SIZE` | `128` | 每次 SLOWLOG GET 的最大条数 |
| `DCE_ENDPOINT` | — | Data Collection Endpoint URL（必填，在 shared-secret 中设置） |
| `DCR_RULE_ID` | — | DCR immutableId（必填，在 shared-secret 中设置） |
