# Azure Metric Stream

两个独立镜像共用 `contract.py`：PlatformTelemetry DCR 将受支持的资源指标导出至 Event Hubs；Batch 采集器按订阅、区域、资源类型拉取 LB / Application Gateway 指标，将响应转换为同一种 **DCR Event Hubs 指标事件格式**后发送到同一 Hub；exporter 用独立 consumer group 读取，暴露 Prometheus `/metrics`。本目录不含目标环境标识、访问凭据、订阅 ID 或部署结果。

如果要采集全指标，Storage 可选择 `Microsoft.storage/storageaccounts:Metrics-Group-All` 的 DCR stream；**LB 和 Application Gateway 不在 Platform Metrics Export DCR 支持清单**，它们的全指标采集仍走 Metrics Batch API，而非改为 DCR。Batch 的“全指标”指每个资源的 metricDefinitions 中所有指标名，逐指标使用支持的主聚合与所有可用维度；不意味着强制采集每项指标的全部非主要聚合。完整导出会放大 Event Hubs 吞吐、Prometheus 序列数和按样本计费，激活 DCR 关联/Batch 任务前须估算实际维度基数并评估预算及 `MAX_SERIES`。

## 消息与语义

每条指标记录是一个 JSON 对象，至少包含 `resourceId`、`metricName`、`unit`、`time`（带时区的 ISO 8601）、`timeGrain`、`dimension`（原始维度名值对象）和 `Average` / `Count` / `Maximum` / `Minimum` / `Total` 中一个或多个聚合值。一个 Event Hubs 消息可包含单个对象、对象数组，或多行以换行分隔的 JSON 对象（DCR 实际投递格式）；所有记录解析成功后才推进该消息的检查点。Batch 生成的记录额外携带 `schemaVersion: 1` 和 `queryWindow`；DCR 原始记录可以不含这两个字段。兼容官方 [Event Hubs 示例格式](https://learn.microsoft.com/en-us/azure/azure-monitor/data-collection/metrics-export-create#event-hubs)；真实记录的完整字段和维度仍须核对。

每个聚合映射为 `azure_platform_metric` **Gauge**，标签为 `resource_id`、`metric_name`、`aggregation`、`unit`、`time_grain` 和所有 `dimension_<规范化原名>`。`Total` 是该时间区间的值，**不是**累计 Counter，不能直接使用 `rate()`；`Count` 也按区间 Gauge 处理。标签名规范化碰撞、非法数值、缺字段会使事件失败且不会提交消费位点。按原始采样时间保留最新序列；同一序列的旧样本/重放不会覆盖新样本。过期序列自动删除，旧到达样本不会复活。`/metrics` 是最新值快照，**不保留补录的历史时间戳**；需要逐分钟历史和乱序回补时须另建 Remote Write 链路。

## 构建和本地验证

```bash
cd azure-metric-stream
python3 -m unittest discover -s tests -v
docker build -f Dockerfile.exporter -t azure-metric-exporter:local .
docker build -f Dockerfile.batch -t azure-metric-batch:local .
```

无需 Azure 的端到端本地测试将合成的 DCR 格式事件与 Batch API 格式响应送入相同 `contract.samples` → `Store` → `/metrics` 路径；这**不代表**真实 Azure 链路验收。真实验收须在部署审批后核对两个来源的资源 ID、具体维度、聚合值和原始采集时间，并在 jump-server Prometheus 查询中核对标签、时间与缺口。

## Exporter 配置

`EVENTHUB_NAMESPACE`（完整 `<namespace>.servicebus.windows.net`）、`EVENTHUB_NAME`、`EVENTHUB_CONSUMER_GROUP`（独立）、`CHECKPOINT_STORAGE_ACCOUNT`、`CHECKPOINT_CONTAINER` 为必填；可选 `PORT=8000`、`SERIES_TTL_SECONDS=900`、`MAX_SERIES=10000`。通过 `DefaultAzureCredential` 使用 Container App 托管身份；读取身份需要 Hub 范围 `Azure Event Hubs Data Receiver` 与检查点 Blob 容器范围 `Storage Blob Data Contributor`。检查点容器须先创建且允许该身份访问。认证与网络错误由 SDK 报错；处理失败时 `/health` 返回 503、`azure_metric_stream_healthy` 变 0，错误事件不提交 checkpoint。告警至少覆盖健康、`azure_metric_stream_last_event_timestamp_seconds` 与 Event Hubs 消费积压。

**抓取必须只指向单副本：**Container App 设 `minReplicas=maxReplicas=1`，不可缩零；多个副本共享同一个负载均衡 `/metrics` 地址会随机得到不完整序列。使用私有入口、VNet 与受控 DNS/网络路径使 jump-server 可达；只允许该 Prometheus 抓取。不可将示例开放为不受限的公网入口。Azure 凭据由托管身份提供，不用 Event Hubs 连接字符串。

## Batch 采集器配置

容器执行 `python3 batch_collector.py`。通过 `BATCH_CONFIG_JSON` 或 `BATCH_CONFIG_PATH` **二选一**提供配置（文件需自行挂载，不提交实际资源清单）：

```json
{
  "resources": [
    {
      "id": "/subscriptions/<subscription-id>/resourceGroups/<resource-group>/providers/Microsoft.Network/loadBalancers/<resource-name>",
      "region": "<region>"
    }
  ],
  "metricSpecs": [
    {
      "resourceType": "Microsoft.Network/loadBalancers",
      "metricNamespace": "Microsoft.Network/loadBalancers",
      "metricNames": ["<metric-name>"],
      "aggregation": "Average",
      "interval": "PT1M",
      "dimensions": ["<dimension-name>"],
      "filter": "<dimension-name> eq '*'",
      "top": 100
    }
  ],
  "eventHub": {
    "fullyQualifiedNamespace": "<namespace>.servicebus.windows.net",
    "name": "<hub-name>"
  },
  "stateBlob": {
    "accountName": "<storage-account>",
    "containerName": "<checkpoint-container>",
    "blobName": "batch/state.json"
  }
}
```

App Gateway 需另配 `Microsoft.Network/applicationGateways` 资源和对应 metricSpec。以目标资源 `metricDefinitions` 核对指标名、聚合和可用维度；每条 spec 仅支持一种聚合和 `PT1M`，维度须显式 wildcard 过滤，`top` 取 1–10000。**达到 `top` 即拒绝推进高水位**：缩小维度过滤器、按维度值拆分查询或调整经过验证的上限后再重放，不能把它当分页。按订阅/区域/类型每批至多 50 个 ID；写失败或 429/5xx 经重试仍失败时不推进高水位，重叠窗口重放在 exporter 端去重。

若同一类型的资源支持不同的指标（如全局 LB 与普通 LB），可在该指标的 `metricSpec` 中设置可选的 `resourceIds` 数组，仅查询 `resources` 中已列出的目标资源；不设置则查询该类型全部资源。全指标采集应根据每个资源的 metricDefinitions 为**每个指标**建立合适的 spec（主聚合、完整维度 wildcard 过滤与已验证 `top`）；不能把不同指标的维度集合混到一个请求，也不能将不支持某指标的资源加入该 spec。指标定义或资源清单变化时须重新核对并迁移高水位。

可选参数：`pollSeconds=60`、`initialLookbackSeconds=600`、`overlapSeconds=180`、`ingestLagSeconds=120`、`maxWindowSeconds=3600`、`httpTimeoutSeconds=30`、`httpMaxAttempts=5`。可选环境变量 `AZURE_CLIENT_ID` 指定用户分配身份、`LOG_LEVEL` 调整日志。Batch 身份须有被查询订阅的 `Monitoring Reader`、Event Hub 的 `Azure Event Hubs Data Sender`，以及检查点 Blob 容器的 `Storage Blob Data Contributor`；OAuth audience 为 Metrics Batch 专用地址。`stateBlob` 与 `statePath` 二选一：推荐前者以无 Azure Files 卷部署，需预建 Blob 容器、为容器应用配置到 Blob 的网络和私有 DNS 路径，并使用托管身份而非密钥；启动时创建空白检查点 Blob（如果尚不存在）并验证访问。每轮在 Blob 上获取 60 秒独占租约、每 20 秒续租，并在带租约和 ETag 条件的写入成功后推进水位；租约、鉴权、网络、发送或写入失败将使进程报错退出，由 Container Apps 重启，重复发送须依赖 exporter 去重。旧的 `statePath` 模式仍支持，但必须使用**持久单写入卷**。Batch Container App 配置 `minReplicas=maxReplicas=1`，不可缩零；额外副本不能同时持有 Blob 租约。资源/指标配置变更会触发高水位指纹校验失败，必须事先规划状态迁移/重放。告警包括容器重启、周期失败、`top` 触顶、限流、Event Hubs 发送失败与消费积压；轮询服务日志不能替代告警。

## 审批前检查与回滚

仓库只提供通用软件，不执行 Azure 或 jump-server 的写操作。变更前在议题明确目标订阅、选定 Storage Account / 地区、同区域 DCR 与 Event Hubs、DCR 系统身份 Sender、exporter Receiver / Blob 检查点权限、Batch 身份订阅级 Monitoring Reader 与 Sender、镜像仓库/Container App 网络、运行规格、Event Hubs 吞吐与保留期、指标及维度、费用估算、抓取目标和告警，然后取得成员明确批准。DCR、Event Hubs 和被监测资源必须同一区域；LB / Application Gateway 当前不在 DCR 支持列表。真实环境清单只保存在 **AzureOps 项目描述的 Markdown**，不进代码仓库。回滚顺序：停 Batch 采集与 Prometheus job、移除 DCR 关联/规则、停 Container App、撤销专用角色授权；检查点与 Event Hubs 留存由审批后的数据保留策略处理，不盲目删除。

参考：[Platform Metrics Export](https://learn.microsoft.com/en-us/azure/azure-monitor/data-collection/metrics-export-create)、[支持资源与区域](https://learn.microsoft.com/en-us/azure/azure-monitor/data-collection/metrics-export-reference)、[Metrics Batch API](https://learn.microsoft.com/en-us/rest/api/monitor/metrics-batch/batch?view=rest-monitor-2023-10-01)。
