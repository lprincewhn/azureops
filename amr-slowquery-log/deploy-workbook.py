#!/usr/bin/env python3
"""Deploy Azure Monitor Workbook for AMR slow query analysis."""

import hashlib
import json
import os
import sys
import uuid

from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential

load_dotenv()
from azure.mgmt.applicationinsights import ApplicationInsightsManagementClient
from azure.mgmt.applicationinsights.models import Workbook

SUBSCRIPTION_ID = os.environ["SUBSCRIPTION_ID"]
RESOURCE_GROUP  = os.environ["RESOURCE_GROUP"]
WORKSPACE_NAME  = os.environ["WORKSPACE_NAME"]
LOCATION        = os.environ["LOCATION"]
WORKSPACE_ID = (
    f"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}"
    f"/providers/Microsoft.OperationalInsights/workspaces/{WORKSPACE_NAME}"
)

# Deterministic GUID so re-runs update rather than duplicate
WORKBOOK_GUID = str(uuid.UUID(hashlib.md5(b"amr-slowquery-workbook").hexdigest()))


def kql(*lines: str) -> str:
    return "\n".join(lines)


BASE_FILTER = (
    "| where TimeGenerated {timeRange}\n"
    "| where Duration_ms >= toint('{minDurationMs}')\n"
    "| where '{commandFilter}' == '' or Command contains '{commandFilter}'\n"
    "| where '{clusterName}' == 'value:All' or column_ifexists('ClusterName', '') == '{clusterName}'"
)


def query_item(title, query, visualization, size=0, extra=None, custom_width=None):
    item = {
        "type": 3,
        "content": {
            "version": "KqlItem/1.0",
            "query": query,
            "size": size,
            "title": title,
            "queryType": 0,
            "resourceType": "microsoft.operationalinsights/workspaces",
            "crossComponentResources": [WORKSPACE_ID],
            "visualization": visualization,
        },
    }
    if extra:
        item["content"].update(extra)
    if custom_width is not None:
        item["customWidth"] = str(custom_width)
    return item


def build_workbook() -> dict:
    heat = {"formatter": 8, "formatOptions": {"palette": "yellowOrangeRed", "min": 0}}
    return {
        "version": "Notebook/1.0",
        "items": [
            # ── Parameters ────────────────────────────────────────────────────
            {
                "type": 9,
                "content": {
                    "version": "KqlParameterItem/1.0",
                    "crossComponentResources": [WORKSPACE_ID],
                    "parameters": [
                        {
                            "id": "p1",
                            "version": "KqlParameterItem/1.0",
                            "name": "timeRange",
                            "label": "时间范围",
                            "type": 4,
                            "value": {"durationMs": 86400000},
                            "typeSettings": {
                                "allowCustom": True,
                                "selectableValues": [
                                    {"durationMs": 3600000},
                                    {"durationMs": 14400000},
                                    {"durationMs": 43200000},
                                    {"durationMs": 86400000},
                                    {"durationMs": 172800000},
                                    {"durationMs": 604800000},
                                ],
                            },
                        },
                        {
                            "id": "p2",
                            "version": "KqlParameterItem/1.0",
                            "name": "minDurationMs",
                            "label": "最小耗时 (ms)",
                            "type": 1,
                            "value": "0",
                            "typeSettings": {"paramValidationRules": [{"regExp": "^\\d+$", "match": True}]},
                        },
                        {
                            "id": "p3",
                            "version": "KqlParameterItem/1.0",
                            "name": "commandFilter",
                            "label": "命令关键字",
                            "type": 1,
                            "value": "",
                        },
                        {
                            "id": "p4",
                            "version": "KqlParameterItem/1.0",
                            "name": "clusterName",
                            "label": "集群",
                            "type": 2,
                            "isRequired": False,
                            "multiSelect": False,
                            "query": "AMRSlowQuery_CL | distinct ClusterName | sort by ClusterName asc",
                            "value": "value:All",
                            "typeSettings": {
                                "additionalResourceOptions": ["value:All"],
                                "showDefault": False,
                            },
                            "defaultValue": "value:All",
                            "queryType": 0,
                            "resourceType": "microsoft.operationalinsights/workspaces",
                            "crossComponentResources": [WORKSPACE_ID],
                        },
                    ],
                },
            },
            # ── Title ─────────────────────────────────────────────────────────
            {
                "type": 1,
                "content": {
                    "json": (
                        "# Azure Managed Redis 慢查询分析\n"
                        "数据源：`AMRSlowQuery_CL`\n\n---"
                    )
                },
            },

            # ════════════════════════════════════════════════════════════════
            # 一、跨集群概览
            # ════════════════════════════════════════════════════════════════
            {"type": 1, "content": {"json": "## 跨集群概览"}},

            # KPI tiles — 5 指标
            query_item(
                title="概览",
                query=kql(
                    "AMRSlowQuery_CL",
                    BASE_FILTER,
                    "| summarize v = count()",
                    "| project Metric = 'Total Queries', Value = tostring(v)",
                    "| union (AMRSlowQuery_CL", BASE_FILTER,
                    "  | summarize v = dcount(ClusterName)",
                    "  | project Metric = 'Active Clusters', Value = tostring(v))",
                    "| union (AMRSlowQuery_CL", BASE_FILTER,
                    "  | summarize v = round(avg(Duration_ms), 2)",
                    "  | project Metric = 'Avg Duration (ms)', Value = tostring(v))",
                    "| union (AMRSlowQuery_CL", BASE_FILTER,
                    "  | summarize v = round(percentile(Duration_ms, 99), 2)",
                    "  | project Metric = 'P99 Duration (ms)', Value = tostring(v))",
                    "| union (AMRSlowQuery_CL", BASE_FILTER,
                    "  | summarize v = dcount(tostring(split(Command, ' ')[0]))",
                    "  | project Metric = 'Unique Commands', Value = tostring(v))",
                ),
                visualization="tiles",
                size=4,
                extra={
                    "tileSettings": {
                        "showBorder": False,
                        "titleContent": {"columnMatch": "Metric", "formatter": 1},
                        "leftContent": {"columnMatch": "Value", "formatter": 12,
                                        "formatOptions": {"palette": "blue"}},
                    }
                },
            ),

            # 集群对比表
            query_item(
                title="集群对比",
                query=kql(
                    "AMRSlowQuery_CL",
                    BASE_FILTER,
                    "| summarize",
                    "    Count          = count(),",
                    "    AvgDuration_ms = round(avg(Duration_ms), 2),",
                    "    P95Duration_ms = round(percentile(Duration_ms, 95), 2),",
                    "    P99Duration_ms = round(percentile(Duration_ms, 99), 2),",
                    "    MaxDuration_ms = round(max(Duration_ms), 2),",
                    "    UniqueCommands = dcount(tostring(split(Command, ' ')[0]))",
                    "    by ClusterName",
                    "| sort by Count desc",
                ),
                visualization="table",
                extra={
                    "gridSettings": {
                        "sortBy": [{"itemKey": "Count", "sortOrder": 2}],
                        "labelSettings": [
                            {"columnId": "AvgDuration_ms", **heat},
                            {"columnId": "P95Duration_ms", **heat},
                            {"columnId": "P99Duration_ms", **heat},
                            {"columnId": "MaxDuration_ms", **heat},
                        ],
                    }
                },
            ),

            # 各集群慢查询次数柱状图
            query_item(
                title="各集群慢查询次数",
                query=kql(
                    "AMRSlowQuery_CL",
                    BASE_FILTER,
                    "| summarize Count = count() by ClusterName",
                    "| sort by Count desc",
                ),
                visualization="barchart",
                size=3,
            ),

            # ════════════════════════════════════════════════════════════════
            # 二、趋势分析
            # ════════════════════════════════════════════════════════════════
            {"type": 1, "content": {"json": "## 趋势分析"}},

            # 慢查询次数趋势（按集群分组）— 左半列
            query_item(
                title="慢查询次数趋势（按集群）",
                query=kql(
                    "AMRSlowQuery_CL",
                    BASE_FILTER,
                    "| summarize Count = count()",
                    "    by ClusterName, bin(TimeGenerated, {timeRange:grain})",
                ),
                visualization="timechart",
                custom_width=50,
            ),

            # 平均耗时趋势（按集群分组）— 右半列
            query_item(
                title="平均耗时趋势（按集群）",
                query=kql(
                    "AMRSlowQuery_CL",
                    BASE_FILTER,
                    "| summarize AvgDuration_ms = round(avg(Duration_ms), 2)",
                    "    by ClusterName, bin(TimeGenerated, {timeRange:grain})",
                ),
                visualization="timechart",
                custom_width=50,
            ),

            # ════════════════════════════════════════════════════════════════
            # 三、命令分析
            # ════════════════════════════════════════════════════════════════
            {"type": 1, "content": {"json": "## 命令分析"}},

            # Top 20 慢命令（全局汇总）— 左侧
            query_item(
                title="Top 20 慢命令",
                query=kql(
                    "AMRSlowQuery_CL",
                    BASE_FILTER,
                    "| extend Cmd = tostring(split(Command, ' ')[0])",
                    "| summarize",
                    "    Count          = count(),",
                    "    AvgDuration_ms = round(avg(Duration_ms), 2),",
                    "    P95Duration_ms = round(percentile(Duration_ms, 95), 2),",
                    "    MaxDuration_ms = round(max(Duration_ms), 2)",
                    "    by Cmd",
                    "| sort by Count desc",
                    "| take 20",
                ),
                visualization="table",
                extra={
                    "gridSettings": {
                        "sortBy": [{"itemKey": "Count", "sortOrder": 2}],
                        "labelSettings": [
                            {"columnId": "AvgDuration_ms", **heat},
                            {"columnId": "MaxDuration_ms", **heat},
                        ],
                    }
                },
                custom_width=70,
            ),

            # 命令分布饼图 — 右侧
            query_item(
                title="命令分布 (Top 10)",
                query=kql(
                    "AMRSlowQuery_CL",
                    BASE_FILTER,
                    "| extend Cmd = tostring(split(Command, ' ')[0])",
                    "| summarize Count = count() by Cmd",
                    "| sort by Count desc",
                    "| take 10",
                ),
                visualization="piechart",
                size=3,
                custom_width=30,
            ),

            # 各集群 Top 命令交叉表
            query_item(
                title="各集群 Top 命令",
                query=kql(
                    "AMRSlowQuery_CL",
                    BASE_FILTER,
                    "| extend Cmd = tostring(split(Command, ' ')[0])",
                    "| summarize",
                    "    Count          = count(),",
                    "    AvgDuration_ms = round(avg(Duration_ms), 2),",
                    "    MaxDuration_ms = round(max(Duration_ms), 2)",
                    "    by ClusterName, Cmd",
                    "| sort by ClusterName asc, Count desc",
                ),
                visualization="table",
                extra={
                    "gridSettings": {
                        "labelSettings": [
                            {"columnId": "AvgDuration_ms", **heat},
                            {"columnId": "MaxDuration_ms", **heat},
                        ],
                    }
                },
            ),

            # ════════════════════════════════════════════════════════════════
            # 四、耗时分布
            # ════════════════════════════════════════════════════════════════
            {"type": 1, "content": {"json": "## 耗时分布"}},

            # 整体耗时分布 — 左半列
            query_item(
                title="耗时分布（整体）",
                query=kql(
                    "AMRSlowQuery_CL",
                    BASE_FILTER,
                    "| extend DurationBucket = case(",
                    "    Duration_ms < 10,   '1: < 10ms',",
                    "    Duration_ms < 50,   '2: 10-50ms',",
                    "    Duration_ms < 100,  '3: 50-100ms',",
                    "    Duration_ms < 500,  '4: 100-500ms',",
                    "    Duration_ms < 1000, '5: 500ms-1s',",
                    "                        '6: > 1s')",
                    "| summarize Count = count() by DurationBucket",
                    "| sort by DurationBucket asc",
                ),
                visualization="barchart",
                size=3,
                custom_width=50,
            ),

            # 各集群耗时分布对比 — 右半列
            query_item(
                title="耗时分布（按集群）",
                query=kql(
                    "AMRSlowQuery_CL",
                    BASE_FILTER,
                    "| extend DurationBucket = case(",
                    "    Duration_ms < 10,   '1: < 10ms',",
                    "    Duration_ms < 50,   '2: 10-50ms',",
                    "    Duration_ms < 100,  '3: 50-100ms',",
                    "    Duration_ms < 500,  '4: 100-500ms',",
                    "    Duration_ms < 1000, '5: 500ms-1s',",
                    "                        '6: > 1s')",
                    "| summarize Count = count() by DurationBucket, ClusterName",
                    "| sort by DurationBucket asc",
                ),
                visualization="barchart",
                size=3,
                custom_width=50,
            ),

            # ════════════════════════════════════════════════════════════════
            # 五、客户端 & 分片
            # ════════════════════════════════════════════════════════════════
            {"type": 1, "content": {"json": "## 分片分布"}},

            # 分片分布（含集群）
            query_item(
                title="分片分布",
                query=kql(
                    "AMRSlowQuery_CL",
                    BASE_FILTER,
                    "| where Node != ''",
                    "| summarize",
                    "    Count          = count(),",
                    "    AvgDuration_ms = round(avg(Duration_ms), 2)",
                    "    by ClusterName, Node",
                    "| sort by ClusterName asc, Count desc",
                ),
                visualization="table",
                size=3,
                extra={
                    "gridSettings": {
                        "labelSettings": [
                            {"columnId": "AvgDuration_ms", **heat},
                        ],
                    }
                },
            ),

            # ════════════════════════════════════════════════════════════════
            # 六、明细查询
            # ════════════════════════════════════════════════════════════════
            {"type": 1, "content": {"json": "## 明细查询"}},

            query_item(
                title="慢查询明细（按耗时降序）",
                query=kql(
                    "AMRSlowQuery_CL",
                    BASE_FILTER,
                    "| project",
                    "    TimeGenerated,",
                    "    ClusterName,",
                    "    Duration_ms,",
                    "    Command,",
                    "    RedisHost,",
                    "    Node,",
                    "    SlowlogId",
                    "| sort by Duration_ms desc",
                ),
                visualization="table",
                extra={
                    "gridSettings": {
                        "filter": True,
                        "labelSettings": [
                            {"columnId": "Duration_ms", **heat},
                        ],
                    }
                },
            ),
        ],
        "fallbackResourceIds": [WORKSPACE_ID],
        "$schema": "https://github.com/microsoft/Application-Insights-Workbooks/blob/master/schema/workbook.json",
    }


def main():
    workbook_content = build_workbook()
    serialized = json.dumps(workbook_content, ensure_ascii=False)

    credential = DefaultAzureCredential()
    client = ApplicationInsightsManagementClient(credential, SUBSCRIPTION_ID)

    workbook = Workbook(
        location=LOCATION,
        kind="shared",
        display_name="AMR 慢查询分析",
        serialized_data=serialized,
        version="1.0",
        source_id=WORKSPACE_ID,
        category="workbook",
    )

    print(f"Deploying workbook {WORKBOOK_GUID} ...")
    result = client.workbooks.create_or_update(
        resource_group_name=RESOURCE_GROUP,
        resource_name=WORKBOOK_GUID,
        workbook_properties=workbook,
    )
    print(f"Done: {result.name}")

    resource_id = (
        f"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}"
        f"/providers/microsoft.insights/workbooks/{WORKBOOK_GUID}"
    )
    print(f"\n直达 URL:\nhttps://portal.azure.com/#resource{resource_id}")
    print(
        f"\n或从 Workspace 进入:\n"
        f"https://portal.azure.com/#resource/subscriptions/{SUBSCRIPTION_ID}"
        f"/resourceGroups/{RESOURCE_GROUP}/providers/microsoft.operationalinsights"
        f"/workspaces/{WORKSPACE_NAME}/workbooks"
    )


if __name__ == "__main__":
    main()
