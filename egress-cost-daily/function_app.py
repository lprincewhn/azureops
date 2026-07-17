"""每日出网流量费用采集 Function。

Timer 触发 -> 调用 Azure Cost Management Query API 拉取"前一天"的出网
(Bandwidth / Data Transfer Out)费用 -> 触发指定 Action Group 通知。

鉴权使用 Function App 的托管标识（Managed Identity），需要在目标订阅上
授予 "Cost Management Reader" 与 "Monitoring Contributor"（触发 Action Group
测试通知所需）角色。
"""
import datetime
import json
import logging
import os
import time

import azure.functions as func
import requests
from azure.communication.email import EmailClient
from azure.identity import DefaultAzureCredential

app = func.FunctionApp()

ARM_ENDPOINT = "https://management.azure.com"
ARM_SCOPE = "https://management.azure.com/.default"

# 计划：每天 UTC 02:00 运行，拉取前一天的费用。
CRON_SCHEDULE = os.environ.get("SCHEDULE", "0 0 2 * * *")


def _get_token(credential: DefaultAzureCredential) -> str:
    return credential.get_token(ARM_SCOPE).token


def _yesterday_range():
    """返回前一天的起止时间（UTC，ISO8601）。"""
    today = datetime.datetime.now(datetime.timezone.utc).date()
    start = today - datetime.timedelta(days=1)
    start_dt = f"{start.isoformat()}T00:00:00+00:00"
    end_dt = f"{start.isoformat()}T23:59:59+00:00"
    return start.isoformat(), start_dt, end_dt


def query_egress_cost(token: str, subscription_id: str, start_dt: str, end_dt: str):
    """查询前一天出网流量费用，按 MeterSubCategory 分组返回明细与合计。"""
    scope = f"/subscriptions/{subscription_id}"
    url = f"{ARM_ENDPOINT}{scope}/providers/Microsoft.CostManagement/query?api-version=2023-11-01"

    # 允许通过环境变量覆盖过滤维度（默认按计费类别 = Bandwidth 捕获出网流量）。
    meter_category = os.environ.get("METER_CATEGORY", "Bandwidth")

    body = {
        "type": "ActualCost",
        "timeframe": "Custom",
        "timePeriod": {"from": start_dt, "to": end_dt},
        "dataset": {
            "granularity": "None",
            "aggregation": {
                "totalCost": {"name": "PreTaxCost", "function": "Sum"}
            },
            "grouping": [
                {"type": "Dimension", "name": "MeterSubCategory"}
            ],
            "filter": {
                "dimensions": {
                    "name": "MeterCategory",
                    "operator": "In",
                    "values": [meter_category],
                }
            },
        },
    }

    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        data=json.dumps(body),
        timeout=60,
    )
    # Cost Management API 限流较严，遇 429 按 Retry-After 退避重试。
    attempts = 0
    while resp.status_code == 429 and attempts < 4:
        wait = int(resp.headers.get("Retry-After", 20))
        wait = min(wait, 60)
        logging.warning("Cost API 429 限流，%s 秒后重试（第 %s 次）", wait, attempts + 1)
        time.sleep(wait)
        resp = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            data=json.dumps(body),
            timeout=60,
        )
        attempts += 1
    resp.raise_for_status()
    data = resp.json()

    columns = [c["name"] for c in data["properties"]["columns"]]
    rows = data["properties"]["rows"]

    cost_idx = columns.index("PreTaxCost")
    sub_idx = columns.index("MeterSubCategory") if "MeterSubCategory" in columns else None
    cur_idx = columns.index("Currency") if "Currency" in columns else None

    total = 0.0
    currency = "USD"
    breakdown = []
    for row in rows:
        cost = float(row[cost_idx])
        total += cost
        sub = row[sub_idx] if sub_idx is not None else "N/A"
        if cur_idx is not None:
            currency = row[cur_idx]
        breakdown.append({"meterSubCategory": sub, "cost": round(cost, 4)})

    return {"total": round(total, 4), "currency": currency, "breakdown": breakdown}


def trigger_action_group(token: str, action_group_id: str, subscription_id: str,
                         report: dict, day: str):
    """通过 Action Group 测试通知 API 触发已配置的接收方（邮件/短信/Webhook 等）。

    该 API 会读取现有 Action Group 的接收方并真实投递一次通知。
    """
    # 读取 Action Group 的接收方配置
    ag_url = f"{ARM_ENDPOINT}{action_group_id}?api-version=2023-01-01"
    ag_resp = requests.get(
        ag_url, headers={"Authorization": f"Bearer {token}"}, timeout=30
    )
    ag_resp.raise_for_status()
    props = ag_resp.json().get("properties", {})

    summary = (
        f"{day} 出网流量费用: {report['total']} {report['currency']}"
    )

    def _clean(receivers):
        # createNotifications 不接受只读字段（如 status），需剔除后回传。
        return [
            {k: v for k, v in r.items() if k != "status"}
            for r in (receivers or [])
        ]

    # alertType 只能取固定枚举，成本场景使用 actualcostbudget。
    payload = {
        "alertType": "actualcostbudget",
        "emailReceivers": _clean(props.get("emailReceivers")),
        "smsReceivers": _clean(props.get("smsReceivers")),
        "webhookReceivers": _clean(props.get("webhookReceivers")),
        "armRoleReceivers": _clean(props.get("armRoleReceivers")),
        "azureAppPushReceivers": _clean(props.get("azureAppPushReceivers")),
        "voiceReceivers": _clean(props.get("voiceReceivers")),
    }

    notify_url = (
        f"{ARM_ENDPOINT}/subscriptions/{subscription_id}"
        f"/providers/Microsoft.Insights/createNotifications?api-version=2023-01-01"
    )
    resp = requests.post(
        notify_url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        data=json.dumps(payload),
        timeout=60,
    )
    resp.raise_for_status()
    logging.info("已触发 Action Group 通知: %s", summary)


def post_webhook(webhook_url: str, report: dict, day: str):
    """可选：直接把富文本报告 POST 到 Webhook（如 Teams/Slack/Logic App）。"""
    payload = {
        "date": day,
        "totalCost": report["total"],
        "currency": report["currency"],
        "breakdown": report["breakdown"],
        "text": f"{day} 出网流量费用合计: {report['total']} {report['currency']}",
    }
    resp = requests.post(webhook_url, json=payload, timeout=30)
    resp.raise_for_status()
    logging.info("已推送 Webhook 报告")


def _build_email_html(report: dict, day: str) -> str:
    """构造含真实金额与分项明细的 HTML 邮件正文。"""
    rows = "".join(
        f"<tr><td style='padding:4px 12px;border:1px solid #ddd'>{b['meterSubCategory']}</td>"
        f"<td style='padding:4px 12px;border:1px solid #ddd;text-align:right'>{b['cost']}</td></tr>"
        for b in report["breakdown"]
    ) or "<tr><td colspan='2' style='padding:4px 12px;border:1px solid #ddd'>无数据</td></tr>"
    return f"""
    <div style="font-family:Segoe UI,Arial,sans-serif;font-size:14px;color:#333">
      <h3>出网流量费用日报 · {day}</h3>
      <p>合计：<b>{report['total']} {report['currency']}</b></p>
      <table style="border-collapse:collapse;border:1px solid #ddd">
        <tr style="background:#f3f3f3">
          <th style="padding:4px 12px;border:1px solid #ddd;text-align:left">计费子类别</th>
          <th style="padding:4px 12px;border:1px solid #ddd;text-align:right">费用 ({report['currency']})</th>
        </tr>
        {rows}
      </table>
      <p style="color:#888;font-size:12px">由 Azure Functions 自动生成（数据来源：Cost Management）。</p>
    </div>
    """


def send_email(conn_str: str, sender: str, recipients: list, report: dict, day: str):
    """通过 Azure Communication Services 直接发送带真实金额的邮件。"""
    client = EmailClient.from_connection_string(conn_str)
    message = {
        "senderAddress": sender,
        "content": {
            "subject": f"[出网费用] {day} 合计 {report['total']} {report['currency']}",
            "html": _build_email_html(report, day),
        },
        "recipients": {"to": [{"address": a.strip()} for a in recipients if a.strip()]},
    }
    poller = client.begin_send(message)
    result = poller.result()
    logging.info("邮件已发送，status=%s id=%s", result.get("status"), result.get("id"))


@app.timer_trigger(schedule=CRON_SCHEDULE, arg_name="timer", run_on_startup=False,
                   use_monitor=True)
def egress_cost_daily(timer: func.TimerRequest) -> None:
    if timer.past_due:
        logging.warning("Timer 已过期，仍继续执行。")

    subscription_id = os.environ["SUBSCRIPTION_ID"]
    action_group_id = os.environ.get("ACTION_GROUP_ID")
    webhook_url = os.environ.get("ACTION_GROUP_WEBHOOK_URL")
    acs_conn = os.environ.get("ACS_CONNECTION_STRING")
    email_sender = os.environ.get("EMAIL_SENDER")
    email_recipients = os.environ.get("EMAIL_RECIPIENTS", "")

    credential = DefaultAzureCredential()
    token = _get_token(credential)

    day, start_dt, end_dt = _yesterday_range()
    logging.info("拉取 %s 的出网流量费用", day)

    report = query_egress_cost(token, subscription_id, start_dt, end_dt)
    logging.info("费用结果: %s", json.dumps(report, ensure_ascii=False))

    sent = False
    if acs_conn and email_sender and email_recipients:
        send_email(acs_conn, email_sender, email_recipients.split(","), report, day)
        sent = True
    if webhook_url:
        post_webhook(webhook_url, report, day)
        sent = True
    if action_group_id:
        trigger_action_group(token, action_group_id, subscription_id, report, day)
        sent = True

    if not sent:
        logging.error("未配置任何通知渠道（ACS 邮件 / Webhook / Action Group）。")
