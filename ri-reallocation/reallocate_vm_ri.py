#!/usr/bin/env python3
"""生成 RI 经济收益或损失重新分摊后的明细副本，不修改源 CSV。

分摊规则：
1. RI 使用记录：pricingModel=Reservation、chargeType=Usage、reservationId
   等于命令行指定的 RI。
2. 只处理 meterCategory=Virtual Machines 的记录。
3. 原 RI 使用记录的 tags 和 ResourceId 保持不变。
4. 原 RI 使用记录不是目标项目时：
     allocatedCostInBillingCurrency = 成本基准 + RI 净收益/损失
5. 目标项目的 VM 记录按费用比例承接 RI 净收益/损失（若某条 RI 使用记录自身命中
   目标，则加回后以全价一并参与该目标分摊）：
     allocatedCostInBillingCurrency = 成本基准 - 分摊金额

正收益降低目标项目费用；负收益代表 RI 超额成本，会增加目标项目费用。
两种场景的整体金额均保持不变。
"""

from __future__ import annotations

import argparse
import csv
import glob
import io
import json
import re
import time
import urllib.request
import urllib.parse
import zipfile
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, NamedTuple


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(
        description="生成 RI 费用重新分摊后的 CSV 明细副本。"
    )
    parser.add_argument("inputs", nargs="+", help="源 CSV 文件或 glob")
    parser.add_argument(
        "--output-dir",
        default="ri-reallocated",
        help="输出目录，默认：ri-reallocated",
    )
    parser.add_argument(
        "--reservations-file",
        required=True,
        help=(
            "预留分摊定义文件（reservations.json）。按每个预留的 bindings 将 RI "
            "优惠收益按 boundQuantity 比例分摊到多个项目（project）"
        ),
    )
    parser.add_argument(
        "--project-tag-key",
        required=True,
        help=(
            "读取资源标签中项目名、并把 binding 的 project 映射为目标标签时使用的"
            "标签键（必填，无默认值），例如 projname 或 costcenter"
        ),
    )
    parser.add_argument(
        "--amount-field",
        default="costInBillingCurrency",
        choices=(
            "costInBillingCurrency",
            "costInBillingCurrencyAfterActualReconciliation",
        ),
        help=(
            "RI 经济收益/损失的成本基准字段；使用 allocate_ri_difference.py "
            "输出时指定 costInBillingCurrencyAfterActualReconciliation"
        ),
    )
    parser.add_argument(
        "--price-sheet-file",
        help=(
            "Azure Price Sheet CSV/ZIP/JSON。未指定时通过 Azure Cost Management SDK "
            "按账单中的 billingAccountId/billingProfileId/invoiceId 自动下载"
        ),
    )
    parser.add_argument(
        "--invoice-id",
        help=(
            "下载 MCA/MPA Price Sheet 时使用的发票 ID。未指定且账单行的 "
            "invoiceId 不完整时，自动回退为按 Billing Profile 下载"
        ),
    )
    parser.add_argument(
        "--billing-account-name",
        help=(
            "完整 MCA/MPA Billing Account Name。未指定时根据账单中的短 "
            "billingAccountId 通过 Microsoft.Billing API 自动解析"
        ),
    )
    parser.add_argument(
        "--price-sheet-timeout",
        type=int,
        default=1800,
        help="等待 Azure 生成 Price Sheet 的最长秒数，默认：1800",
    )
    parser.add_argument(
        "--save-price-sheet-file",
        help="将自动下载的 Azure Price Sheet 原始 CSV/ZIP/JSON 保存到指定路径",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="只生成汇总文件，不生成明细副本",
    )
    return parser.parse_args()


def expand_inputs(patterns: list[str]) -> list[Path]:
    """展开文件路径或 glob，并校验输入文件存在。"""
    paths: list[Path] = []
    for pattern in patterns:
        matches = [Path(item) for item in glob.glob(pattern)]
        paths.extend(matches or [Path(pattern)])
    result = sorted({path.resolve() for path in paths})
    missing = [path for path in result if not path.is_file()]
    if missing:
        raise FileNotFoundError(
            "找不到输入文件：" + ", ".join(str(path) for path in missing)
        )
    return result


def parse_tags(raw: str) -> dict[str, Any]:
    """解析账单中的 JSON 标签字段。"""
    if not raw.strip():
        return {}
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("tags 字段不是 JSON 对象")
    return value


# reservationId → 分摊目标列表，每个目标为 ((标签键, 标签值), 权重)。
RiTargets = dict[str, list[tuple[tuple[str, str], Decimal]]]
# reservationId → RI 收益匹配模式（"model" 或 "flex-group"）。
RiModes = dict[str, str]
# reservationId → 权重分母（boundTotal）。绑定项目按 boundQuantity/boundTotal 分摊，
# 剩余 (boundTotal - Σ boundQuantity)/boundTotal 的收益留在原 RI 使用项目（其他项目）。
RiDenominators = dict[str, Decimal]
# reservationId → (appliedScopeType, normalized appliedScopeId).
RiScopes = dict[str, tuple[str, str]]


def _match_mode_from_flexibility(flexibility: Any) -> str:
    """根据预留的 flexibility 字段推导 RI 收益匹配模式。

    实例大小灵活性开启（``on``）时按灵活性组匹配（``flex-group``），
    否则按精确机型匹配（``model``）。
    """
    return "flex-group" if str(flexibility or "").strip().lower() == "on" else "model"


def _reservation_id_from_external(external: Any) -> str:
    """从 externalReservationId 中提取账单里使用的 reservationId。

    形如 ``/providers/microsoft.capacity/reservationOrders/<order>/reservations/<rid>``，
    取 ``/reservations/`` 之后的一段；没有该分段时按原值返回。这是账单
    ``reservationId`` 列使用的预留 GUID，权威来源；对象里的 ``id`` /
    ``bindings[].reservationId`` 通常是内部记录 UUID，与账单无关。
    """
    text = str(external or "").strip()
    if not text:
        return ""
    marker = "/reservations/"
    if marker in text:
        tail = text.rsplit(marker, 1)[1]
        return tail.strip("/").split("/")[0].strip()
    return text


def _normalize_ri_scope(item: dict[str, Any]) -> tuple[str, str]:
    """Normalize the Azure reservation benefit scope used to select receivers."""
    scope_type = str(item.get("appliedScopeType") or "Shared").strip().lower()
    scope_id = str(item.get("appliedScopeId") or "").strip().rstrip("/").lower()
    if scope_type == "shared":
        return ("shared", "")
    if scope_type in {"single", "managementgroup"}:
        if not scope_id:
            raise ValueError(
                f"{item.get('appliedScopeType')} scope reservation 缺少 appliedScopeId"
            )
        return (scope_type, scope_id)
    raise ValueError(
        f"暂不支持 appliedScopeType={item.get('appliedScopeType')!r}；"
        "仅支持 Shared、Single（订阅或资源组）和 ManagementGroup"
    )


def _row_subscription_id(row: dict[str, str]) -> str:
    """Read the subscription ID from its column or Azure resource ID."""
    explicit = str(
        row.get("SubscriptionId") or row.get("subscriptionId") or ""
    ).strip()
    if explicit:
        return explicit.lower()
    resource_id = str(row.get("ResourceId") or row.get("resourceId") or "").strip()
    match = re.match(r"^/subscriptions/([^/]+)(?:/|$)", resource_id, re.IGNORECASE)
    return match.group(1).lower() if match else ""


def _management_group_name(scope_id: str) -> str:
    """Extract a management group name from an Azure resource ID or bare name."""
    marker = "/managementgroups/"
    normalized = scope_id.strip().rstrip("/")
    lower = normalized.lower()
    if marker in lower:
        start = lower.rfind(marker) + len(marker)
        return normalized[start:].split("/", 1)[0]
    return normalized


def resolve_management_group_scopes_from_entities(
    scopes: RiScopes, credential: Any
) -> dict[tuple[str, str], frozenset[str]]:
    """Resolve management-group membership through the tenant entity hierarchy."""
    token = credential.get_token("https://management.azure.com/.default").token
    request = urllib.request.Request(
        "https://management.azure.com/providers/Microsoft.Management/"
        "getEntities?api-version=2020-05-01",
        data=json.dumps({"query": "", "view": "FullHierarchy"}).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        payload = json.load(response)
    subscriptions: list[tuple[str, set[str]]] = []
    for entity in payload.get("value", []):
        if not isinstance(entity, dict):
            continue
        if str(entity.get("type") or "").lower() != "/subscriptions":
            continue
        subscription_id = str(entity.get("name") or "").strip().lower()
        properties = entity.get("properties") or {}
        parent_chain = {
            str(parent).strip().lower()
            for parent in properties.get("parentNameChain") or []
        }
        if subscription_id:
            subscriptions.append((subscription_id, parent_chain))

    result: dict[tuple[str, str], frozenset[str]] = {}
    for scope in {item for item in scopes.values() if item[0] == "managementgroup"}:
        group_name = _management_group_name(scope[1]).lower()
        result[scope] = frozenset(
            subscription_id
            for subscription_id, parent_chain in subscriptions
            if group_name in parent_chain
        )
    return result


def resolve_management_group_scopes(
    scopes: RiScopes, client: Any | None = None
) -> dict[tuple[str, str], frozenset[str]]:
    """Resolve ManagementGroup scopes to descendant subscription IDs via Azure SDK."""
    management_group_scopes = {
        scope for scope in scopes.values() if scope[0] == "managementgroup"
    }
    if not management_group_scopes:
        return {}

    owns_client = client is None
    credential = None
    if client is None:
        try:
            from azure.identity import AzureCliCredential
            from azure.core.exceptions import HttpResponseError
            from azure.mgmt.managementgroups import ManagementGroupsMgmtClient
        except ImportError as exc:
            raise RuntimeError(
                "ManagementGroup scope 需要安装 requirements.txt 中的 Azure SDK"
            ) from exc
        credential = AzureCliCredential()
        client = ManagementGroupsMgmtClient(credential=credential)

    result: dict[tuple[str, str], frozenset[str]] = {}
    try:
        try:
            for scope in sorted(management_group_scopes):
                group_name = _management_group_name(scope[1])
                subscription_ids: set[str] = set()
                for descendant in client.management_groups.get_descendants(group_name):
                    descendant_type = str(
                        getattr(descendant, "type", None)
                        or (
                            descendant.get("type")
                            if isinstance(descendant, dict)
                            else ""
                        )
                    )
                    if descendant_type.lower() != (
                        "microsoft.management/managementgroups/subscriptions"
                    ).lower():
                        continue
                    name = str(
                        getattr(descendant, "name", None)
                        or (
                            descendant.get("name")
                            if isinstance(descendant, dict)
                            else ""
                        )
                    ).strip()
                    if name:
                        subscription_ids.add(name.lower())
                result[scope] = frozenset(subscription_ids)
        except HttpResponseError as exc:
            if (
                not owns_client
                or credential is None
                or getattr(exc, "status_code", None) != 403
            ):
                raise
            result = resolve_management_group_scopes_from_entities(
                scopes, credential
            )
    finally:
        if owns_client:
            client.close()
    return result


def row_matches_ri_scope(
    row: dict[str, str],
    scope: tuple[str, str],
    management_group_subscriptions: dict[
        tuple[str, str], frozenset[str]
    ] | None = None,
) -> bool:
    """Return whether a billing row is eligible for the reservation scope."""
    scope_type, scope_id = scope
    if scope_type == "shared":
        return True
    if scope_type == "managementgroup":
        subscriptions = (management_group_subscriptions or {}).get(scope)
        if subscriptions is None:
            raise ValueError(f"ManagementGroup scope {scope_id!r} 尚未解析")
        return _row_subscription_id(row) in subscriptions

    resource_id = str(row.get("ResourceId") or row.get("resourceId") or "").strip()
    normalized_resource_id = resource_id.rstrip("/").lower()
    if normalized_resource_id and (
        normalized_resource_id == scope_id
        or normalized_resource_id.startswith(scope_id + "/")
    ):
        return True

    subscription_match = re.fullmatch(
        r"/subscriptions/([^/]+)", scope_id, flags=re.IGNORECASE
    )
    if subscription_match:
        return _row_subscription_id(row) == subscription_match.group(1).lower()
    return False


def load_reservations_config(
    path: Path, project_tag_key: str
) -> tuple[RiTargets, RiModes, RiDenominators, RiScopes]:
    """读取 reservations.json，构建 reservationId → [(目标, 权重)] 映射、匹配模式及权重分母。

    每个预留按 ``bindings`` 拆分：``project`` 作为目标标签值（键由
    ``project_tag_key`` 指定，必填无默认），``boundQuantity`` 作为权重。
    同一预留内相同 project 的多个 binding 权重合并；权重非正的 binding 忽略；
    没有有效 binding 的预留跳过。RI 收益匹配模式由预留的 ``flexibility`` 字段
    推导（``on`` → flex-group，否则 model），无需命令行指定。

    权重分母取 ``boundTotal``：绑定项目各按 ``boundQuantity / boundTotal`` 分摊，
    剩余的 ``(boundTotal - Σ boundQuantity) / boundTotal`` 那部分收益不再搬走，
    留在实际使用该 RI 的原项目（其他项目）。当 ``boundTotal`` 缺失、非正或小于
    Σ boundQuantity 时，回退为 Σ boundQuantity（即无剩余、全额分摊到绑定项目）。

    返回 ``(ri_targets, ri_modes, ri_denominators, ri_scopes)``。
    """
    if not path.is_file():
        raise FileNotFoundError(f"找不到 reservations 文件：{path}")
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    if isinstance(data, dict):
        if isinstance(data.get("reservations"), list):
            items = data["reservations"]
        else:
            items = [data]
    elif isinstance(data, list):
        items = data
    else:
        raise ValueError("reservations 文件 JSON 顶层必须是对象或数组")

    key = (project_tag_key or "").strip()
    if not key:
        raise ValueError("project_tag_key 不能为空，请通过 --project-tag-key 指定")
    result: RiTargets = {}
    modes: RiModes = {}
    denominators: RiDenominators = {}
    scopes: RiScopes = {}
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("reservation 条目必须是对象")
        reservation_id = _reservation_id_from_external(
            item.get("externalReservationId")
        )
        if not reservation_id:
            reservation_id = str(item.get("reservationId") or "").strip()
        if not reservation_id:
            raise ValueError("reservation 条目缺少 reservationId/externalReservationId")

        weights: dict[str, Decimal] = {}
        for binding in item.get("bindings") or []:
            if not isinstance(binding, dict):
                continue
            code = str(binding.get("project") or "").strip()
            if not code:
                continue
            try:
                weight = Decimal(str(binding.get("boundQuantity", 0)))
            except InvalidOperation:
                weight = Decimal("0")
            if weight <= 0:
                continue
            weights[code] = weights.get(code, Decimal("0")) + weight
        if not weights:
            # 没有有效 binding（未绑定项目）的预留无法分摊，跳过。
            continue
        if reservation_id in result:
            raise ValueError(
                f"reservations 文件存在重复 reservationId：{reservation_id!r}"
            )
        result[reservation_id] = [
            ((key, code), weight) for code, weight in weights.items()
        ]
        modes[reservation_id] = _match_mode_from_flexibility(
            item.get("flexibility")
        )
        scopes[reservation_id] = _normalize_ri_scope(item)
        bound_sum = sum(weights.values(), Decimal("0"))
        try:
            bound_total = Decimal(str(item.get("boundTotal")))
        except (InvalidOperation, TypeError):
            bound_total = None
        # boundTotal 缺失/非正/小于已绑定权重之和时，回退为全额分摊（无其他项目剩余）。
        if bound_total is None or bound_total < bound_sum:
            bound_total = bound_sum
        denominators[reservation_id] = bound_total
    if not result:
        raise ValueError(
            "reservations 文件没有可用的 RI 分摊定义（缺少有效 bindings）"
        )
    return result, modes, denominators, scopes


def load_reservations_file(
    path: Path, project_tag_key: str
) -> tuple[RiTargets, RiModes, RiDenominators]:
    """Load reservation targets, match modes, and allocation denominators."""
    targets, modes, denominators, _scopes = load_reservations_config(
        path, project_tag_key
    )
    return targets, modes, denominators


def build_ri_targets(
    args: argparse.Namespace,
) -> tuple[RiTargets, RiModes, RiDenominators]:
    """构建 reservationId → [(目标, 权重)] 映射、匹配模式及权重分母。

    读取 --reservations-file 指定的 reservations.json，按每个预留的 bindings
    权重把一个 RI 分摊到多个项目（project），并按预留的 flexibility 字段
    推导 RI 收益匹配模式；权重分母取 boundTotal（详见 load_reservations_file）。
    """
    return load_reservations_file(Path(args.reservations_file), args.project_tag_key)


def _row_contributions(
    amount: Decimal,
    targets_list: list[tuple[tuple[str, str], Decimal]],
    denominator: Decimal,
) -> tuple[Decimal, list[tuple[tuple[str, str], Decimal]], str]:
    """计算单条 RI 使用记录的加回金额与各目标收益池贡献。

    ``denominator`` 为权重分母（boundTotal）。绑定项目各分得
    ``amount × boundQuantity / boundTotal``；加回金额 ``add_back`` 为各目标贡献之和，
    即 ``amount × Σ boundQuantity / boundTotal``。剩余部分不加回，留在原 RI 使用项目
    （其他项目）。``denominator`` 非正时按 Σ 权重全额分摊。

    返回 ``(add_back, contributions, label)``：``contributions`` 为 [(目标, 贡献)]，
    各贡献之和等于 ``add_back``；``label`` 为写入 allocationTarget 列的目标标识。
    """
    total_weight = sum((weight for _target, weight in targets_list), Decimal("0"))
    if denominator is None or denominator <= 0:
        denominator = total_weight
    contributions = [
        (target, amount * weight / denominator)
        for target, weight in targets_list
    ]
    add_back = sum((c for _target, c in contributions), Decimal("0"))
    label = "|".join(target[1] for target, _weight in targets_list)
    return add_back, contributions, label


def decimal_from_row(row: dict[str, str], field: str) -> Decimal:
    """将指定金额字段转换为 Decimal，避免浮点数精度误差。"""
    raw = (row.get(field) or "").strip()
    if not raw:
        return Decimal("0")
    try:
        return Decimal(raw)
    except InvalidOperation as exc:
        raise ValueError(f"{field} 不是有效金额：{raw!r}") from exc


class PriceRate(NamedTuple):
    """A customer Consumption rate from an Azure Price Sheet."""

    meter_id: str
    unit_price: Decimal
    currency: str
    effective_start: date | None
    effective_end: date | None


def _canonical_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _parse_date(value: str) -> date | None:
    text = (value or "").strip()
    if not text:
        return None
    if "T" in text:
        text = text.split("T", 1)[0]
    for pattern in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, pattern).date()
        except ValueError:
            pass
    raise ValueError(f"无法解析日期：{value!r}")


def _csv_payloads(payload: bytes, source: str) -> list[tuple[str, bytes]]:
    if payload.startswith(b"PK\x03\x04"):
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            files = [
                (name, archive.read(name))
                for name in archive.namelist()
                if name.lower().endswith(".csv") and not name.endswith("/")
            ]
        if not files:
            raise ValueError(f"Azure Price Sheet ZIP 不包含 CSV：{source}")
        return files
    return [(source, payload)]


def parse_price_sheet(payload: bytes, source: str) -> dict[str, list[PriceRate]]:
    """Parse Consumption tier-zero rates from CSV, ZIP, or JSON Price Sheets."""
    rates: defaultdict[str, set[PriceRate]] = defaultdict(set)
    stripped = payload.lstrip(b"\xef\xbb\xbf \t\r\n")
    if stripped.startswith(b"["):
        parsed = json.loads(payload.decode("utf-8-sig"))
        if not isinstance(parsed, list) or not all(
            isinstance(row, dict) for row in parsed
        ):
            raise ValueError("Azure Price Sheet JSON 顶层必须是对象数组")
        sources: list[tuple[str, list[dict[str, Any]], list[str]]] = [
            (
                source,
                parsed,
                list(parsed[0]) if parsed else [],
            )
        ]
    else:
        sources = []
        for name, content in _csv_payloads(payload, source):
            reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))
            if reader.fieldnames is None:
                raise ValueError(f"Azure Price Sheet CSV 没有表头：{name}")
            sources.append((name, list(reader), list(reader.fieldnames)))

    for name, rows, fieldnames in sources:
        headers = {_canonical_header(field): field for field in fieldnames}

        def field(row: dict[str, Any], *aliases: str) -> str:
            for alias in aliases:
                original = headers.get(_canonical_header(alias))
                if original is not None:
                    value = row.get(original)
                    return "" if value is None else str(value).strip()
            return ""

        for row in rows:
            price_type = field(row, "priceType")
            if price_type and price_type.lower() != "consumption":
                continue
            tier = field(row, "tierMinimumUnits")
            if tier:
                try:
                    if Decimal(tier) != 0:
                        continue
                except InvalidOperation as exc:
                    raise ValueError(
                        f"Azure Price Sheet tierMinimumUnits 非法：{tier!r}"
                    ) from exc
            meter_id = field(row, "meterId").lower()
            raw_price = field(row, "unitPrice")
            if not meter_id or not raw_price:
                continue
            try:
                unit_price = Decimal(raw_price)
            except InvalidOperation as exc:
                raise ValueError(
                    f"Azure Price Sheet unitPrice 非法：{raw_price!r}"
                ) from exc
            rates[meter_id].add(
                PriceRate(
                    meter_id=meter_id,
                    unit_price=unit_price,
                    currency=field(
                        row, "billingCurrency", "currency", "currencyCode"
                    ).upper(),
                    effective_start=_parse_date(field(row, "effectiveStartDate")),
                    effective_end=_parse_date(field(row, "effectiveEndDate")),
                )
            )
    if not rates:
        raise ValueError("Azure Price Sheet 中没有可用的 Consumption tier-zero 价格")
    return {meter_id: sorted(values, key=repr) for meter_id, values in rates.items()}


def price_for_row(row: dict[str, str], rates: dict[str, list[PriceRate]]) -> Decimal:
    """Return the unique active customer PAYG unit price for a usage row."""
    meter_id = (row.get("meterId") or "").strip().lower()
    if not meter_id:
        raise ValueError("RI Usage 明细缺少 meterId，无法匹配 Azure Price Sheet")
    usage_date = _parse_date(row.get("date") or "")
    if usage_date is None:
        raise ValueError("RI Usage 明细缺少 date，无法匹配 Azure Price Sheet")
    currency = (row.get("billingCurrency") or "").strip().upper()
    candidates = []
    for rate in rates.get(meter_id, []):
        if rate.currency and currency and rate.currency != currency:
            continue
        if rate.effective_start and usage_date < rate.effective_start:
            continue
        if rate.effective_end and usage_date > rate.effective_end:
            continue
        candidates.append(rate)
    prices = {rate.unit_price for rate in candidates}
    if not prices:
        raise ValueError(
            f"Azure Price Sheet 找不到 meterId={meter_id!r}、date={usage_date}、"
            f"currency={currency!r} 的 Consumption 价格"
        )
    if len(prices) != 1:
        raise ValueError(
            f"Azure Price Sheet 对 meterId={meter_id!r}、date={usage_date} "
            f"匹配到多个 unitPrice：{sorted(prices)}"
        )
    return next(iter(prices))


def _model_value(value: Any, *names: str) -> Any:
    for name in names:
        if isinstance(value, dict) and name in value:
            return value[name]
        result = getattr(value, name, None)
        if result is not None:
            return result
    return None


def wait_for_price_sheet(poller: Any, timeout: int) -> Any:
    """Wait for a Price Sheet LRO and fail clearly when the deadline expires."""
    poller.wait(timeout=timeout)
    if not poller.done():
        raise TimeoutError(
            f"Azure Price Sheet 在 {timeout} 秒内未生成完成；"
            "服务端任务可能仍在运行，请稍后重试或使用已取得的 downloadUrl"
        )
    return poller.result()


def resolve_billing_account_name(
    billing_account_id: str, credential: Any
) -> str:
    """Resolve a Cost Export MCA account ID to its full Billing account name."""
    if ":" in billing_account_id:
        return billing_account_id
    token = credential.get_token("https://management.azure.com/.default").token
    request = urllib.request.Request(
        "https://management.azure.com/providers/Microsoft.Billing/"
        "billingAccounts?api-version=2020-05-01",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        payload = json.load(response)
    names = [
        str(item.get("name") or "")
        for item in payload.get("value", [])
        if isinstance(item, dict)
    ]
    matches = [
        name
        for name in names
        if name == billing_account_id or name.startswith(f"{billing_account_id}:")
    ]
    if len(matches) != 1:
        raise ValueError(
            f"无法将 billingAccountId={billing_account_id!r} 唯一映射为完整 "
            f"Billing Account Name（匹配数：{len(matches)}）；"
            "请通过 --billing-account-name 显式指定"
        )
    return matches[0]


def _authorized_json_request(
    url: str, credential: Any, method: str = "GET"
) -> tuple[Any, Any]:
    token = credential.get_token("https://management.azure.com/.default").token
    request = urllib.request.Request(
        url,
        data=b"" if method == "POST" else None,
        headers={"Authorization": f"Bearer {token}"},
        method=method,
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        body = response.read()
        return response, json.loads(body) if body else {}


def download_price_sheet_by_invoice(
    billing_account_name: str,
    billing_profile_id: str,
    invoice_id: str,
    credential: Any,
    timeout: int,
) -> Any:
    """Run the invoice Price Sheet LRO using its Completed terminal status."""
    quote = lambda value: urllib.parse.quote(value, safe=":-_")
    url = (
        "https://management.azure.com/providers/Microsoft.Billing/"
        f"billingAccounts/{quote(billing_account_name)}/"
        f"billingProfiles/{quote(billing_profile_id)}/"
        f"invoices/{quote(invoice_id)}/providers/Microsoft.CostManagement/"
        "pricesheets/default/download?api-version=2025-03-01"
    )
    response, payload = _authorized_json_request(url, credential, method="POST")
    if getattr(response, "status", None) == 200:
        return payload
    if getattr(response, "status", None) != 202:
        raise ValueError(
            f"Azure Price Sheet API 返回非预期状态：{response.status}"
        )

    async_url = response.headers.get("Azure-AsyncOperation")
    location_url = response.headers.get("Location")
    if not async_url or not location_url:
        raise ValueError(
            "Azure Price Sheet API 的 202 响应缺少 "
            "Azure-AsyncOperation 或 Location"
        )
    retry_after = int(response.headers.get("Retry-After") or "10")
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(
                f"Azure Price Sheet 在 {timeout} 秒内未生成完成；"
                "服务端任务可能仍在运行，请稍后重试"
            )
        time.sleep(min(retry_after, remaining))
        status_response, status_payload = _authorized_json_request(
            async_url, credential
        )
        status = str(status_payload.get("status") or "").strip().lower()
        if status in {"completed", "succeeded"}:
            _final_response, final_payload = _authorized_json_request(
                location_url, credential
            )
            return final_payload
        if status in {"failed", "canceled", "cancelled"}:
            raise ValueError(
                f"Azure Price Sheet 生成失败，状态：{status_payload.get('status')}"
            )
        if getattr(status_response, "status", None) not in {200, 202}:
            raise ValueError(
                "Azure Price Sheet 状态查询返回非预期状态："
                f"{status_response.status}"
            )
        retry_after = int(status_response.headers.get("Retry-After") or "10")


def download_price_sheet_by_billing_profile(
    billing_account_name: str,
    billing_profile_id: str,
    credential: Any,
    timeout: int,
) -> Any:
    """Run the Billing Profile Price Sheet LRO through its Completed status."""
    quote = lambda value: urllib.parse.quote(value, safe=":-_")
    url = (
        "https://management.azure.com/providers/Microsoft.Billing/"
        f"billingAccounts/{quote(billing_account_name)}/"
        f"billingProfiles/{quote(billing_profile_id)}/"
        "providers/Microsoft.CostManagement/pricesheets/default/download"
        "?api-version=2025-03-01"
    )
    response, payload = _authorized_json_request(url, credential, method="POST")
    if getattr(response, "status", None) == 200:
        return payload
    if getattr(response, "status", None) != 202:
        raise ValueError(
            f"Azure Price Sheet API 返回非预期状态：{response.status}"
        )

    async_url = (
        response.headers.get("Azure-Consumption-AsyncOperation")
        or response.headers.get("Azure-AsyncOperation")
    )
    if not async_url:
        raise ValueError("Azure Price Sheet API 的 202 响应缺少异步状态 URL")
    retry_after = int(response.headers.get("Retry-After") or "10")
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(
                f"Azure Price Sheet 在 {timeout} 秒内未生成完成；"
                "服务端任务可能仍在运行，请稍后重试"
            )
        time.sleep(min(retry_after, remaining))
        status_response, status_payload = _authorized_json_request(
            async_url, credential
        )
        status = str(status_payload.get("status") or "").strip().lower()
        if status in {"completed", "succeeded"}:
            return status_payload
        if status in {"failed", "canceled", "cancelled"}:
            raise ValueError(
                f"Azure Price Sheet 生成失败，状态：{status_payload.get('status')}"
            )
        if getattr(status_response, "status", None) not in {200, 202}:
            raise ValueError(
                "Azure Price Sheet 状态查询返回非预期状态："
                f"{status_response.status}"
            )
        retry_after = int(status_response.headers.get("Retry-After") or "10")


def price_sheet_download_url(result: Any) -> str:
    """Extract a temporary download URL from direct and wrapped API results."""
    url = _model_value(result, "download_url", "downloadUrl")
    if url:
        return str(url)
    properties = _model_value(result, "properties") or {}
    url = _model_value(properties, "report_url", "reportUrl", "downloadUrl")
    if url:
        return str(url)
    published = _model_value(result, "published_entity", "publishedEntity") or {}
    published_properties = _model_value(published, "properties") or {}
    return str(
        _model_value(
            published_properties, "download_url", "downloadUrl", "reportUrl"
        )
        or ""
    )


def download_price_sheet(
    rows: list[dict[str, str]],
    invoice_id: str | None = None,
    billing_account_name: str | None = None,
    timeout: int = 1800,
    client: Any | None = None,
    credential: Any | None = None,
) -> bytes:
    """Download the applicable Azure Price Sheet through Cost Management SDK."""
    def unique(field: str, required: bool = True) -> str:
        values = {(row.get(field) or "").strip() for row in rows}
        values.discard("")
        if len(values) > 1:
            raise ValueError(f"输入账单包含多个 {field}，请分批处理")
        if required and not values:
            raise ValueError(f"输入账单缺少 {field}，无法下载 Azure Price Sheet")
        return next(iter(values), "")

    billing_account_id = unique("billingAccountId")
    billing_profile_id = unique("billingProfileId", required=False)
    selected_invoice_id = (invoice_id or "").strip()
    if not selected_invoice_id:
        row_invoice_ids = [(row.get("invoiceId") or "").strip() for row in rows]
        if row_invoice_ids and all(row_invoice_ids):
            invoice_ids = set(row_invoice_ids)
            if len(invoice_ids) > 1:
                raise ValueError("输入账单包含多个 invoiceId，请分批处理")
            selected_invoice_id = next(iter(invoice_ids))
    usage_dates = {
        parsed
        for row in rows
        if (parsed := _parse_date(row.get("date") or "")) is not None
    }
    if not usage_dates:
        raise ValueError("输入账单缺少 date，无法确定 Azure Price Sheet 账期")
    periods = {(item.year, item.month) for item in usage_dates}
    if len(periods) != 1:
        raise ValueError("输入账单跨多个自然月，请按月分批处理")

    if timeout <= 0:
        raise ValueError("price_sheet_timeout 必须大于 0")
    owns_client = client is None
    if client is None:
        try:
            from azure.identity import AzureCliCredential
            from azure.mgmt.costmanagement import CostManagementClient
        except ImportError as exc:
            raise RuntimeError(
                "自动下载 Azure Price Sheet 需要安装 requirements.txt 中的 Azure SDK"
            ) from exc
        credential = credential or AzureCliCredential()
        client = CostManagementClient(credential=credential)

    def resolve_account_name() -> str:
        account_name = (billing_account_name or "").strip()
        if account_name:
            return account_name
        if credential is None:
            raise ValueError(
                "自动解析完整 Billing Account Name 需要 Azure credential；"
                "传入自定义 client 时请同时传入 credential，或通过 "
                "--billing-account-name 显式指定"
            )
        return resolve_billing_account_name(billing_account_id, credential)

    try:
        if billing_profile_id:
            account_name = resolve_account_name()
            if selected_invoice_id:
                print(
                    f"正在生成 invoiceId={selected_invoice_id} 的 Azure Price Sheet，"
                    f"最长等待 {timeout} 秒..."
                )
                if credential is not None:
                    result = download_price_sheet_by_invoice(
                        account_name,
                        billing_profile_id,
                        selected_invoice_id,
                        credential,
                        timeout,
                    )
                else:
                    poller = client.price_sheet.begin_download_by_invoice(
                        billing_account_name=account_name,
                        billing_profile_name=billing_profile_id,
                        invoice_name=selected_invoice_id,
                    )
                    result = wait_for_price_sheet(poller, timeout)
            else:
                print(
                    "账单 invoiceId 不完整，正在通过 Billing Profile 生成 Azure "
                    f"Price Sheet，最长等待 {timeout} 秒..."
                )
                if credential is not None:
                    result = download_price_sheet_by_billing_profile(
                        account_name,
                        billing_profile_id,
                        credential,
                        timeout,
                    )
                else:
                    poller = client.price_sheet.begin_download_by_billing_profile(
                        billing_account_name=account_name,
                        billing_profile_name=billing_profile_id,
                    )
                    result = wait_for_price_sheet(poller, timeout)
        else:
            year, month = next(iter(periods))
            poller = client.price_sheet.begin_download_by_billing_account(
                billing_account_id=billing_account_id,
                billing_period_name=f"{year:04d}{month:02d}",
            )
            result = wait_for_price_sheet(poller, timeout)

        url = price_sheet_download_url(result)
        if not url:
            raise ValueError("Azure Price Sheet API 响应中缺少下载 URL")
        with urllib.request.urlopen(str(url), timeout=120) as response:
            return response.read()
    finally:
        if owns_client:
            client.close()


def load_price_sheet(
    rows: list[dict[str, str]],
    price_sheet_file: str | None,
    invoice_id: str | None = None,
    billing_account_name: str | None = None,
    timeout: int = 1800,
    save_price_sheet_file: str | None = None,
) -> tuple[dict[str, list[PriceRate]], str]:
    if price_sheet_file:
        path = Path(price_sheet_file)
        if not path.is_file():
            raise FileNotFoundError(f"找不到 Azure Price Sheet：{path}")
        return parse_price_sheet(path.read_bytes(), str(path)), str(path)
    payload = download_price_sheet(
        rows,
        invoice_id=invoice_id,
        billing_account_name=billing_account_name,
        timeout=timeout,
    )
    if save_price_sheet_file:
        target = Path(save_price_sheet_file)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
    return parse_price_sheet(payload, "Azure Cost Management API"), "Azure Cost Management API"


def is_ri_usage(row: dict[str, str], reservation_ids: set[str]) -> bool:
    """判断明细是否属于指定 reservationId 的实际 RI 使用记录。"""
    return (
        row.get("pricingModel") == "Reservation"
        and row.get("chargeType") == "Usage"
        and row.get("reservationId", "").strip() in reservation_ids
    )


def has_target_tag(row: dict[str, str], target_tag: tuple[str, str]) -> bool:
    """判断明细是否包含指定的接收项目标签。"""
    key, expected_value = target_tag
    return str(parse_tags(row.get("tags", "")).get(key) or "") == expected_value


def additional_info(row: dict[str, str]) -> dict[str, Any]:
    """解析账单的 additionalInfo JSON 字段；无效时返回空字典。"""
    try:
        info = json.loads(row.get("additionalInfo") or "{}")
    except json.JSONDecodeError:
        return {}
    return info if isinstance(info, dict) else {}


def vm_model(row: dict[str, str]) -> str:
    """获取 Azure VM 机型，优先使用 additionalInfo.ServiceType。"""
    return str(additional_info(row).get("ServiceType") or row.get("meterName") or "")


def ri_normalization_ratio(row: dict[str, str]) -> Decimal | None:
    """获取 additionalInfo.RINormalizationRatio；缺失或非法时返回 None。

    这是 Azure 账单中真实存在的实例大小灵活性字段：当 RI 以大小灵活性方式
    应用到某个机型时，该比率表示 RI 在该机型上的归一化占用系数。
    """
    raw = additional_info(row).get("RINormalizationRatio")
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return Decimal(str(raw))
    except InvalidOperation:
        return None


# 从机型名派生实例大小灵活性组：family + 附加特性 + 版本（去掉 vCPU 核数与受限核数）。
# 例：Standard_D2s_v5 / Standard_D4s_v5 / Standard_D8-2s_v5 → 组 "Ds_v5"；
#     Standard_E8s_v5 → "Es_v5"；Standard_D2_v5 → "D_v5"（非高级存储，另一组）。
# 说明：这是基于命名规则的启发式分组，覆盖 D/E/F 等主流系列的常见场景，
# 并非 Azure 官方灵活性比率表的逐条复刻；边缘系列如有出入可按官方表扩展。
_SIZE_PATTERN = re.compile(r"^([A-Za-z]+)(\d+)(?:-\d+)?([A-Za-z]*)(_.*)?$")


def flexibility_group(model: str) -> str:
    """根据机型名派生实例大小灵活性组标识；无法解析时返回原始机型名。"""
    name = model.strip()
    if name.startswith("Standard_"):
        name = name[len("Standard_") :]
    match = _SIZE_PATTERN.match(name)
    if not match:
        return model.strip()
    family, _cores, features, version = match.groups()
    return f"{family}{features}{version or ''}"


def is_size_flexible(row: dict[str, str]) -> bool:
    """启发式判断该明细是否体现了实例大小灵活性（应用到非基准规格）。

    依据真实字段 RINormalizationRatio：比率存在且不等于 1 时，说明该 RI 被
    归一化到了与基准不同的规格。注意：比率为 1 也可能是基准规格本身，
    因此该判断仅为按行提示，权威的 On/Off 需 Reservation API（账单不含）。
    """
    ratio = ri_normalization_ratio(row)
    return ratio is not None and ratio != Decimal("1")


def allocated_field_name(amount_field: str) -> str:
    """根据金额字段派生分摊后金额的输出列名。

    使输出列货币与 --amount-field 一致，避免用非账单货币时列名产生误导。
    默认 costInBillingCurrency 仍产出 allocatedCostInBillingCurrency（向后兼容）。
    """
    if amount_field == "costInBillingCurrencyAfterActualReconciliation":
        return "allocatedCostInBillingCurrency"
    return "allocated" + amount_field[:1].upper() + amount_field[1:]


def vm_region(row: dict[str, str]) -> str:
    """获取 Azure VM 区域。"""
    return (
        row.get("meterRegion")
        or row.get("resourceLocation")
        or row.get("location")
        or ""
    )


def allocation_key(row: dict[str, str], match_mode: str = "model") -> tuple[str, str]:
    """返回 RI 收益匹配使用的键。

    - model：按精确机型和区域匹配（默认，兼容历史行为）。
    - flex-group：按机型派生的实例大小灵活性组和区域匹配，使同一 RI 覆盖的
      同系列不同规格（如 D2s_v5 与 D4s_v5）落入同一收益池，避免"RI 单规格、
      目标项目另一规格"时分摊失败。无法解析机型时回退到精确机型。
    """
    region = vm_region(row)
    if match_mode == "flex-group":
        model = vm_model(row)
        group = flexibility_group(model)
        if group:
            return f"flexgroup:{group}", region
    return vm_model(row), region


def project_of(row: dict[str, str], project_tag_key: str) -> str:
    """获取明细的项目名（来自 project_tag_key 指定的标签）；缺失时使用统一占位名称。"""
    return str(parse_tags(row.get("tags", "")).get(project_tag_key) or "<missing>")


def main() -> None:
    """读取账单、计算分摊并生成明细副本和汇总报告。"""
    args = parse_args()
    ri_targets, ri_modes, ri_denominators, ri_scopes = load_reservations_config(
        Path(args.reservations_file), args.project_tag_key
    )
    management_group_subscriptions = resolve_management_group_scopes(ri_scopes)
    reservation_ids = set(ri_targets)
    targets = sorted(
        {target for entries in ri_targets.values() for target, _weight in entries}
    )
    # 每个分摊目标的匹配模式来自绑定它的预留的 flexibility。若同一目标被匹配模式
    # 不同的预留同时绑定，收益池无法一致隔离，直接报错要求先统一口径。
    target_modes: dict[tuple[str, str], str] = {}
    target_profiles: defaultdict[
        tuple[str, str], set[tuple[tuple[str, str], str]]
    ] = defaultdict(set)
    for reservation_id, entries in ri_targets.items():
        mode = ri_modes[reservation_id]
        scope = ri_scopes[reservation_id]
        for target, _weight in entries:
            existing = target_modes.get(target)
            if existing is not None and existing != mode:
                raise ValueError(
                    f"分摊目标 {'='.join(target)!r} 被匹配模式不同的预留同时绑定"
                    f"（{existing} 与 {mode}）；请统一相关预留的 flexibility。"
                )
            target_modes[target] = mode
            target_profiles[target].add((scope, mode))
    input_paths = expand_inputs(args.inputs)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # 先将明细加载到内存，第二遍处理时可以按目标项目的总费用计算比例。
    rows: list[dict[str, str]] = []
    fieldnames: list[str] | None = None
    source_files: list[str] = []
    total_rows = 0
    ri_usage_rows = 0
    ri_reallocated_rows = 0
    ri_size_flexible_rows = 0
    ri_amount = Decimal("0")
    ri_raw_total = Decimal("0")
    ri_payg_total = Decimal("0")
    ri_gross_savings_total = Decimal("0")
    ri_unused_cost_total = Decimal("0")
    project_before: defaultdict[str, Decimal] = defaultdict(Decimal)
    project_ri: defaultdict[str, Decimal] = defaultdict(Decimal)
    # 收益池按 (RI scope, 分摊目标, 匹配键) 隔离，避免跨订阅/资源组使用 RI。
    pool_key_type = tuple[tuple[str, str], tuple[str, str], tuple[str, str]]
    target_non_ri_indexes: defaultdict[pool_key_type, list[int]] = defaultdict(list)
    target_non_ri_total_by_key: defaultdict[pool_key_type, Decimal] = defaultdict(
        Decimal
    )
    ri_amount_by_key: defaultdict[pool_key_type, Decimal] = defaultdict(Decimal)
    ri_amount_by_reservation_key: defaultdict[
        tuple[pool_key_type, str], Decimal
    ] = defaultdict(Decimal)
    ri_service_types: defaultdict[str, set[str]] = defaultdict(set)
    # RI 使用记录加回后的信息：加回金额、目标标签、以及作为接收方时的全价基数。
    ri_usage_indexes: set[int] = set()
    ri_add_back_by_index: dict[int, Decimal] = {}
    ri_label_by_index: dict[int, str] = {}
    receiver_basis_by_index: dict[int, Decimal] = {}

    # First load all source rows. Real RI savings require reservation-level totals
    # (including UnusedReservation), so allocation starts only after the full scan.
    for input_path in input_paths:
        source_files.append(str(input_path))
        with input_path.open("r", encoding="utf-8-sig", newline="") as source:
            reader = csv.DictReader(source)
            if reader.fieldnames is None:
                raise ValueError(f"{input_path} 没有 CSV 表头")
            if fieldnames is None:
                fieldnames = list(reader.fieldnames)
            elif fieldnames != list(reader.fieldnames):
                raise ValueError(f"{input_path} 的 CSV 表头与其他输入文件不一致")
            if args.amount_field not in reader.fieldnames:
                raise ValueError(
                    f"{input_path} 缺少成本基准字段 {args.amount_field!r}"
                )

            for source_row_number, row in enumerate(reader, start=2):
                total_rows += 1
                row["_source_file"] = str(input_path)
                row["_source_row_number"] = str(source_row_number)
                rows.append(row)

    price_rates, price_sheet_source = load_price_sheet(
        rows,
        args.price_sheet_file,
        invoice_id=args.invoice_id,
        billing_account_name=args.billing_account_name,
        timeout=args.price_sheet_timeout,
        save_price_sheet_file=args.save_price_sheet_file,
    )
    gross_by_index: dict[int, Decimal] = {}
    payg_by_index: dict[int, Decimal] = {}
    gross_by_reservation: defaultdict[str, Decimal] = defaultdict(Decimal)
    benefit_by_reservation: defaultdict[str, Decimal] = defaultdict(Decimal)
    excess_cost_by_reservation: defaultdict[str, Decimal] = defaultdict(Decimal)
    unused_by_reservation: defaultdict[str, Decimal] = defaultdict(Decimal)

    for row_index, row in enumerate(rows):
        reservation_id = row.get("reservationId", "").strip()
        if is_ri_usage(row, reservation_ids):
            quantity = decimal_from_row(row, "quantity")
            if quantity < 0:
                raise ValueError("RI Usage quantity 不能为负数")
            payg_equivalent = price_for_row(row, price_rates) * quantity
            amortized_cost = decimal_from_row(row, args.amount_field)
            gross_savings = payg_equivalent - amortized_cost
            payg_by_index[row_index] = payg_equivalent
            gross_by_index[row_index] = gross_savings
            gross_by_reservation[reservation_id] += gross_savings
            if gross_savings > 0:
                benefit_by_reservation[reservation_id] += gross_savings
            elif gross_savings < 0:
                excess_cost_by_reservation[reservation_id] -= gross_savings
        elif (
            reservation_id in reservation_ids
            and row.get("chargeType") == "UnusedReservation"
            and row.get("pricingModel") == "Reservation"
        ):
            unused_by_reservation[reservation_id] += decimal_from_row(
                row, args.amount_field
            )

    ri_payg_total = sum(payg_by_index.values(), Decimal("0"))
    ri_gross_savings_total = sum(gross_by_index.values(), Decimal("0"))
    ri_unused_cost_total = sum(unused_by_reservation.values(), Decimal("0"))

    for row_index, row in enumerate(rows):
        if row.get("meterCategory") != "Virtual Machines":
            continue

        project = project_of(row, args.project_tag_key)
        amount = decimal_from_row(row, args.amount_field)
        project_before[project] += amount

        if is_ri_usage(row, reservation_ids):
            reservation_id = row.get("reservationId", "").strip()
            targets_list = ri_targets[reservation_id]
            ri_usage_rows += 1
            ri_service_types[reservation_id].add(vm_model(row))
            if is_size_flexible(row):
                ri_size_flexible_rows += 1
            gross_savings = gross_by_index[row_index]
            add_back, contributions, label = _row_contributions(
                gross_savings, targets_list, ri_denominators[reservation_id]
            )
            ri_reallocated_rows += 1
            ri_amount += add_back
            ri_raw_total += amount
            project_ri[project] += add_back
            ri_usage_indexes.add(row_index)
            ri_add_back_by_index[row_index] = add_back
            ri_label_by_index[row_index] = label
            alloc_key = allocation_key(row, ri_modes[reservation_id])
            for target, contribution in contributions:
                pool_key = (ri_scopes[reservation_id], target, alloc_key)
                ri_amount_by_key[pool_key] += contribution
                if contribution != 0:
                    ri_amount_by_reservation_key[
                        (pool_key, reservation_id)
                    ] += contribution
            matched = [t for t in targets if has_target_tag(row, t)]
            if len(matched) > 1:
                labels = "、".join("=".join(t) for t in matched)
                raise ValueError(
                    f"虚拟机明细同时匹配多个分摊目标（{labels}）；"
                    "一条明细只能归属一个分摊目标。"
                )
            if matched:
                full_price = amount + add_back
                receiver_basis_by_index[row_index] = full_price
                for scope, mode in target_profiles[matched[0]]:
                    if not row_matches_ri_scope(
                        row, scope, management_group_subscriptions
                    ):
                        continue
                    pool_key = (
                        scope,
                        matched[0],
                        allocation_key(row, mode),
                    )
                    target_non_ri_indexes[pool_key].append(row_index)
                    target_non_ri_total_by_key[pool_key] += full_price
        else:
            matched = [t for t in targets if has_target_tag(row, t)]
            if len(matched) > 1:
                labels = "、".join("=".join(t) for t in matched)
                raise ValueError(
                    f"虚拟机明细同时匹配多个分摊目标（{labels}）；"
                    "一条明细只能归属一个分摊目标。"
                )
            if matched:
                receiver_basis_by_index[row_index] = amount
                for scope, mode in target_profiles[matched[0]]:
                    if not row_matches_ri_scope(
                        row, scope, management_group_subscriptions
                    ):
                        continue
                    pool_key = (
                        scope,
                        matched[0],
                        allocation_key(row, mode),
                    )
                    target_non_ri_indexes[pool_key].append(row_index)
                    target_non_ri_total_by_key[pool_key] += amount

    target_non_ri_total = sum(
        target_non_ri_total_by_key.values(), Decimal("0")
    )

    for pool_key, key_ri_amount in ri_amount_by_key.items():
        scope, target, alloc_key = pool_key
        target_label = "=".join(target)
        # RI 金额为 0 时无需分摊，跳过校验，避免对零成本 RI 记录误报。
        if key_ri_amount == 0:
            continue
        key_target_total = target_non_ri_total_by_key.get(pool_key, Decimal("0"))
        if key_target_total == 0:
            raise ValueError(
                f"找不到分摊目标 {target_label!r} 与 RI 机型和区域 {alloc_key!r} "
                f"且符合 RI scope {scope!r} 的虚拟机明细"
                "（含加回后的 RI 使用记录）。"
            )
        if key_ri_amount > 0 and key_target_total < key_ri_amount:
            raise ValueError(
                f"分摊目标 {target_label!r} 的匹配机型和区域 {alloc_key!r} "
                f"虚拟机全价费用 {key_target_total} 小于待分摊 RI 净收益 "
                f"{key_ri_amount}，无法按比例分摊后保持非负费用。"
            )

    # RI 使用记录先加回自身 RI 金额（体现未人为分配前的原始资源成本）；若其标签命中
    # 某个分摊目标，则加回后再以全价参与该目标收益分摊，净额为加回金额减去应摊份额。
    allocation_by_index: dict[int, Decimal] = {}
    target_value_by_index: dict[int, str] = {}
    allocation_details_by_index: defaultdict[
        int, list[tuple[str, str, str, Decimal]]
    ] = defaultdict(list)
    for index in ri_usage_indexes:
        add_back = ri_add_back_by_index[index]
        allocation_by_index[index] = add_back
        target_value_by_index[index] = ri_label_by_index[index]
        reservation_id = rows[index].get("reservationId", "").strip()
        if reservation_id and add_back != 0:
            allocation_details_by_index[index].append(
                (
                    "RI_USAGE_COST_REASSIGNED",
                    ri_label_by_index[index],
                    reservation_id,
                    add_back,
                )
            )

    # 每条目标明细只承接相同分摊目标、相同机型和区域 RI 收益池中的金额；加回后的
    # RI 使用记录以全价基数参与，普通非 RI 明细以原始费用基数参与。
    for pool_key, indexes in target_non_ri_indexes.items():
        _scope, target, _alloc_key = pool_key
        key_ri_amount = ri_amount_by_key.get(pool_key, Decimal("0"))
        # 没有 RI 收益或超额成本可分摊时保持金额不变。
        if key_ri_amount == 0:
            continue
        key_target_total = target_non_ri_total_by_key[pool_key]
        for index in indexes:
            basis = receiver_basis_by_index[index]
            for (
                contribution_pool_key,
                reservation_id,
            ), reservation_amount in ri_amount_by_reservation_key.items():
                if contribution_pool_key != pool_key:
                    continue
                share = reservation_amount * basis / key_target_total
                if share == 0:
                    continue
                allocation_by_index[index] = (
                    allocation_by_index.get(index, Decimal("0")) - share
                )
                allocation_details_by_index[index].append(
                    (
                        "RI_BENEFIT_ASSIGNED",
                        target[1],
                        reservation_id,
                        -share,
                    )
                )
            # RI 使用记录本身也是接收方时保留其加回目标标签，其余用池目标标签。
            if index not in ri_usage_indexes:
                target_value_by_index[index] = target[1]

    output_paths: list[str] = []
    allocated_field = allocated_field_name(args.amount_field)
    if not args.summary_only:
        if fieldnames is None:
            raise ValueError("没有读取到 CSV 表头")
        fieldnames = [
            *fieldnames,
            allocated_field,
            "riAllocationAmount",
            "riPaygEquivalentAmount",
            "riAmortizedCost",
            "riBenefitOrLoss",
            "allocationType",
            "allocationTarget",
        ]

        row_offset = 0
        for input_path in input_paths:
            output_path = output_dir / input_path.name
            output_paths.append(str(output_path))
            source_row_count = sum(
                1 for row in rows if row["_source_file"] == str(input_path)
            )
            with output_path.open("w", encoding="utf-8", newline="") as target:
                writer = csv.DictWriter(target, fieldnames=fieldnames)
                writer.writeheader()
                for index in range(row_offset, row_offset + source_row_count):
                    row = dict(rows[index])
                    row.pop("_source_file", None)
                    row.pop("_source_row_number", None)
                    original = decimal_from_row(row, args.amount_field)
                    adjustment = allocation_by_index.get(index, Decimal("0"))
                    row[allocated_field] = str(
                        original + adjustment
                    )
                    row["riAllocationAmount"] = str(adjustment)
                    if index in ri_usage_indexes:
                        row["riPaygEquivalentAmount"] = str(payg_by_index[index])
                        row["riAmortizedCost"] = str(original)
                        row["riBenefitOrLoss"] = str(gross_by_index[index])
                    else:
                        row["riPaygEquivalentAmount"] = ""
                        row["riAmortizedCost"] = ""
                        row["riBenefitOrLoss"] = ""
                    # RI 使用记录：其 RI 使用收益被重新分配（可能同时又接收了本项目应得
                    # 收益），净额可正可负；统一标记为 RI_USAGE_COST_REASSIGNED。
                    if index in ri_usage_indexes and adjustment != 0:
                        row["allocationType"] = "RI_USAGE_COST_REASSIGNED"
                        row["allocationTarget"] = target_value_by_index.get(index, "")
                    # 非 RI 接收记录：负调整为收益，正调整为超额成本。
                    elif adjustment != 0:
                        row["allocationType"] = "RI_BENEFIT_ASSIGNED"
                        row["allocationTarget"] = target_value_by_index.get(index, "")
                    else:
                        row["allocationType"] = ""
                        row["allocationTarget"] = ""
                    writer.writerow(row)
            row_offset += source_row_count

        allocation_details_path = output_dir / "ri-allocation-details.csv"
        with allocation_details_path.open(
            "w", encoding="utf-8", newline=""
        ) as target:
            writer = csv.writer(target)
            writer.writerow(
                [
                    "sourceFile",
                    "sourceRowNumber",
                    "ResourceId",
                    "allocationType",
                    "allocationTarget",
                    "riAllocationReservationIds",
                    "allocationAmount",
                ]
            )
            for index in sorted(allocation_details_by_index):
                source_row = rows[index]
                for (
                    allocation_type,
                    allocation_target,
                    reservation_id,
                    amount,
                ) in sorted(
                    allocation_details_by_index[index],
                    key=lambda detail: (detail[0], detail[2]),
                ):
                    writer.writerow(
                        [
                            source_row["_source_file"],
                            source_row["_source_row_number"],
                            source_row.get("ResourceId")
                            or source_row.get("resourceId")
                            or "",
                            allocation_type,
                            allocation_target,
                            reservation_id,
                            str(amount),
                        ]
                    )
        output_paths.append(str(allocation_details_path))

    # 每个分摊目标承接的有符号 RI 经济差额（按目标标签值汇总）。
    assigned_by_target: defaultdict[str, Decimal] = defaultdict(Decimal)
    for (_scope, target, _alloc_key), key_ri_amount in ri_amount_by_key.items():
        assigned_by_target[target[1]] += key_ri_amount

    ri_gross_benefit_total = sum(
        (amount for amount in gross_by_index.values() if amount > 0),
        Decimal("0"),
    )
    ri_excess_cost_total = -sum(
        (amount for amount in gross_by_index.values() if amount < 0),
        Decimal("0"),
    )

    project_after = dict(project_before)
    for project, added_amount in project_ri.items():
        project_after[project] = project_after.get(project, Decimal("0")) + added_amount
    for target_value, assigned_amount in assigned_by_target.items():
        project_after[target_value] = (
            project_after.get(target_value, Decimal("0")) - assigned_amount
        )

    allocation_path = output_dir / "project-allocation.csv"
    with allocation_path.open("w", encoding="utf-8", newline="") as target:
        writer = csv.writer(target)
        writer.writerow(
            [
                "projname",
                "beforeAmount",
                "afterAllocatedAmount",
                "delta",
                "riAmountAdded",
                "riAmountAssigned",
            ]
        )
        projects = sorted(
            set(project_before) | set(project_after),
            key=lambda project: (-project_before.get(project, Decimal("0")), project),
        )
        for project in projects:
            added = project_ri.get(project, Decimal("0"))
            assigned = assigned_by_target.get(project, Decimal("0"))
            before = project_before.get(project, Decimal("0"))
            after = project_after.get(project, Decimal("0"))
            writer.writerow(
                [
                    project,
                    str(before),
                    str(after),
                    str(after - before),
                    str(added),
                    str(assigned),
                ]
            )

    summary = {
        "allocationMode": "reservations",
        "mappings": [
            {
                "reservationId": reservation_id,
                "matchMode": ri_modes[reservation_id],
                "appliedScopeType": ri_scopes[reservation_id][0],
                "appliedScopeId": ri_scopes[reservation_id][1],
                "managementGroupSubscriptionCount": (
                    len(management_group_subscriptions[ri_scopes[reservation_id]])
                    if ri_scopes[reservation_id][0] == "managementgroup"
                    else None
                ),
                "boundTotal": str(ri_denominators[reservation_id]),
                "boundWeightSum": str(
                    sum(
                        (weight for _target, weight in ri_targets[reservation_id]),
                        Decimal("0"),
                    )
                ),
                "targets": [
                    {"key": target[0], "value": target[1], "weight": str(weight)}
                    for target, weight in ri_targets[reservation_id]
                ],
            }
            for reservation_id in sorted(ri_targets)
        ],
        "targets": ["=".join(target) for target in targets],
        "reservationIds": sorted(reservation_ids),
        "amountField": args.amount_field,
        "allocatedCostField": allocated_field,
        "priceSheetSource": price_sheet_source,
        "priceBasis": "Azure Price Sheet Consumption unitPrice × quantity",
        "matchModeByReservation": {
            reservation_id: ri_modes[reservation_id]
            for reservation_id in sorted(ri_modes)
        },
        "costScope": "meterCategory == Virtual Machines",
        "riSelection": {
            "pricingModel": "Reservation",
            "chargeType": "Usage",
            "reservationId": sorted(reservation_ids),
        },
        "inputFiles": source_files,
        "outputFiles": output_paths + [str(allocation_path)],
        "totalRows": total_rows,
        "riUsageRows": ri_usage_rows,
        "riReallocatedRows": ri_reallocated_rows,
        "riSizeFlexibleRows": ri_size_flexible_rows,
        "riServiceTypes": {
            reservation_id: sorted(models)
            for reservation_id, models in sorted(ri_service_types.items())
        },
        "riAllocatableAmount": str(ri_amount),
        "riRawTotalAmount": str(ri_raw_total),
        "riPaygEquivalentAmount": str(ri_payg_total),
        "riAmortizedCost": str(ri_raw_total),
        "riGrossBenefit": str(ri_gross_benefit_total),
        "riExcessCost": str(ri_excess_cost_total),
        "riNetBenefitOrLoss": str(ri_gross_savings_total),
        "riUnusedCost": str(ri_unused_cost_total),
        "riPortfolioNetSavings": str(
            ri_gross_savings_total - ri_unused_cost_total
        ),
        "riSavingsByReservation": {
            reservation_id: {
                "netBenefitOrLoss": str(gross_by_reservation[reservation_id]),
                "grossBenefit": str(benefit_by_reservation[reservation_id]),
                "excessCost": str(excess_cost_by_reservation[reservation_id]),
                "unusedCost": str(unused_by_reservation[reservation_id]),
                "portfolioNetSavings": str(
                    gross_by_reservation[reservation_id]
                    - unused_by_reservation[reservation_id]
                ),
            }
            for reservation_id in sorted(reservation_ids)
        },
        "targetVmReceiverAmount": str(target_non_ri_total),
        "assignedByTarget": {
            target_value: str(amount)
            for target_value, amount in sorted(assigned_by_target.items())
        },
        "riAllocationKeys": [
            {
                "appliedScopeType": scope[0],
                "appliedScopeId": scope[1],
                "target": "=".join(target),
                "matchKey": alloc_key[0],
                "region": alloc_key[1],
                "riAmount": str(ri_amount_by_key[(scope, target, alloc_key)]),
                "targetVmReceiverAmount": str(
                    target_non_ri_total_by_key.get(
                        (scope, target, alloc_key), Decimal("0")
                    )
                ),
            }
            for (scope, target, alloc_key) in sorted(ri_amount_by_key)
        ],
        "sourceFilesModified": False,
        "resourceTagsModified": False,
        "resourceIdsModified": False,
        "detailRowsHaveAllocatedCost": not args.summary_only,
    }
    summary_path = output_dir / "ri-summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    print(f"分摊目标数：{len(targets)}")
    print(f"RI 使用记录：{ri_usage_rows} 条")
    print(f"RI PAYG 等价成本：{ri_payg_total}")
    print(f"RI 成本基准（{args.amount_field}）：{ri_raw_total}")
    print(f"RI 使用正收益：{ri_gross_benefit_total}")
    print(f"RI 使用超额成本：{ri_excess_cost_total}")
    print(f"RI 使用净收益/损失：{ri_gross_savings_total}")
    print(f"RI 未使用成本：{ri_unused_cost_total}")
    print(f"RI 组合净收益（仅汇总）：{ri_gross_savings_total - ri_unused_cost_total}")
    print(f"按 binding 待分摊使用收益：{ri_amount}")
    print(f"目标项目虚拟机接收费用：{target_non_ri_total}")
    print(f"输出目录：{output_dir}")
    print(f"汇总文件：{summary_path}")


if __name__ == "__main__":
    main()
