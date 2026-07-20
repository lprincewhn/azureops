# RI 费用重新分摊说明

## 1. 目的

本方案用于生成一份新的 Azure 成本明细副本，将指定 `reservationId` 的 RI 使用金额重新分摊到指定标签对应的项目，同时保留源文件不变。

原始资源的以下字段保持不变：

- `tags`
- `ResourceId`
- `pricingModel`
- 原始 `costInBillingCurrency`

输出明细新增分摊字段，不覆盖 Azure 原始账单字段。

## 2. RI 使用记录识别规则

同时满足以下条件的记录被识别为实际 RI 使用记录：

```text
pricingModel = Reservation
chargeType = Usage
reservationId = 命令行指定的 reservationId
```

当前脚本只对以下类别进行项目级分摊：

```text
meterCategory = Virtual Machines
```

RI 金额默认使用：

```text
costInBillingCurrency
```

可通过重复指定 `--reservation-id` 同时选择多个 RI；必须通过 `--target-tag key=value` 指定优惠收益接收项目，例如 `--target-tag projname=fota`。RI 收益只会分配给与 RI 使用记录同时匹配以下字段的目标项目明细：

```text
机型 = additionalInfo.ServiceType
区域 = meterRegion（缺失时依次使用 resourceLocation、location）
```

## 3. 分摊逻辑

### 3.1 RI 使用记录

对于不匹配目标标签的 RI 使用记录，将该行的 RI 使用金额加回资源成本：

```text
allocatedCostInBillingCurrency
  = costInBillingCurrency + RI使用金额
```

因此：

```text
allocatedCostInBillingCurrency > costInBillingCurrency
```

这些记录标记为：

```text
allocationType = RI_USAGE_COST_REASSIGNED
allocationTarget = 目标标签值
riAllocationAmount = 正数
```

### 3.2 目标项目明细

目标范围为：

```text
meterCategory = Virtual Machines
目标标签 key=value
且不是实际 RI 使用记录
```

RI 金额按相同机型、相同区域的目标项目明细原始虚拟机费用比例分摊，不同机型或区域的虚拟机不会承接该 RI 收益。

设：

```text
RI总金额 = 同一机型和区域下所有不匹配目标标签的指定 RI 使用记录金额合计
目标项目非RI虚拟机费用总额 = 同一机型和区域下所有目标项目明细的原始费用合计
```

每一条 `fota` 明细的分摊金额为：

```text
该行分摊金额
  = RI总金额
  × 该行原始费用
  ÷ 目标项目非RI虚拟机费用总额
```

分摊后的金额为：

```text
allocatedCostInBillingCurrency
  = costInBillingCurrency - 该行分摊金额
```

因此：

```text
allocatedCostInBillingCurrency < costInBillingCurrency
```

这些记录标记为：

```text
allocationType = RI_BENEFIT_ASSIGNED
allocationTarget = 目标标签值
riAllocationAmount = 负数
```

### 3.3 金额守恒

分摊前后虚拟机费用总额保持不变：

```text
非目标标签 RI 记录增加金额
  = 相同机型和区域的目标项目明细扣减金额
```

脚本使用 `Decimal` 计算，避免浮点数误差。

## 4. 输出字段

输出明细在原有字段后追加：

| 字段 | 说明 |
|---|---|
| `allocatedCostInBillingCurrency` | 分摊后的计算金额 |
| `riAllocationAmount` | 本行 RI 分摊调整金额，正数为加回，负数为扣减 |
| `allocationType` | 分摊类型 |
| `allocationTarget` | 分摊目标项目，即目标标签的值 |

原始 `costInBillingCurrency` 不修改，便于对账。

## 5. 脚本文件

```text
reallocate_ri_to_fota.py
```

脚本默认不会修改源文件。

## 6. 执行方法

在包含源 CSV 的目录执行：

```bash
python3 reallocate_ri_to_fota.py \
  part_0_0001.csv \
  part_1_0001.csv \
  --reservation-id 8345b648-839b-4fdc-acbc-a776bdfe00d5 \
  --target-tag projname=fota \
  --output-dir ri-reallocated
```

也可以使用 glob：

```bash
python3 reallocate_ri_to_fota.py \
  "part_*_0001.csv" \
  --reservation-id 8345b648-839b-4fdc-acbc-a776bdfe00d5 \
  --target-tag projname=fota \
  --output-dir ri-reallocated
```

指定多个 RI：

```bash
python3 reallocate_ri_to_fota.py \
  part_0_0001.csv \
  part_1_0001.csv \
  --reservation-id ri-id-1 \
  --reservation-id ri-id-2 \
  --target-tag projname=fota \
  --output-dir ri-reallocated
```

使用其他标签作为接收项目条件：

```bash
python3 reallocate_ri_to_fota.py \
  part_0_0001.csv \
  part_1_0001.csv \
  --reservation-id 8345b648-839b-4fdc-acbc-a776bdfe00d5 \
  --target-tag app=nacos \
  --output-dir ri-reallocated
```

指定其他金额字段：

```bash
python3 reallocate_ri_to_fota.py \
  part_0_0001.csv \
  part_1_0001.csv \
  --reservation-id 8345b648-839b-4fdc-acbc-a776bdfe00d5 \
  --target-tag projname=fota \
  --amount-field costInUsd \
  --output-dir ri-reallocated
```

只生成汇总、不生成明细副本：

```bash
python3 reallocate_ri_to_fota.py \
  part_0_0001.csv \
  part_1_0001.csv \
  --reservation-id 8345b648-839b-4fdc-acbc-a776bdfe00d5 \
  --target-tag projname=fota \
  --summary-only \
  --output-dir ri-reallocation-summary
```

## 7. 输出文件

执行完成后，输出目录包含：

```text
ri-reallocated/
├── part_0_0001.csv
├── part_1_0001.csv
├── project-allocation.csv
└── ri-summary.json
```

### 明细 CSV

原始明细复制到新文件中，只有新增的分摊字段会体现计算结果；`tags` 和 `ResourceId` 保持原值。

### project-allocation.csv

包含每个项目的：

- 分摊前金额
- 分摊后金额
- 变化金额
- 加回的 RI 金额
- 分配给目标项目的 RI 金额

### ri-summary.json

包含：

- 输入文件
- 输出文件
- RI 记录数量
- RI 分摊记录数量
- RI 金额
- 目标项目非 RI 虚拟机费用
- 源文件是否被修改
- 标签和资源 ID 是否被修改

## 8. 注意事项

1. 源 CSV 不会被覆盖。
2. 脚本不会修改 `tags` 和 `ResourceId`。
3. 脚本不会把 `pricingModel=Reservation` 改成 `OnDemand`。
4. 如果目标标签对应项目中没有匹配机型和区域的非 RI 虚拟机费用，或费用小于对应 RI 金额，脚本会报错并停止，避免跨规格、跨区域分摊或产生负费用。
5. 输出文件中的 `allocatedCostInBillingCurrency` 是分摊分析金额，不是 Azure 原始账单字段。

## 9. 对账脚本

`reconcile_ri_allocation.py` 用于比较原始账单和分摊后账单，按 `ResourceId` 汇总每台虚拟机的处理前费用、处理后费用和变化金额，并补充区域与机型。

执行命令：

```bash
python3 reconcile_ri_allocation.py \
  "part_*_0001.csv" \
  --after-dir ri-reallocated \
  --output-dir ri-reallocated
```

本次执行日志：

```text
虚拟机资源数：317
费用发生变化的虚拟机数：18
处理前合计：26170.556282735074312
处理后合计：26170.55628273507431200000000
变化合计：0E-23
```

对账输出：

```text
ri-reallocated/
├── vm-cost-comparison.csv
└── changed-vm-cost-comparison.csv
```

其中 `changed-vm-cost-comparison.csv` 仅包含发生费用变化的虚拟机，并包含 `region`、`vmModel`、处理前费用、处理后费用和 `feeChangeInBillingCurrency`。

## 10. 本次分摊执行日志

分摊命令：

```bash
python3 reallocate_ri_to_fota.py \
  part_0_0001.csv part_1_0001.csv \
  --reservation-id 8345b648-839b-4fdc-acbc-a776bdfe00d5 \
  --target-tag projname=fota \
  --output-dir ri-reallocated
```

执行结果：

```text
RI 使用记录：24 条
RI 使用金额：12.776931506849312
目标项目非 RI 虚拟机费用：510.8680547140
输出目录：ri-reallocated
汇总文件：ri-reallocated/ri-summary.json
```

## 11. 机型与区域匹配规则变更

分摊算法已更新为按 `additionalInfo.ServiceType` 和区域进行匹配。当前账单中指定 RI 的匹配键为：

```text
机型：Standard_D2s_v5
区域：US West 3
RI 金额：12.776931506849312
```

当前 `fota` 非 RI 虚拟机的可用匹配明细位于 `AP Southeast`，没有 `Standard_D2s_v5 + US West 3` 的目标明细。因此再次执行上述示例命令时，脚本会显式报错并停止，不会生成跨区域分摊结果。

## 12. observe-platform 分摊执行记录

将接收项目改为 `observe-platform` 后，命令如下：

```bash
python3 reallocate_ri_to_fota.py \
  part_0_0001.csv part_1_0001.csv \
  --reservation-id 8345b648-839b-4fdc-acbc-a776bdfe00d5 \
  --target-tag projname=observe-platform \
  --output-dir ri-reallocated-observe-platform
```

执行及对账结果：

```text
RI 使用记录：24 条
RI 使用金额：12.623371726465751
目标项目非 RI 虚拟机费用：586.3782283132
虚拟机资源数：317
费用发生变化的虚拟机数：4
变化合计：0E-23
```
