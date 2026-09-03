# Azure Pricing Sheet 下载工具

提供两个独立命令，通过当前 `az login` 身份调用 Azure Cost Management API，
等待 Pricing Sheet 生成后下载到本地：

- `download_by_billing_profile.py`：下载 Billing Profile 当前月份的 Pricing Sheet。
- `download_by_invoice.py`：下载指定 Invoice ID 对应账期的 Pricing Sheet。

## 准备

```bash
cd pricing-sheet
pip install -r requirements.txt
az login
```

登录身份需要对目标 Billing Profile 具有读取价格表的权限。`billing_account`
应使用完整 MCA Billing Account Name（可能包含 `:<tenant-id>_<date>` 后缀），
而不是成本导出文件中的短 ID。

## 按 Billing Profile 下载

```bash
python download_by_billing_profile.py \
  "<billing-account-name>" \
  "<billing-profile-name>" \
  --output pricesheet-current.zip
```

Billing Profile API 只提供当前月份的 Pricing Sheet。

## 按 Invoice ID 下载

```bash
python download_by_invoice.py \
  "<billing-account-name>" \
  "<billing-profile-name>" \
  "<invoice-id>" \
  --output pricesheet-invoice.zip
```

两个命令默认最长等待 1800 秒，可通过 `--timeout <seconds>` 调整。目标目录
不存在时会自动创建；下载完成前数据写入临时文件，成功后才替换目标文件。
