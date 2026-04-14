# 农业农村部生猪专题月度数据抓取说明

## 来源说明

- 专题根路径：`https://www.moa.gov.cn/ztzl/szcpxx/jdsj/`
- 数据形式：
  - 页面内直接展示 HTML 表格
  - 同时提供 `表格下载` Excel
- 当前目标字段：
  - 能繁母猪存栏
  - 全国生猪出场价格
- 同时落一张“指标长表”，保留全部页面指标

## URL 规则

### 常规月页

- `2021-12` 到 `2026-02`：
  - `https://www.moa.gov.cn/ztzl/szcpxx/jdsj/<year>/<yyyymm>/`

示例：

- `https://www.moa.gov.cn/ztzl/szcpxx/jdsj/2023/202312/`
- `https://www.moa.gov.cn/ztzl/szcpxx/jdsj/2026/202602/`

### 2021 年 4 月到 11 月特殊路径

- `https://www.moa.gov.cn/ztzl/szcpxx/jdsj/202108/`

即：

- `202104/` 到 `202111/`

### 2021 年一季度特殊页

- `https://www.moa.gov.cn/ztzl/szcpxx/jdsj/2021yjd/`

该页单独代表 `2021Q1`

## 抓取策略

### 主策略

- 优先直接解析页面 HTML 表格
- 仅当 HTML 表格异常时，回退解析 Excel

### 留痕

- 页面 html：`data/raw/moa_hog_monthly/page_html/`
- 页面 text：`data/raw/moa_hog_monthly/page_text/`
- 页面 meta：`data/raw/moa_hog_monthly/page_meta/`
- 原始 Excel：`data/raw/moa_hog_monthly/excel_files/`
- 页面索引：`data/raw/moa_hog_monthly/index_pages/page_index.csv`

## 输出表设计

### 1. 指标长表

文件：

- `data/interim/moa_hog_monthly_parsed/moa_hog_indicators_long.csv`

### 2. 核心宽表

文件：

- `data/interim/moa_hog_monthly_parsed/moa_hog_core_metrics.csv`

核心宽表字段：

- `period_key`
- `report_period`
- `period_type`
- `report_year`
- `report_month`
- `page_url`
- `excel_url`
- `publish_date`
- `publish_date_source`
- `breeding_sow_inventory`
- `breeding_sow_inventory_unit`
- `breeding_sow_inventory_indicator_raw`
- `breeding_sow_inventory_mom`
- `breeding_sow_inventory_yoy`
- `breeding_sow_inventory_period_type`
- `hog_exfarm_price`
- `hog_exfarm_price_unit`
- `hog_exfarm_price_indicator_raw`
- `hog_exfarm_price_mom`
- `hog_exfarm_price_yoy`
- `parsing_source`
- `raw_html_path`
- `raw_excel_path`
- `validation_flag`
- `validation_notes`

## 字段口径说明

### 1. publish_date

该专题页本身没有稳定的页面发布时间字段，因此：

- `publish_date` 当前取自 Excel 附件文件名中的日期 token
- `publish_date_source = excel_url_token`

这是一种代理发布时间，不是页面显式时间字段。

### 2. 全国生猪出场价格

- 月月都有
- 单位统一为 `元/公斤`
- 当前抓取成功率为 `100%`

### 3. 能繁母猪存栏

- 不是所有月份都提供“绝对值存栏（万头）”
- `2021Q1` 到 `2021-08` 多为“定点监测能繁母猪存栏/变化”，未给出绝对值
- `2025-11`、`2026-01`、`2026-02` 页面中未抓到该指标
- 因此：
  - `breeding_sow_inventory` 允许为空
  - `validation_flag = sow_inventory_missing` 不一定代表解析失败，也可能是来源未发布绝对值

## 当前全量结果

- 页面总数：`60`
- 成功处理：`60`
- 失败页面：`0`
- 时间覆盖：`2021Q1` 到 `2026-02`
- 指标长表：`1507` 行
- 核心宽表：`60` 行
- `hog_exfarm_price` 非空率：`100%`
- `breeding_sow_inventory` 非空率：`85%`

人工复核重点时期：

- `2021Q1`
- `2021-04`
- `2021-05`
- `2021-06`
- `2021-07`
- `2021-08`
- `2025-11`
- `2026-01`
- `2026-02`

复核清单：

- `data/metadata/moa_hog_monthly_manual_review.csv`