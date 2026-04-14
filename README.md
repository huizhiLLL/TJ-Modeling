# TJ-Modeling

面向统计建模的数据准备仓库。

## 说明

优先看结果表：

- `data/interim/`
  这里是第一版结构化表，是建模时最直接的入口
相关 raw_data/logs 由于体积原因不推送，仅存在本地，仍然留痕可溯源

## 目录结构

```text
data/
  interim/
    agri_weekly_parsed/
    moa_hog_monthly_parsed/
    moa_xm_weekly_parsed/
  metadata/
scripts/
docs/
```

说明：

- `interim/`：第一版结构化结果，建模优先使用这里
- `metadata/`：摘要、人工复核清单、覆盖边界说明
- `docs/`：来源级说明文档，详细写 URL 规则、字段口径和异常说明

## 当前已接入的数据源

### 1. 中国农业信息网周报

来源：

- 中国农业信息网监测预警栏目“畜产品和饲料集贸市场价格情况”

时间覆盖：

- `2022-08-02` 到 `2026-04-08`

核心结果：

- 列表页访问：`50` 页
- 目标周报：`177` 篇
- 去重后最终唯一周记录：`175` 条
- 核心字段 `hog_price / corn_price / soymeal_price` 提取成功率：`100%`

建模入口：

- `data/interim/agri_weekly_parsed/agri_weekly_prices.csv`

字段设计：

- `article_url`
- `title`
- `publish_date`
- `week_label`
- `collect_date`
- `hog_price`
- `hog_price_unit`
- `corn_price`
- `corn_price_unit`
- `soymeal_price`
- `soymeal_price_unit`
- `piglet_price`
- `piglet_price_unit`
- `mixed_feed_price`
- `mixed_feed_price_unit`
- `parsing_method`
- `raw_text_path`
- `raw_html_path`
- `validation_flag`
- `validation_notes`

注意：

- 该来源最早只追到 `2022-08`，这属于当前公开归档边界，不是抓取失败
- 有少量重复周已做去重，详情见摘要

### 2. 农业农村部生猪专题月度数据页

来源：

- 农业农村部生猪专题月度数据页

时间覆盖：

- `2021Q1` 到 `2026-02`

核心结果：

- 页面总数：`60`
- 成功处理：`60`
- 指标长表：`1507` 行
- `hog_exfarm_price` 非空率：`100%`
- `breeding_sow_inventory` 非空率：`85%`

建模入口：

- `data/interim/moa_hog_monthly_parsed/moa_hog_core_metrics.csv`
- `data/interim/moa_hog_monthly_parsed/moa_hog_indicators_long.csv`

推荐使用方式：

- 只做核心建模：先用 `moa_hog_core_metrics.csv`
- 后续要扩展更多指标：转去 `moa_hog_indicators_long.csv`

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

注意：

- 页面本身没有稳定显式发布时间字段，当前 `publish_date` 是从 Excel 附件文件名推断出的代理发布日期
- `breeding_sow_inventory` 不是每个月都有绝对值，空值不一定表示解析失败，也可能是来源只发布了“变化”而非“存栏（万头）”

### 3. 农业农村部公开栏目“监测预警/畜牧”历史周报

来源：

- 农业农村部公开栏目 `https://www.moa.gov.cn/gk/jcyj/xm/`

时间覆盖：

- `2011-01-14` 到 `2015-06-23`

核心结果：

- 列表页访问：`25` 页
- 目标文章：`181` 篇
- 成功处理：`181` 篇
- `live_hog_price / corn_price / soymeal_price` 非空率均为 `100%`

建模入口：

- `data/interim/moa_xm_weekly_parsed/moa_xm_weekly_prices.csv`
- `data/interim/moa_xm_weekly_parsed/moa_xm_weekly_indicators_long.csv`

字段设计：

- `article_url`
- `title`
- `publish_date`
- `week_label`
- `collect_date`
- `live_hog_price`
- `live_hog_price_unit`
- `corn_price`
- `corn_price_unit`
- `soymeal_price`
- `soymeal_price_unit`
- `parsing_method`
- `raw_text_path`
- `raw_html_path`
- `validation_flag`
- `validation_notes`

注意：

- 这个旧源正文里多数写的是 `活猪`，所以字段名保留为 `live_hog_price`
- 如果后续要和写成 `生猪` 的其他来源合并，建议在建模层做统一映射，不要直接在原始结构化层改名
- 该来源只有 1 篇文章缺少 `collect_date`，但不影响核心价格字段

## 建模优先看的表

1. `data/interim/agri_weekly_parsed/agri_weekly_prices.csv`
   对应 `2022-08` 之后的周度价格，字段最直接。
2. `data/interim/moa_xm_weekly_parsed/moa_xm_weekly_prices.csv`
   对应 `2011-2015` 的历史周度价格，可补老阶段。
3. `data/interim/moa_hog_monthly_parsed/moa_hog_core_metrics.csv`
   对应月度生猪专题指标，适合补充能繁母猪存栏、全国生猪出场价格等中低频变量。

## 使用前要注意的口径问题

- `hog_price` 和 `live_hog_price`
  这两列来源口径接近，但原始表述不同。一个写“生猪”，一个写“活猪”。建模前建议显式做映射或统一命名。
- `publish_date` 和 `collect_date`
  这两个字段不是一回事。周报类数据建模时，通常更值得关注 `collect_date`；如果缺失，再退回 `publish_date`。
- 月度源的 `breeding_sow_inventory`
  空值是因为该月页面没给绝对量。
