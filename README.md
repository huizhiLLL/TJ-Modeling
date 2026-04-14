# TJ-Modeling

面向统计建模的数据准备仓库。

## 怎么读

优先看两层：

- `data/interim/`
  第一版结构化结果，通常是建模的直接入口。
- `data/metadata/`
  摘要、人工复核清单、覆盖范围说明，用来判断哪些表可以直接用，哪些还要补校验。

说明：

- `raw/` 和 `logs/` 出于体积考虑不默认推送，但本地会完整保留，保证可追溯。
- 如果要核对原文、重做抽取、排查异常，需要回到本地 `data/raw/` 和 `data/logs/`。

## 推荐目录结构

```text
data/
  raw/
    <source_name>/
  interim/
    agri_weekly_parsed/
    moa_hog_monthly_parsed/
    moa_xm_weekly_parsed/
    soozhu_henan_hog_daily_parsed/
  metadata/
scripts/
docs/
```

说明：

- `interim/`：结构化结果主表和长表
- `metadata/`：摘要、复核清单、覆盖边界
- `scripts/`：抓取脚本
- `docs/`：来源级说明文档

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
- `hog_price / corn_price / soymeal_price` 提取成功率：`100%`

建模入口：

- `data/interim/agri_weekly_parsed/agri_weekly_prices.csv`

主要字段：

- `publish_date`
- `week_label`
- `collect_date`
- `hog_price`
- `corn_price`
- `soymeal_price`
- `piglet_price`
- `mixed_feed_price`
- `validation_flag`
- `validation_notes`

注意：

- 该来源当前最早只到 `2022-08`，这是公开归档边界，不是抓取失败。

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

- 核心建模先看 `moa_hog_core_metrics.csv`
- 需要扩展更多月度指标时再看 `moa_hog_indicators_long.csv`

核心字段：

- `period_key`
- `report_period`
- `publish_date`
- `breeding_sow_inventory`
- `hog_exfarm_price`
- `hog_exfarm_price_mom`
- `hog_exfarm_price_yoy`
- `validation_flag`
- `validation_notes`

注意：

- 页面本身没有稳定显式发布时间字段，当前 `publish_date` 是从 Excel 附件文件名推断出的代理发布日期。
- `breeding_sow_inventory` 为空不一定表示解析失败，可能是该月只发布了“变化”而没给绝对值。

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

主要字段：

- `publish_date`
- `week_label`
- `collect_date`
- `live_hog_price`
- `corn_price`
- `soymeal_price`
- `validation_flag`
- `validation_notes`

注意：

- 这个旧源正文多数写的是 `活猪`，所以字段名保留为 `live_hog_price`。
- 如果后续要和写成 `生猪` 的其他来源合并，建议在建模层做统一映射，不要直接在结构化层改名。

### 4. 搜猪网河南省瘦肉型肉猪日度行情

来源：

- 搜猪网站内检索：关键词 `河南省瘦肉型肉猪(110kg左右)价格`

时间覆盖：

- `2014-12-17 10:30` 到 `2026-04-14 10:30`

核心结果：

- 搜索结果页：`124` 页
- 搜索结果文章：`2478` 篇
- 成功处理：`2478` 篇
- 各地均价明细：`8012` 行
- `avg_price / last_year_price / mom_pct` 非空：`2354`
- `yoy_pct` 页面当前普遍未展示，默认留空

建模入口：

- `data/interim/soozhu_henan_hog_daily_parsed/soozhu_henan_hog_daily_prices.csv`
- `data/interim/soozhu_henan_hog_daily_parsed/soozhu_henan_hog_local_prices.csv`

推荐使用方式：

- 做河南省日度均价主序列：先看 `soozhu_henan_hog_daily_prices.csv`
- 要分析均价构成或做地区核查：再看 `soozhu_henan_hog_local_prices.csv`

主表字段：

- `publish_datetime`
- `province`
- `product_name`
- `avg_price`
- `last_year_price`
- `mom_pct`
- `yoy_pct`
- `validation_flag`
- `validation_notes`

明细表字段：

- `publish_datetime`
- `province`
- `city`
- `county`
- `price`
- `unit`

注意：

- 搜索分页不是换 URL 页码，而是通过 `PageNo` 参数请求 JSON 结果。
- 页面里稳定展示 `日均价格 / 去年同期 / 环比`，但 `同比` 标签多数存在而数值未展示，所以 `yoy_pct` 当前默认允许为空。
- 当前仍有 `124` 篇文章缺少主值，主要集中在部分日期页面结构不完整或值未正常展示，需要按建模需要决定是否剔除或补抓。

## 建模优先看的表

如果只是先把可用主表拉进建模环境，小枝建议按这个顺序看：

1. `data/interim/agri_weekly_parsed/agri_weekly_prices.csv`
   用于 `2022-08` 之后的官方周度价格。
2. `data/interim/moa_xm_weekly_parsed/moa_xm_weekly_prices.csv`
   用于补 `2011-2015` 的历史周度价格。
3. `data/interim/moa_hog_monthly_parsed/moa_hog_core_metrics.csv`
   用于补充月度的能繁母猪存栏和全国生猪出场价格。
4. `data/interim/soozhu_henan_hog_daily_parsed/soozhu_henan_hog_daily_prices.csv`
   用于河南省瘦肉型肉猪日度价格序列。

## 使用前要注意的口径问题

- `hog_price` 和 `live_hog_price`
  一个来源写“生猪”，一个来源写“活猪”，口径接近但不建议直接混名。
- `publish_date`、`publish_datetime` 和 `collect_date`
  不是同一概念。周报类数据通常优先用 `collect_date`，没有时再退回发布时间。
- 月度源的 `breeding_sow_inventory`
  空值不一定是解析失败，可能是页面没有发布绝对量。
- 搜猪网日度源的 `yoy_pct`
  当前页面多数不展示该值，空值是结构性缺失，不应直接当抓取失败处理。
