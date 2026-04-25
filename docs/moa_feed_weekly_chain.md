# 农业农村部周报链整合表说明

## 目标

把同一类“畜产品和饲料集贸市场价格情况”周报在不同年份、不同站点路径下的分散发布链整合为一张统一表，当前聚焦字段：

- `corn_price`
- `soymeal_price`

## 整合范围

当前整合了三段来源：

### 1. 旧公开栏目周报

- 来源表：`data/interim/moa_xm_weekly_parsed/moa_xm_weekly_prices.csv`
- 时间覆盖：`2011-01-14` 到 `2015-06-23`
- 分段标记：`moa_xm_weekly`

### 2. 2016-2018 补充段

- 来源表：`data/interim/moa_feed_weekly_legacy_supplement_parsed/moa_feed_weekly_legacy_supplement_prices.csv`
- 时间覆盖：`2016-03-08` 到 `2018-12-11`
- 分段标记：`moa_feed_weekly_legacy_supplement`

说明：

- 这一段来自分散站点路径下的样例 URL 补充
- 当前以稳定 seed 列表方式接入

### 3. 2019-2022 `scs` 补充段

- 来源表：`data/interim/scs_feed_weekly_parsed/scs_feed_weekly_prices.csv`
- 时间覆盖：`2019-04-23` 到 `2022-09-20`
- 分段标记：`scs_feed_weekly`

## 统一表

- 输出表：`data/interim/moa_feed_weekly_chain_parsed/moa_feed_weekly_chain_prices.csv`

字段：

- `article_url`
- `title`
- `publish_date`
- `week_label`
- `collect_date`
- `corn_price`
- `corn_price_unit`
- `soymeal_price`
- `soymeal_price_unit`
- `source_segment`
- `raw_html_path`
- `raw_text_path`
- `parsing_method`
- `validation_flag`
- `validation_notes`

## 去重规则

整合时按：

- `publish_date`
- `week_label`

做键控合并。

当前合并优先级是：

1. `moa_xm_weekly`
2. `moa_feed_weekly_legacy_supplement`
3. `scs_feed_weekly`

后写入的分段会覆盖前面的相同键记录。

## 当前结果

- 总记录数：`287`
- 时间覆盖：`2011-01-14` 到 `2022-09-20`
- `corn_price` 非空：`283`
- `soymeal_price` 非空：`287`

分段构成：

- `moa_xm_weekly`：`181`
- `moa_feed_weekly_legacy_supplement`：`10`
- `scs_feed_weekly`：`96`

## 使用建议

如果你的任务是只做：

- 全国周度玉米价格
- 全国周度豆粕价格

优先直接使用这张整合表：

- `data/interim/moa_feed_weekly_chain_parsed/moa_feed_weekly_chain_prices.csv`

如果要核对分段来源，再分别回看三张底表。

## 生成方式

- `2016-2018` 补充抓取脚本：`scripts/moa_feed_weekly_legacy_supplement_crawler.py`
- `2019-2022` 补充段脚本：`scripts/scs_feed_weekly_crawler.py`
- 整合脚本：`scripts/moa_feed_weekly_chain_merge.py`
