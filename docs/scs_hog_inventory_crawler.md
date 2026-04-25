# 市场与信息化司生猪存栏变化率补充源抓取说明

## 来源定位

这个来源的目标不是补“绝对存栏量”，而是补：

- 生猪存栏环比变化率
- 生猪存栏同比变化率
- 能繁母猪存栏环比变化率
- 能繁母猪存栏同比变化率

适用时间段：

- `2018`
- `2019`

## 发现方式

这个来源没有稳定公开的文章列表，官方站内搜索召回也不够可靠，因此当前采用：

- 外部搜索发现候选文章
- 人工核对标题
- 固化成种子 URL 列表

当前脚本内置了 `18` 篇已验证的种子文章。

注意：

- 这意味着当前发现层是“半自动”
- 如果后续发现新文章，只需要把 seed 列表补进去，再重跑脚本即可

## 目标标题类型

主标题主要有两类：

- `201x年x月份400个监测县生猪存栏信息`
- `201x年x月份生猪存栏信息`

兼容补充：

- `201x年1—3月份400个监测县生猪存栏信息`

## 原始层落盘

- 列表索引：`data/raw/scs_hog_inventory_articles/index_pages/article_index.csv`
- 正文 html：`data/raw/scs_hog_inventory_articles/article_html/`
- 正文 text：`data/raw/scs_hog_inventory_articles/article_text/`
- 文章 meta：`data/raw/scs_hog_inventory_articles/article_meta/`

## 结构化表

- `data/interim/scs_hog_inventory_parsed/scs_hog_inventory_changes.csv`

字段：

- `article_url`
- `title`
- `publish_date`
- `report_period`
- `report_year`
- `report_month`
- `period_type`
- `sample_scope`
- `sub_period_label`
- `hog_inventory_mom_pct`
- `hog_inventory_yoy_pct`
- `breeding_sow_inventory_mom_pct`
- `breeding_sow_inventory_yoy_pct`
- `raw_html_path`
- `raw_text_path`
- `validation_flag`
- `validation_notes`

## 页面结构说明

### 1. 月度页

月度页通常只有一个简单表格：

- `月份`
- `生猪存栏`
- `能繁母猪存栏`
- `比上月增减`
- `比去年同期增减`

结构化时直接抽成一条记录。

### 2. 季度页

如：

- `2018年1—3月份400个监测县生猪存栏信息`

这类页面的表格是逐月列出：

- `1月`
- `2月`
- `3月`

并分别给出四个变化率字段。

结构化时已展开成多条月度记录，`period_type=month_from_quarter_range`。

## 当前全量结果

- 种子文章：`18`
- 结构化记录：`20`
- 失败：`0`
- 时间覆盖：`2018-02-13` 到 `2019-11-29`

字段完整度：

- `hog_inventory_yoy_pct` 非空：`20`
- `breeding_sow_inventory_yoy_pct` 非空：`20`
- `hog_inventory_mom_pct` 非空：`19`
- `breeding_sow_inventory_mom_pct` 非空：`19`

当前仅有 1 条人工复核项：

- `2018年1—3月份400个监测县生猪存栏信息`
- 原因：`1月` 的环比字段原文为 `—`

这类缺失属于原文未提供，不是解析失败。