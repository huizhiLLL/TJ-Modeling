# 搜猪网河南省瘦肉型肉猪日度行情抓取说明

## 来源说明

- 搜索页：`https://www.soozhu.com/site/search/?key=河南省瘦肉型肉猪(110kg左右)价格`
- 文章页示例：`https://www.soozhu.com/article/516345/`

这个来源不是固定栏目列表，而是站内检索结果。目标文章都来自同一个搜索关键词，因此列表筛选逻辑比前几个来源更简单。

## 分页规则

分页不是通过显式 URL 切页，而是前端通过 GET 请求搜索接口返回 JSON：

```text
/site/search/?key=<关键词>&PageNo=<页码>&ListCountPerPage=20&timestamp=<时间戳>
```

当前已验证：

- 先访问搜索首页建立 session
- 再带 `PageNo` 参数请求时，可以直接返回 JSON

返回结构核心字段：

- `pageNo`
- `pageCount`
- `datacount`
- `obj_list`
- `viewurl`
- `pubdate`
- `author`

## 原始层落盘

- 搜索结果索引：`data/raw/soozhu_henan_hog_daily/search_pages/search_index.csv`
- 搜索页 JSON：`data/raw/soozhu_henan_hog_daily/search_pages/`
- 文章 html：`data/raw/soozhu_henan_hog_daily/article_html/`
- 文章 text：`data/raw/soozhu_henan_hog_daily/article_text/`
- 文章 meta：`data/raw/soozhu_henan_hog_daily/article_meta/`

## 结构化表

### 1. 文章级主表

- `data/interim/soozhu_henan_hog_daily_parsed/soozhu_henan_hog_daily_prices.csv`

字段：

- `search_keyword`
- `search_page_no`
- `article_id`
- `article_url`
- `title`
- `publish_datetime`
- `province`
- `product_name`
- `avg_price`
- `avg_price_unit`
- `last_year_price`
- `last_year_price_unit`
- `mom_pct`
- `yoy_pct`
- `raw_html_path`
- `parsing_method`
- `validation_flag`
- `validation_notes`

### 2. 各地均价明细表

- `data/interim/soozhu_henan_hog_daily_parsed/soozhu_henan_hog_local_prices.csv`

字段：

- `article_id`
- `article_url`
- `publish_datetime`
- `province`
- `city`
- `county`
- `price`
- `unit`

## 页面结构口径

文章页内通常会出现如下结构：

- 标题：`2026.4.14河南省瘦肉型肉猪(110kg左右)价格`
- 时间：`2026-04-14 10:30`
- 内容块：
  - `日均价格`
  - `去年同期`
  - `环比`
  - `同比`
  - `各地均价`

当前验证发现：

- `日均价格`、`去年同期`、`环比` 基本能稳定抓到
- `同比` 标签多数存在，但页面内往往没有实际数值展示，因此当前 `yoy_pct` 默认为可空

## 当前全量结果

- 搜索结果页：`124`
- 搜索结果文章：`2478`
- 成功处理：`2478`
- 各地均价明细：`8012` 行
- 时间覆盖：`2014-12-17 10:30` 到 `2026-04-14 10:30`
- `avg_price / last_year_price / mom_pct` 非空：`2354`
- `yoy_pct` 非空：`0`

说明：

- 这说明页面并不是所有文章都稳定展示主值
- 目前有 `124` 篇文章被标记为 `missing_key_metrics`
- 同时，`yoy_pct` 的缺失是页面结构性缺失，不默认判为抓取失败
