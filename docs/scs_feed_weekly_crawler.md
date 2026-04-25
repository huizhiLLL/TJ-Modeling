# 市场与信息化司隐藏周报补充源抓取说明

## 来源定位

这个来源用于补充 `2019-2022` 期间周报中的：

- `corn_price`
- `soymeal_price`

这批文章与前面周报源内容相似，但不在公开列表里稳定可枚举，因此这里采用保守方案接入。

## 目标文章类型

这批文章标题主要是：

- `x月份第x周畜产品和饲料集贸市场价格情况`
- `x月11-17日畜产品和饲料集贸市场价格情况`

样例：

- `https://scs.moa.gov.cn/jcyj/202201/t20220112_6411227.htm`
- `https://scs.moa.gov.cn/jcyj/202101/t20210120_6411011.htm`
- `https://scs.moa.gov.cn/jcyj/201904/t20190425_6410589.htm`

## 发现方式

### 当前方案

由于：

- `jcyj` 公开分页页中无法直接枚举出这批老周报
- 官方站内搜索对精确标题召回不稳定

所以当前采用：

- 搜索发现一批稳定可访问的文章
- 将文章 URL 固化为种子列表
- 脚本按种子列表批量抓取

这是一种保守但稳妥的方式，优先保证可重复抓取和口径稳定。

### 当前种子规模

- 种子文章：`96`

## 原始层落盘

- 列表索引：`data/raw/scs_feed_weekly_articles/index_pages/article_index.csv`
- 正文 html：`data/raw/scs_feed_weekly_articles/article_html/`
- 正文 text：`data/raw/scs_feed_weekly_articles/article_text/`
- 文章 meta：`data/raw/scs_feed_weekly_articles/article_meta/`

## 结构化表

- `data/interim/scs_feed_weekly_parsed/scs_feed_weekly_prices.csv`

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
- `raw_html_path`
- `raw_text_path`
- `parsing_method`
- `validation_flag`
- `validation_notes`

## 抽取方式

当前正文抽取采用：

- 正文文本正则

核心规则：

- `全国玉米平均价格([0-9.]+)元/公斤`
- `全国豆粕平均价格([0-9.]+)元/公斤`

这个站点的文本正文虽然有较多空白和导航噪音，但把全文压缩后，这两个字段能比较稳定命中。

## 当前全量结果

- 种子文章：`96`
- 成功处理：`96`
- 失败：`0`
- 时间覆盖：`2019-04-23` 到 `2022-09-20`
- `corn_price` 非空：`92`
- `soymeal_price` 非空：`96`

人工复核项：

- `2020-09-29`
- `2020-10-13`
- `2020-10-20`
- `2020-10-27`

这些文章当前缺的是 `corn_price`，但 `soymeal_price` 已成功提取。

## 使用建议

这个来源适合做：

- 2019-2022 周度玉米、豆粕价格补充
- 与其他周报源按 `publish_date / week_label` 做拼接校验

不建议把它当成这个站点的“完整公开归档”，因为当前发现层本质上还是种子列表驱动。

## 运行命令

全量抓取：

```powershell
& 'C:\Users\31691\AppData\Local\Programs\Python\Python313\python.exe' scripts\scs_feed_weekly_crawler.py
```

小样本验证：

```powershell
& 'C:\Users\31691\AppData\Local\Programs\Python\Python313\python.exe' scripts\scs_feed_weekly_crawler.py --max-articles 8
```
