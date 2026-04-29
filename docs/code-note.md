## 附录A 关键代码片段与说明

本文所涉完整代码、数据处理脚本已整理于 GitHub 仓库：`[https://github.com/huizhiLLL/TJ-Modeling]`

## 附表 A-1 代码片段与作用对照

| 编号 | 主题 | 主要作用 |
| --- | --- | --- |
| A-1 | 抓取留痕与分层保存 | 保证原始网页、正文文本、元信息、结构化结果和日志均可回溯 |
| A-2 | 周报字段抽取与质量标记 | 统一提取发布时间、采集日和核心价格字段，并记录异常状态 |
| A-3 | 多来源周报链拼接与去重 | 将不同年份、不同来源的周报整合为统一时间序列 |
| A-4 | 日度数据转周度样本 | 将河南省日度猪价整理为周度目标变量并保留缺失信息 |

## A.1 抓取留痕与分层保存

为保证抓取结果可重跑、可核对，本文将抓取数据按原始层、中间层和元数据层分别保存。原始层保留列表索引、文章 HTML、正文文本和页面元信息；中间层保留结构化后的价格表；元数据层保留运行日志与质量摘要。其核心实现如下：

```python
def init_paths(project_root):
    data_root = project_root / "data"
    article_root = data_root / "raw" / "agri_weekly_articles"
    index_root = article_root / "index_pages"
    html_root = article_root / "article_html"
    text_root = article_root / "article_text"
    meta_root = article_root / "article_meta"
    interim_root = data_root / "interim" / "agri_weekly_parsed"
    logs_root = data_root / "logs"
    metadata_root = data_root / "metadata"

    for path in [index_root, html_root, text_root, meta_root, interim_root, logs_root, metadata_root]:
        path.mkdir(parents=True, exist_ok=True)
```

上述设计确保论文中的结构化结果能够追溯到原始网页内容，并为后续字段校验、异常定位和重复实验提供统一的数据基础。

## A.2 周报字段抽取与质量标记

对周报文章，本文优先根据页面正式时间字段和正文中的“采集日”表述确定时间信息；对于价格指标，则采用“表格优先、正文正则回退”的抽取策略。同时，对核心字段缺失等情况生成质量标记。核心逻辑如下：

```python
def parse_collect_date(text, publish_date):
    match = re.search(r"采集日为(\d{1,2})月(\d{1,2})日", text.replace(" ", ""))
    if not match or not publish_date:
        return None
    year = publish_date[:4]
    return f"{year}-{int(match.group(1)):02d}-{int(match.group(2)):02d}"


def parse_article_record(article_meta):
    week_label = week_label_from_title(article_meta["title"])
    collect_date = parse_collect_date(article_meta["content_text"], article_meta["publish_date"])

    table_values = parse_table_values(article_meta["content_html"])
    regex_values = parse_regex_values(article_meta["content_text"])
    parsed_values = prefer_table_else_regex(table_values, regex_values)

    missing_core = [f for f in CORE_FIELDS if parsed_values.get(f) is None]
    validation_flag = "missing_core_fields" if missing_core else "ok"
```

这一处理方式避免了直接使用 URL 中的日期痕迹代替正式发布时间，也避免了在页面结构变化时完全依赖单一抽取方法。质量标记进一步为后续人工复核和模型稳健性分析提供了依据。

## A.3 多来源周报链拼接与去重

由于全国周度价格数据分散发布于不同年份和不同站点路径下，本文未将其视为单一连续来源，而是先分别抓取，再按统一字段口径进行拼接。拼接时，以 `publish_date` 与 `week_label` 组成复合键进行去重，并保留来源标识。核心逻辑如下：

```python
def to_key(row):
    return (row.get("publish_date", ""), row.get("week_label", ""))


merged = {}

for row in source_moa_xm_weekly:
    merged[to_key(row)] = {**row, "source_segment": "moa_xm_weekly"}

for row in source_legacy_supplement:
    merged[to_key(row)] = {**row, "source_segment": "moa_feed_weekly_legacy_supplement"}

for row in source_scs_weekly:
    merged[to_key(row)] = {**row, "source_segment": "scs_feed_weekly"}
```

这一过程保证了不同来源之间的统一口径，也保留了每条记录的可回溯来源，从而使跨年份周报序列既连续又可核验。

## A.4 日度数据转周度建模样本

河南省猪价原始数据以日度文章形式发布。为与研究中的周度建模框架相匹配，本文按自然周对日度数据进行聚合，并同时保留有效观测天数与缺失比例等质量信息。核心实现如下：

```python
daily["week_start"] = get_week_start(daily["publish_datetime"])
daily["week_end"] = get_week_end(daily["week_start"])

weekly = (
    daily.groupby("week_start", sort=True)
    .agg(
        week_end=("week_end", "first"),
        obs_days=("publish_datetime", "count"),
        valid_days=("avg_price", lambda s: int(s.notna().sum())),
        target_price_median=("avg_price", "median"),
        target_price_mean=("avg_price", "mean"),
        target_price_last=("avg_price", last_valid),
    )
    .sort_index()
)

weekly["missing_ratio"] = ((7 - weekly["valid_days"]) / 7).round(4)
weekly["is_source_gap"] = weekly["valid_days"].eq(0)
```

本文以周内价格中位数作为严格版主值，同时保留均值、末次有效值、有效观测天数和缺失比例，以兼顾稳健性和后续建模灵活性。对于完全无有效观测的周次，不直接删除，而是保留为显式缺口。