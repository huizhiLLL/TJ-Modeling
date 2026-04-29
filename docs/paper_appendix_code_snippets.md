# 论文附录代码片段推荐说明

本文档用于说明：在论文附录中，建议选取哪些关键代码片段或伪代码，以及每段代码对应要表达的方法含义。目标不是完整展示工程实现，而是突出本项目在数据获取、清洗整理、跨来源拼接与建模预处理上的可复核性与可复现性。

## 一、推荐结论

建议附录优先放以下 `4` 类核心代码：

1. 抓取留痕与分层保存
2. 周报字段提取与质量标记
3. 多来源周报链拼接与去重
4. 日度数据转周度建模样本

如果附录篇幅还允许，可以再补 `1` 段节假日/缺失周标记逻辑，作为质量控制补充。  
不建议放大段网络请求样板、长 URL 种子列表、整页 HTML 解析细节，因为这些内容工程意义大于论文表达价值。

## 二、附录代码片段对照表

| 片段编号 | 推荐主题 | 建议来源脚本 | 论文中要说明的问题 | 建议形式 |
| --- | --- | --- | --- | --- |
| A | 抓取留痕与分层保存 | `scripts/agri_weekly_crawler.py` | 原始数据如何保存，如何保证可回溯、可重跑 | 精简代码片段 |
| B | 字段提取与质量标记 | `scripts/agri_weekly_crawler.py` | 发布时间、采集日、价格字段如何抽取，异常如何标记 | 精简代码片段 |
| C | 多来源拼接与去重 | `scripts/moa_feed_weekly_chain_merge.py` | 不同年份周报如何合并为统一时间序列 | 精简代码片段或伪代码 |
| D | 日度转周度样本构造 | `scripts/henan_weekly_preprocessor.py` | 如何将河南日度猪价整理为周度建模目标变量 | 精简代码片段 |
| E | 缺失与节假日质量控制 | `scripts/henan_weekly_preprocessor.py` | 缺失周、边界周、节假日周如何识别并保留痕迹 | 短伪代码 |

## 三、推荐片段与说明

### 片段 A：抓取留痕与分层保存

**建议用途**

这段代码适合放在附录最前面，用来说明本研究不是只保留最终表，而是把抓取结果分为原始层、结构化层、日志层和元数据层，保证后续核查和重复运行都可实现。

**建议来源**

- `scripts/agri_weekly_crawler.py`
- 重点位置：`init_paths()`、`init_logger()`

**建议附录展示代码（精简版）**

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

**附录说明文字建议**

可在论文中配套写成：

> 为保证数据抓取过程可追溯、可复核、可重跑，本文未直接只保留最终结构化表，而是将抓取结果按原始索引、文章 HTML、正文文本、元信息、结构化中间表、运行日志与质量元数据分层保存。该设计使得后续字段校验、异常定位与重复实验具备明确的数据依据。

**这一段最想传达的点**

1. 数据抓取不是黑箱。
2. 论文中的结构化结果可以回溯到原网页文本。
3. 即使后续抽取规则调整，也可以基于原始层重跑而不依赖人工重新整理。

### 片段 B：字段提取与质量标记

**建议用途**

这段是附录里最值得放的核心方法片段之一，用来体现：研究并未机械使用 URL 日期或标题字面值，而是优先从页面正式字段和正文表述中提取 `publish_date`、`collect_date` 和价格字段，并同时生成质量标记。

**建议来源**

- `scripts/agri_weekly_crawler.py`
- 重点位置：`parse_collect_date()`、`parse_article_record()`

**建议附录展示代码（精简版）**

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

    if missing_core:
        validation_flag = "missing_core_fields"
    else:
        validation_flag = "ok"
```

**附录说明文字建议**

可在论文中配套写成：

> 在字段抽取过程中，本文优先依据页面正式发布时间和正文中的“采集日为 x 月 x 日”表述确定时间字段，并采用“表格优先、正文正则回退”的方式提取价格指标。对于核心字段缺失、单位异常、采集日缺失等情况，程序同步生成质量标记，以便后续人工复核和模型阶段识别潜在噪声。

**这一段最想传达的点**

1. 时间字段并非直接采用 URL 中的年月痕迹。
2. 字段抽取采用稳妥的“表格优先、文本回退”策略。
3. 异常不是静默忽略，而是显式记录。

### 片段 C：多来源周报链拼接与去重

**建议用途**

这段代码特别适合用来解释：为什么论文里的全国周度价格序列可以跨越多个年份，并且不是来自单一页面，而是对不同公开来源进行统一拼接后的结果。

**建议来源**

- `scripts/moa_feed_weekly_chain_merge.py`
- 重点位置：`to_key()` 和三段数据写入 `merged` 的逻辑

**建议附录展示代码（精简版）**

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

rows = sorted(merged.values(), key=lambda r: (r["publish_date"], r["article_url"]))
```

**附录说明文字建议**

可在论文中配套写成：

> 对全国周度玉米和豆粕价格数据，本文并未假设单一官方栏目能够连续覆盖全部年份，而是将公开历史周报、补充发现的过渡时期周报与市场与信息化司隐藏周报按统一字段口径拼接。拼接时以 `publish_date + week_label` 作为主键进行去重，并保留来源分段标识，以便后续追踪某一记录的具体来源。

**这一段最想传达的点**

1. 多年序列是“跨来源整合”的结果。
2. 去重键是明确的，不是人工主观删选。
3. 每条记录都能回溯到具体来源分段。

### 片段 D：日度数据转周度建模样本

**建议用途**

这一段最适合直接解释“模型输入是怎么来的”。如果论文正文要交代周度目标变量构造方法，这段几乎是必放项。

**建议来源**

- `scripts/henan_weekly_preprocessor.py`
- 重点位置：`build_target_weekly()`

**建议附录展示代码（精简版）**

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

**附录说明文字建议**

可在论文中配套写成：

> 对河南省日度猪价数据，本文以自然周为基本时间单位进行聚合，构造周度目标变量。其中，周内价格中位数作为严格版主值，同时保留均值、末次有效值、有效观测天数与缺失比例等统计量，以便兼顾稳健性和建模灵活性。

**这一段最想传达的点**

1. 周度样本不是简单取某一天，而是按周聚合。
2. 主值选择有依据，且同时保留其他统计量。
3. 缺失比例和有效天数进入了样本质量描述。

### 片段 E：缺失与节假日质量控制

**建议用途**

如果附录还能多放一小段，小枝更推荐这一段，而不是再放一段爬虫细节。因为它能体现你们对数据边界和异常样本的审慎处理。

**建议来源**

- `scripts/henan_weekly_preprocessor.py`
- 重点位置：`holiday_window_flag()` 与 `target_status / target_notes` 生成逻辑

**建议附录展示伪代码**

```text
for each week:
    if valid_days == 0:
        mark as empty_week
        add note "week_missing"
    elif valid_days < 4:
        mark as partial_week
        add note "sparse_week"

    if week overlaps Spring Festival / May Day / National Day:
        add note "holiday_window"

    if week is first or last week in sample:
        add note "boundary_week"
```

**附录说明文字建议**

可在论文中配套写成：

> 在周度样本构造后，本文未对缺失周和节假日周进行直接删除，而是以显式标记方式保留其信息。这样既能避免研究者在预处理阶段过早引入主观删选，也有利于后续模型阶段针对边界周、节假日扰动和稀疏观测周进行稳健性分析。

**这一段最想传达的点**

1. 缺失是被保留并标记的，不是被悄悄补平。
2. 节假日冲击被视为可解释的样本特征，而不是简单噪声。
3. 预处理阶段已经为稳健性检验留下接口。

## 四、附录排版建议

为了让附录看起来更像论文而不是代码仓库导出，建议这样排版：

1. 每段代码控制在 `10-25` 行。
2. 每段代码下面配 `2-4` 句说明，说明“输入是什么、规则是什么、输出是什么、为什么这样设计”。
3. 代码标题尽量写成方法标题，而不是文件名标题。  
   例如写“全国周报链拼接与去重逻辑”，不要直接写“moa_feed_weekly_chain_merge.py”。
4. 如果论文附录篇幅有限，优先保留 `B + C + D` 三段，它们对“数据可信”和“模型可复现”的支撑最强。

## 五、推荐的附录小节结构

可以在论文附录中采用如下结构：

### 附录 A.1 数据抓取与留痕保存

放片段 A，强调分层保存和日志。

### 附录 A.2 周报字段抽取与质量标记

放片段 B，强调时间字段和核心价格字段抽取。

### 附录 A.3 多来源周报数据拼接

放片段 C，强调跨年份连续序列构造。

### 附录 A.4 河南日度猪价周度化处理

放片段 D，强调模型目标变量构造。

### 附录 A.5 样本质量控制规则

放片段 E，强调缺失、节假日与边界周标记。

## 六、最终建议

如果会枝最后只想保留最少但最关键的内容，小枝推荐优先放这三段：

1. `字段提取与质量标记`
2. `多来源周报链拼接与去重`
3. `日度转周度样本构造`

这三段最能直接支撑论文里的三个核心问题：

- 数据是怎么可靠得到的
- 跨来源序列是怎么统一起来的
- 模型输入变量是怎么构造出来的

如果后续需要，小枝可以继续把这份文档再往前推一步，直接替会枝整理成“论文附录可直接粘贴版”，包括更正式的学术表述、编号、小节标题和更短的代码节选版本。
