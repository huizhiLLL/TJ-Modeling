# 农业农村部公开栏目畜牧历史周报抓取说明

## 来源说明

- 列表页根路径：`https://www.moa.gov.cn/gk/jcyj/xm/index.htm`
- 分页规则：
  - 第 1 页：`index.htm`
  - 第 2 页：`index_1.htm`
  - 最后一页：`index_24.htm`
- 当前执行页数：`25`

## 目标文章规则

从列表页中筛选标题包含 `畜产品和饲料集贸市场价格情况` 的文章。

这个来源的标题变体比新源更多，至少包含：

- `6月份第3周畜产品和饲料集贸市场价格情况`
- `4月份最后一周畜产品和饲料集贸市场价格情况`
- `6月份最后1周畜产品和饲料集贸市场价格情况`
- `2013年5月最后一周畜产品和饲料集贸市场价格情况`

因此标题匹配必须兼容：

- 可选年份前缀
- `月` / `月份`
- `第x周`
- `最后一周` / `最后1周`

## 数据结构特点

这个来源与新周报源相似，但正文结构分为两类：

### 1. 后期文章

- 页面内带表格
- 可直接用 `pandas.read_html` 解析
- 表格项目中可直接提取：
  - `活猪`
  - `玉米`
  - `豆粕`

### 2. 早期文章

- 没有表格
- 只有正文段落
- 需从文本中正则抽取：
  - `全国活猪平均价格`
  - `全国玉米平均价格`
  - `全国豆粕平均价格`

因此本来源采用：

- 表格优先
- 正文正则回退

## 字段口径

当前核心字段为：

- `live_hog_price`
- `corn_price`
- `soymeal_price`

说明：

- 该来源正文多数写 `活猪`
- 为避免和后续来源中的 `生猪` 混口径，当前结构化字段保留为 `live_hog_price`
- 若后续需要跨源合并，可在建模层再统一映射

## 输出文件

### 原始层

- 列表索引：`data/raw/moa_xm_weekly_articles/index_pages/article_index.csv`
- 正文 html：`data/raw/moa_xm_weekly_articles/article_html/`
- 正文 text：`data/raw/moa_xm_weekly_articles/article_text/`
- 文章 meta：`data/raw/moa_xm_weekly_articles/article_meta/`

### 结构化层

- 指标长表：`data/interim/moa_xm_weekly_parsed/moa_xm_weekly_indicators_long.csv`
- 核心宽表：`data/interim/moa_xm_weekly_parsed/moa_xm_weekly_prices.csv`

核心宽表字段：

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

## 页面时间字段

该来源可直接使用文章页正式时间字段：

- `meta[name=publishdate]`

因此 `publish_date` 直接取页面字段，不依赖 URL 目录日期。

## 当前全量结果

- 列表页访问：`25` 页
- 目标文章：`181` 篇
- 成功处理：`181` 篇
- 失败：`0`
- 时间覆盖：`2011-01-14` 到 `2015-06-23`
- `live_hog_price` 非空：`181`
- `corn_price` 非空：`181`
- `soymeal_price` 非空：`181`

人工复核项目前仅剩 1 篇：

- `2011-08-31`
- 原因：`collect_date_missing`

这不影响三项核心价格字段。
