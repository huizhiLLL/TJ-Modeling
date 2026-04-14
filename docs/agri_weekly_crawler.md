# 中国农业信息网周报抓取说明

## 目标来源

- 栏目首页：`https://www.agri.cn/sj/jcyj/index.htm`
- 分页规则：
  - 第 1 页：`index.htm`
  - 第 2 页：`index_1.htm`
  - 第 n 页：`index_{n-1}.htm`
- 当前执行范围：第 1 页到第 50 页

## 已实现工作流

### 1. 列表页遍历

- 遍历 `index.htm` 到 `index_49.htm`
- 仅保留形如 `./202604/t20260409_8826713.htm` 的文章链接
- 过滤掉“阅读更多 >”和站点底部其他说明链接
- 用标题正则筛选周报：
  - `^\d{1,2}月第\d{1,2}周畜产品和饲料集贸市场价格情况$`

### 2. 原始层落盘

- 列表页索引：`data/raw/agri_weekly_articles/index_pages/article_index.csv`
- 正文 html：`data/raw/agri_weekly_articles/article_html/`
- 正文纯文本：`data/raw/agri_weekly_articles/article_text/`
- 文章 meta：`data/raw/agri_weekly_articles/article_meta/`

说明：

- html 优先保存正文主容器 `.content_body_box.ArticleDetails`
- text 由正文容器抽取后做空白清洗与换行标准化
- meta 中固定记录 `source_name=中国农业信息网`、`source_channel=监测预警/畜产品和饲料集贸市场价格情况`

### 3. 结构化抽取

- 优先用 `pandas.read_html` 解析正文表格
- 回退用正文正则抽取：
  - `全国生猪平均价格`
  - `全国玉米平均价格`
  - `全国豆粕平均价格`
  - 可选：`全国仔猪平均价格`、`育肥猪配合饲料平均价格`
- `week_label` 从标题抽取
- `collect_date` 优先识别“采集日为 x 月 x 日”

### 4. 校验规则

- 核心字段：`hog_price`、`corn_price`、`soymeal_price`
- 重复周处理：按 `发布年份 + week_label` 识别重复周，保留最新一条
- 摘要输出：`data/metadata/agri_weekly_summary.json`
- 人工复核清单：`data/metadata/agri_weekly_manual_review.csv`

## 当前运行结果边界

本轮按会话要求完整遍历了 50 个列表页。实际在这 50 页内可发现的目标周报共 177 篇，时间覆盖范围为：

- 起始：`2022-08-02`
- 截止：`2026-04-08`

## 后续建议

### 主方案

- 以当前 `data/raw` 和 `data/interim` 结果作为周度价格建模第一版底稿
- 对 `data/metadata/agri_weekly_manual_review.csv` 中条目做人工抽查，重点确认 `collect_date_missing` 是否允许为空