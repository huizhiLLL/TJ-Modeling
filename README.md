# TJ-Modeling

当前已接入以下抓取流水线：

- 中国农业信息网监测预警栏目“畜产品和饲料集贸市场价格情况”周报
- 农业农村部生猪专题月度数据页

## 当前已实现

- 列表页索引抓取与标题筛选
- 周报文章 raw html / raw text / meta 留痕落盘
- 生猪、玉米、豆粕等核心价格字段的第一版结构化抽取
- 生猪专题页面 raw html / raw excel / meta 留痕落盘
- 生猪专题指标长表与核心宽表抽取
- 日志、摘要、人工复核清单输出

## 关键路径

- 抓取脚本：`scripts/agri_weekly_crawler.py`
- 抓取脚本：`scripts/moa_hog_monthly_crawler.py`
- 列表索引：`data/raw/agri_weekly_articles/index_pages/article_index.csv`
- 结构化结果：`data/interim/agri_weekly_parsed/agri_weekly_prices.csv`
- 抓取摘要：`data/metadata/agri_weekly_summary.json`
- 人工复核清单：`data/metadata/agri_weekly_manual_review.csv`
- 页面索引：`data/raw/moa_hog_monthly/index_pages/page_index.csv`
- 指标长表：`data/interim/moa_hog_monthly_parsed/moa_hog_indicators_long.csv`
- 核心宽表：`data/interim/moa_hog_monthly_parsed/moa_hog_core_metrics.csv`
- 抓取摘要：`data/metadata/moa_hog_monthly_summary.json`
- 人工复核清单：`data/metadata/moa_hog_monthly_manual_review.csv`