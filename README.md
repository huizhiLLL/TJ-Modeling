# TJ-Modeling

统计建模项目的数据准备仓库，当前已接入中国农业信息网监测预警栏目“畜产品和饲料集贸市场价格情况”周报抓取流水线。

## 当前已实现

- 列表页索引抓取与标题筛选
- 周报文章 raw html / raw text / meta 留痕落盘
- 生猪、玉米、豆粕等核心价格字段的第一版结构化抽取
- 日志、摘要、人工复核清单输出

## 关键路径

- 抓取脚本：`scripts/agri_weekly_crawler.py`
- 列表索引：`data/raw/agri_weekly_articles/index_pages/article_index.csv`
- 结构化结果：`data/interim/agri_weekly_parsed/agri_weekly_prices.csv`
- 抓取摘要：`data/metadata/agri_weekly_summary.json`
- 人工复核清单：`data/metadata/agri_weekly_manual_review.csv`

## 运行方式

建议使用项目指定解释器：

```powershell
& 'C:\Users\31691\AppData\Local\Programs\Python\Python313\python.exe' scripts\agri_weekly_crawler.py --end-page 50
```

如需先做小样本验证：

```powershell
& 'C:\Users\31691\AppData\Local\Programs\Python\Python313\python.exe' scripts\agri_weekly_crawler.py --end-page 2 --max-target-articles 5
```
