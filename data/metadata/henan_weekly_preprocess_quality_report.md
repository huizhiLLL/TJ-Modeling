# 河南周度预处理质量报告

## 1. 目标序列概况

- 周度覆盖范围：`2014-12-15` 到 `2026-04-19`
- 周度总行数：`592`
- 完整周数量：`375`
- 稀疏周数量：`173`
- 整周缺失数量：`44`
- 建模版孤立缺口补值数量：`20`

## 2. 关键特征可用性

| 字段 | 非空行数 | 总行数 | 非空率 |
|---|---:|---:|---:|
| `nat_hog_price_weekly` | 198 | 592 | 33.45% |
| `nat_corn_price_weekly` | 198 | 592 | 33.45% |
| `nat_soymeal_price_weekly` | 198 | 592 | 33.45% |
| `monthly_breeding_sow_inventory` | 222 | 592 | 37.50% |
| `monthly_hog_exfarm_price` | 256 | 592 | 43.24% |
| `monthly_hog_inventory_mom_pct` | 77 | 592 | 13.01% |

## 3. 重点说明

- 河南周度主价格默认使用周内日均价中位数。
- 全国周度背景价格优先使用 `collect_date`，没有时退回 `publish_date`。
- 月度特征以日历月平铺到周度，不代表真实周频变化。
- 建模版只对单个孤立缺口做线性补值，连续缺口保留空值。

## 4. 输出文件

- 严格版目标表：`data/modeling/henan_weekly_target_strict.csv`
- 严格版特征表：`data/modeling/henan_weekly_features_strict.csv`
- 建模版特征表：`data/modeling/henan_weekly_features_modeling.csv`
- 字段字典：`data/metadata/henan_weekly_preprocess_data_dictionary.csv`
- 描述统计表：`data/metadata/henan_weekly_descriptive_stats.csv`

## 5. 已生成图表

- `henan_weekly_target_trend.png`：河南周度价格主序列折线图
- `henan_weekly_valid_days.png`：周度有效观测天数柱状图
- `henan_weekly_missing_heatmap.png`：周度缺失热力图
- `henan_weekly_distribution.png`：周度价格分布图与箱线图
- `henan_weekly_vs_national.png`：河南与全国周度猪价对比图
- `henan_weekly_feature_scatter.png`：河南价格与核心周度特征散点图
- `henan_weekly_corr_heatmap.png`：关键变量 Spearman 相关性热力图
- `henan_weekly_rolling_diagnostics.png`：滚动均值与滚动波动率诊断图

## 6. 描述统计摘录

| 字段 | 名称 | count | missing_count | missing_rate | mean | std | min | p25 | median | p75 | max |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| target_price_model | 河南周度主价格 | 568 | 24 | 0.0405 | 17.823301 | 6.718735 | 8.74 | 13.9 | 15.51 | 18.78125 | 39.97 |
| nat_hog_price_weekly | 全国周度猪价 | 198 | 394 | 0.6655 | 16.023283 | 3.34232 | 10.4 | 14.195 | 15.165 | 17.13 | 27.66 |
| nat_corn_price_weekly | 全国周度玉米价 | 198 | 394 | 0.6655 | 2.63904 | 0.259743 | 2.27 | 2.44 | 2.525 | 2.94 | 3.07 |
| nat_soymeal_price_weekly | 全国周度豆粕价 | 198 | 394 | 0.6655 | 3.924141 | 0.640671 | 3.22 | 3.37 | 3.695 | 4.435 | 5.58 |
| monthly_breeding_sow_inventory | 月度能繁母猪存栏 | 222 | 370 | 0.625 | 4173.851351 | 139.031037 | 3961.0 | 4042.0 | 4177.0 | 4296.0 | 4459.0 |
| monthly_hog_exfarm_price | 月度全国生猪出场价 | 256 | 336 | 0.5676 | 16.614922 | 3.377257 | 11.99 | 14.545 | 15.775 | 17.88 | 27.39 |
| target_pct_change_1w | 河南周环比 | 560 | 32 | 0.0541 | 0.000721 | 0.04671 | -0.182222 | -0.023004 | -0.001221 | 0.020162 | 0.334756 |
| target_yoy_52w | 河南52周同比 | 492 | 100 | 0.1689 | 0.105929 | 0.567471 | -0.661828 | -0.258055 | -0.051122 | 0.313558 | 2.555992 |
