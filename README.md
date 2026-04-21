# TJ-Modeling

面向统计建模的数据仓库

优先看 `data/modeling/` 和 `data/metadata/`

## Map

- `data/modeling/henan_weekly_target_strict.csv`
  河南周度目标严格版。只做周度聚合和标准化，尽量保留真实缺口。
- `data/modeling/henan_weekly_features_strict.csv`
  河南周度严格版特征表。包含目标变量、全国周度背景变量、月度映射变量和质量标记。
- `data/modeling/henan_weekly_features_modeling.csv`
  河南周度建模版特征表。包含有限补缺和常用派生特征，适合直接进入基线建模。
- `data/metadata/henan_weekly_preprocess_data_dictionary.csv`
  字段字典。
- `data/metadata/henan_weekly_preprocess_quality_report.md`
  预处理质量报告。
- `data/metadata/figures/henan_weekly/`
  已生成的基础可视化图表。

## 说明

- 周度主价格默认使用“周内日均价中位数”作为目标值
- `target_price_median` 是严格版主值，`target_price_model` 是建模版推荐主值
- 建模版只对少量孤立缺口做了有限补值，补值痕迹保存在 `imputed_flag`、`imputation_method`、`imputation_scope`
- 全国周度变量不是全样本完整覆盖，存在时间断层，缺失属于来源边界的一部分
- 月度变量映射到周度时采用“日历月平铺”，这只是时间对齐，不代表真实周频变化

## 图表

基础图表已经生成在 `data/metadata/figures/henan_weekly/`

如果要核对图表口径，先看：

- `data/metadata/henan_weekly_visualization_checklist.md`
- `data/metadata/henan_weekly_preprocess_quality_report.md`