#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib import font_manager


TARGET_SOURCE_NAME = "soozhu_henan_hog_daily"
EVENT_ASF_START = pd.Timestamp("2018-08-03")


@dataclass
class PreprocessPaths:
    project_root: Path
    modeling_root: Path
    metadata_root: Path
    figures_root: Path
    target_csv: Path
    features_strict_csv: Path
    features_modeling_csv: Path
    dictionary_csv: Path
    stats_csv: Path
    quality_report_md: Path
    figure_checklist_md: Path


def init_paths(project_root: Path) -> PreprocessPaths:
    data_root = project_root / "data"
    modeling_root = data_root / "modeling"
    metadata_root = data_root / "metadata"
    figures_root = metadata_root / "figures" / "henan_weekly"

    for path in [modeling_root, metadata_root, figures_root]:
        path.mkdir(parents=True, exist_ok=True)

    return PreprocessPaths(
        project_root=project_root,
        modeling_root=modeling_root,
        metadata_root=metadata_root,
        figures_root=figures_root,
        target_csv=modeling_root / "henan_weekly_target_strict.csv",
        features_strict_csv=modeling_root / "henan_weekly_features_strict.csv",
        features_modeling_csv=modeling_root / "henan_weekly_features_modeling.csv",
        dictionary_csv=metadata_root / "henan_weekly_preprocess_data_dictionary.csv",
        stats_csv=metadata_root / "henan_weekly_descriptive_stats.csv",
        quality_report_md=metadata_root / "henan_weekly_preprocess_quality_report.md",
        figure_checklist_md=metadata_root / "henan_weekly_visualization_checklist.md",
    )


def init_logger() -> logging.Logger:
    logger = logging.getLogger("henan_weekly_preprocessor")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(handler)
    return logger


def rel_path(path: Path, project_root: Path) -> str:
    return path.relative_to(project_root).as_posix()


def pick_chinese_font() -> list[str]:
    preferred = [
        "Noto Serif SC",
        "STSong",
        "Songti SC",
        "Microsoft YaHei",
        "Noto Sans CJK SC",
        "Source Han Sans SC",
        "SimSun",
        "FangSong",
        "KaiTi",
        "SimHei",
        "Arial Unicode MS",
    ]
    available = {font.name for font in font_manager.fontManager.ttflist}
    chosen = [name for name in preferred if name in available]
    return chosen if chosen else ["DejaVu Sans"]


def configure_plot_style() -> None:
    fonts = pick_chinese_font()
    matplotlib.rcParams["font.family"] = "serif"
    matplotlib.rcParams["font.serif"] = fonts + ["DejaVu Serif"]
    matplotlib.rcParams["font.sans-serif"] = fonts + ["DejaVu Sans"]
    matplotlib.rcParams["axes.unicode_minus"] = False
    matplotlib.rcParams["figure.dpi"] = 140
    matplotlib.rcParams["savefig.dpi"] = 240
    matplotlib.rcParams["axes.titleweight"] = "semibold"
    matplotlib.rcParams["axes.labelweight"] = "regular"
    matplotlib.rcParams["axes.titlesize"] = 24
    matplotlib.rcParams["axes.labelsize"] = 20
    matplotlib.rcParams["legend.fontsize"] = 16
    matplotlib.rcParams["xtick.labelsize"] = 14
    matplotlib.rcParams["ytick.labelsize"] = 14
    sns.set_theme(
        style="whitegrid",
        context="talk",
        rc={
            "font.family": "serif",
            "font.serif": fonts + ["DejaVu Serif"],
            "font.sans-serif": fonts + ["DejaVu Sans"],
        },
    )


def load_csv(project_root: Path, relative_path: str, **kwargs: Any) -> pd.DataFrame:
    return pd.read_csv(project_root / relative_path, **kwargs)


def get_week_start(series: pd.Series) -> pd.Series:
    return series.dt.to_period("W-SUN").dt.start_time


def get_week_end(week_start: pd.Series) -> pd.Series:
    return week_start + pd.Timedelta(days=6)


def last_valid(series: pd.Series) -> float | None:
    valid = series.dropna()
    if valid.empty:
        return None
    return float(valid.iloc[-1])


def holiday_window_flag(week_start: pd.Timestamp) -> bool:
    if pd.isna(week_start):
        return False
    week_dates = pd.date_range(week_start, week_start + pd.Timedelta(days=6), freq="D")
    for date in week_dates:
        month_day = (date.month, date.day)
        if (1, 20) <= month_day <= (2, 20):
            return True
        if (5, 1) <= month_day <= (5, 5):
            return True
        if (10, 1) <= month_day <= (10, 7):
            return True
    return False


def build_target_weekly(project_root: Path) -> pd.DataFrame:
    daily = load_csv(
        project_root,
        "data/interim/soozhu_henan_hog_daily_parsed/soozhu_henan_hog_daily_prices.csv",
        parse_dates=["publish_datetime"],
    )
    daily = daily.sort_values("publish_datetime").copy()
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
            target_price_min=("avg_price", "min"),
            target_price_max=("avg_price", "max"),
            target_last_year_price_mean=("last_year_price", "mean"),
            target_mom_pct_mean=("mom_pct", "mean"),
        )
        .sort_index()
    )

    full_index = pd.date_range(weekly.index.min(), weekly.index.max(), freq="W-MON")
    weekly = weekly.reindex(full_index)
    weekly.index.name = "week_start"
    weekly = weekly.reset_index()
    weekly["week_end"] = get_week_end(weekly["week_start"])
    weekly["obs_days"] = weekly["obs_days"].fillna(0).astype(int)
    weekly["valid_days"] = weekly["valid_days"].fillna(0).astype(int)
    weekly["missing_ratio"] = ((7 - weekly["valid_days"]) / 7).round(4)
    weekly["target_source"] = TARGET_SOURCE_NAME
    weekly["is_source_gap"] = weekly["valid_days"].eq(0)
    weekly["is_boundary_week"] = False
    weekly.loc[[0, len(weekly) - 1], "is_boundary_week"] = True
    weekly["is_holiday_gap"] = weekly["week_start"].map(holiday_window_flag)

    status = np.select(
        [
            weekly["valid_days"].ge(4),
            weekly["valid_days"].between(1, 3),
            weekly["valid_days"].eq(1),
            weekly["valid_days"].eq(0),
        ],
        ["complete_week", "partial_week", "single_point_week", "empty_week"],
        default="unknown",
    )
    weekly["target_status"] = status

    notes: list[str] = []
    for _, row in weekly.iterrows():
        row_notes: list[str] = []
        if row["valid_days"] == 0:
            row_notes.append("week_missing")
        elif row["valid_days"] < 4:
            row_notes.append("sparse_week")
        if row["is_holiday_gap"]:
            row_notes.append("holiday_window")
        if row["is_boundary_week"]:
            row_notes.append("boundary_week")
        notes.append("; ".join(row_notes))
    weekly["target_notes"] = notes

    column_order = [
        "week_start",
        "week_end",
        "target_price_median",
        "target_price_mean",
        "target_price_last",
        "target_price_min",
        "target_price_max",
        "target_last_year_price_mean",
        "target_mom_pct_mean",
        "obs_days",
        "valid_days",
        "missing_ratio",
        "target_source",
        "target_status",
        "is_source_gap",
        "is_holiday_gap",
        "is_boundary_week",
        "target_notes",
    ]
    return weekly[column_order]


def standardize_nat_weekly(project_root: Path) -> pd.DataFrame:
    agri = load_csv(
        project_root,
        "data/interim/agri_weekly_parsed/agri_weekly_prices.csv",
        parse_dates=["publish_date", "collect_date"],
    )
    agri = agri.assign(
        reference_date=agri["collect_date"].fillna(agri["publish_date"]),
        nat_hog_price_weekly=agri["hog_price"],
        nat_corn_price_weekly=agri["corn_price"],
        nat_soymeal_price_weekly=agri["soymeal_price"],
        nat_piglet_price_weekly=agri["piglet_price"],
        nat_feed_price_weekly=agri["mixed_feed_price"],
        nat_price_source="agri_weekly",
        nat_price_date_role=np.where(agri["collect_date"].notna(), "collect_date", "publish_date"),
    )
    agri["week_start"] = get_week_start(agri["reference_date"])

    moa = load_csv(
        project_root,
        "data/interim/moa_xm_weekly_parsed/moa_xm_weekly_prices.csv",
        parse_dates=["publish_date", "collect_date"],
    )
    moa = moa.assign(
        reference_date=moa["collect_date"].fillna(moa["publish_date"]),
        nat_hog_price_weekly=moa["live_hog_price"],
        nat_corn_price_weekly=moa["corn_price"],
        nat_soymeal_price_weekly=moa["soymeal_price"],
        nat_piglet_price_weekly=np.nan,
        nat_feed_price_weekly=np.nan,
        nat_price_source="moa_xm_weekly",
        nat_price_date_role=np.where(moa["collect_date"].notna(), "collect_date", "publish_date"),
    )
    moa["week_start"] = get_week_start(moa["reference_date"])

    hog_keep_cols = [
        "week_start",
        "reference_date",
        "nat_hog_price_weekly",
        "nat_piglet_price_weekly",
        "nat_feed_price_weekly",
        "nat_price_source",
        "nat_price_date_role",
    ]
    hog_nat = pd.concat([moa[hog_keep_cols], agri[hog_keep_cols]], ignore_index=True)
    hog_nat["source_priority"] = hog_nat["nat_price_source"].map({"agri_weekly": 1, "moa_xm_weekly": 2}).fillna(9)
    hog_nat = hog_nat.sort_values(["week_start", "source_priority", "reference_date"])
    hog_nat = hog_nat.drop_duplicates("week_start", keep="first").drop(columns="source_priority")
    hog_nat = hog_nat.rename(columns={"reference_date": "nat_price_reference_date"})

    feed_chain = load_csv(
        project_root,
        "data/interim/moa_feed_weekly_chain_parsed/moa_feed_weekly_chain_prices.csv",
        parse_dates=["publish_date", "collect_date"],
    )
    feed_chain = feed_chain.assign(
        reference_date=feed_chain["collect_date"].fillna(feed_chain["publish_date"]),
        nat_corn_price_weekly=feed_chain["corn_price"],
        nat_soymeal_price_weekly=feed_chain["soymeal_price"],
        nat_corn_soymeal_source=feed_chain["source_segment"].fillna("moa_feed_weekly_chain"),
        nat_corn_soymeal_date_role=np.where(
            feed_chain["collect_date"].notna(),
            "collect_date",
            "publish_date",
        ),
    )
    feed_chain["week_start"] = get_week_start(feed_chain["reference_date"])

    recent_feed = agri.assign(
        nat_corn_soymeal_source="agri_weekly",
        nat_corn_soymeal_date_role=np.where(
            agri["collect_date"].notna(),
            "collect_date",
            "publish_date",
        ),
    )
    feed_keep_cols = [
        "week_start",
        "reference_date",
        "nat_corn_price_weekly",
        "nat_soymeal_price_weekly",
        "nat_corn_soymeal_source",
        "nat_corn_soymeal_date_role",
    ]
    feed_nat = pd.concat([feed_chain[feed_keep_cols], recent_feed[feed_keep_cols]], ignore_index=True)
    feed_nat["source_priority"] = feed_nat["nat_corn_soymeal_source"].map({"agri_weekly": 1}).fillna(2)
    feed_nat = feed_nat.sort_values(["week_start", "source_priority", "reference_date"])
    feed_nat = feed_nat.drop_duplicates("week_start", keep="first").drop(columns="source_priority")
    feed_nat = feed_nat.rename(columns={"reference_date": "nat_corn_soymeal_reference_date"})

    nat = hog_nat.merge(feed_nat, on="week_start", how="outer")
    return nat


def build_monthly_core(project_root: Path) -> pd.DataFrame:
    month = load_csv(
        project_root,
        "data/interim/moa_hog_monthly_parsed/moa_hog_core_metrics.csv",
    )
    month = month.loc[month["period_type"].eq("month")].copy()
    month["month_key"] = month["report_period"].astype(str)
    month = month.rename(
        columns={
            "breeding_sow_inventory": "monthly_breeding_sow_inventory",
            "breeding_sow_inventory_mom": "monthly_breeding_sow_inventory_mom",
            "breeding_sow_inventory_yoy": "monthly_breeding_sow_inventory_yoy",
            "hog_exfarm_price": "monthly_hog_exfarm_price",
            "hog_exfarm_price_mom": "monthly_hog_exfarm_price_mom",
            "hog_exfarm_price_yoy": "monthly_hog_exfarm_price_yoy",
            "publish_date": "monthly_report_publish_date",
            "validation_flag": "monthly_core_validation_flag",
            "validation_notes": "monthly_core_validation_notes",
        }
    )
    keep_cols = [
        "month_key",
        "monthly_breeding_sow_inventory",
        "monthly_breeding_sow_inventory_mom",
        "monthly_breeding_sow_inventory_yoy",
        "monthly_hog_exfarm_price",
        "monthly_hog_exfarm_price_mom",
        "monthly_hog_exfarm_price_yoy",
        "monthly_report_publish_date",
        "monthly_core_validation_flag",
        "monthly_core_validation_notes",
    ]
    return month[keep_cols].drop_duplicates("month_key")


def build_scs_monthly(project_root: Path) -> pd.DataFrame:
    scs = load_csv(
        project_root,
        "data/interim/scs_hog_inventory_parsed/scs_hog_inventory_changes.csv",
    )
    scs["month_key"] = scs["report_period"].astype(str)
    value_cols = [
        "hog_inventory_mom_pct",
        "hog_inventory_yoy_pct",
        "breeding_sow_inventory_mom_pct",
        "breeding_sow_inventory_yoy_pct",
    ]
    scs["non_null_score"] = scs[value_cols].notna().sum(axis=1)
    scs["ok_flag"] = scs["validation_flag"].eq("ok").astype(int)
    scs = scs.sort_values(["month_key", "ok_flag", "non_null_score"], ascending=[True, False, False])
    scs = scs.drop_duplicates("month_key", keep="first")
    scs = scs.rename(
        columns={
            "hog_inventory_mom_pct": "monthly_hog_inventory_mom_pct",
            "hog_inventory_yoy_pct": "monthly_hog_inventory_yoy_pct",
            "breeding_sow_inventory_mom_pct": "monthly_scs_breeding_sow_inventory_mom_pct",
            "breeding_sow_inventory_yoy_pct": "monthly_scs_breeding_sow_inventory_yoy_pct",
            "validation_flag": "monthly_scs_validation_flag",
            "validation_notes": "monthly_scs_validation_notes",
        }
    )
    keep_cols = [
        "month_key",
        "monthly_hog_inventory_mom_pct",
        "monthly_hog_inventory_yoy_pct",
        "monthly_scs_breeding_sow_inventory_mom_pct",
        "monthly_scs_breeding_sow_inventory_yoy_pct",
        "monthly_scs_validation_flag",
        "monthly_scs_validation_notes",
    ]
    return scs[keep_cols]


def build_features_strict(project_root: Path, target_weekly: pd.DataFrame) -> pd.DataFrame:
    features = target_weekly.copy()
    features["month_key"] = features["week_start"].dt.to_period("M").astype(str)
    features["month_to_week_method"] = "calendar_month_flat_fill"
    features["is_monthly_carried"] = True

    nat = standardize_nat_weekly(project_root)
    monthly_core = build_monthly_core(project_root)
    monthly_scs = build_scs_monthly(project_root)

    features = features.merge(nat, on="week_start", how="left")
    features = features.merge(monthly_core, on="month_key", how="left")
    features = features.merge(monthly_scs, on="month_key", how="left")

    features["nat_feature_available"] = (
        features["nat_hog_price_weekly"].notna()
        | features["nat_corn_price_weekly"].notna()
        | features["nat_soymeal_price_weekly"].notna()
    )
    features["monthly_feature_available"] = (
        features["monthly_breeding_sow_inventory"].notna()
        | features["monthly_hog_exfarm_price"].notna()
        | features["monthly_hog_inventory_mom_pct"].notna()
    )
    return features


def build_features_modeling(features_strict: pd.DataFrame) -> pd.DataFrame:
    model = features_strict.copy()
    raw_target = model["target_price_median"].copy()

    isolated_gap_mask = (
        raw_target.isna()
        & raw_target.shift(1).notna()
        & raw_target.shift(-1).notna()
    )
    model["target_price_model"] = raw_target
    model.loc[isolated_gap_mask, "target_price_model"] = (
        raw_target.shift(1) + raw_target.shift(-1)
    ) / 2

    model["imputed_flag"] = isolated_gap_mask
    model["imputation_method"] = np.where(
        isolated_gap_mask,
        "linear_between_adjacent_weeks",
        "",
    )
    model["imputation_scope"] = np.where(
        isolated_gap_mask,
        "target_price_model",
        "",
    )

    target_series = model["target_price_model"]
    model["target_log_price"] = np.log(target_series.where(target_series > 0))
    model["target_pct_change_1w"] = target_series.pct_change(1)
    model["target_pct_change_4w"] = target_series.pct_change(4)
    model["target_yoy_52w"] = target_series.pct_change(52)
    model["target_ma_4w"] = target_series.rolling(4, min_periods=2).mean()
    model["target_ma_12w"] = target_series.rolling(12, min_periods=4).mean()
    model["target_volatility_4w"] = model["target_pct_change_1w"].rolling(4, min_periods=2).std()
    model["target_volatility_12w"] = model["target_pct_change_1w"].rolling(12, min_periods=4).std()
    model["spread_vs_nat_hog"] = target_series - model["nat_hog_price_weekly"]
    model["ratio_vs_corn"] = target_series / model["nat_corn_price_weekly"]
    model["ratio_vs_soymeal"] = target_series / model["nat_soymeal_price_weekly"]
    model["event_asf"] = model["week_start"].ge(EVENT_ASF_START).astype(int)
    model["month_no"] = model["week_start"].dt.month
    model["week_of_year"] = model["week_start"].dt.isocalendar().week.astype(int)

    return model


def build_descriptive_stats(features_modeling: pd.DataFrame) -> pd.DataFrame:
    focus_cols = {
        "target_price_model": "河南周度主价格",
        "nat_hog_price_weekly": "全国周度猪价",
        "nat_corn_price_weekly": "全国周度玉米价",
        "nat_soymeal_price_weekly": "全国周度豆粕价",
        "monthly_breeding_sow_inventory": "月度能繁母猪存栏",
        "monthly_hog_exfarm_price": "月度全国生猪出场价",
        "target_pct_change_1w": "河南周环比",
        "target_yoy_52w": "河南52周同比",
    }
    rows: list[dict[str, Any]] = []
    for col, label in focus_cols.items():
        series = pd.to_numeric(features_modeling[col], errors="coerce")
        rows.append(
            {
                "column_name": col,
                "display_name": label,
                "count": int(series.notna().sum()),
                "missing_count": int(series.isna().sum()),
                "missing_rate": round(float(series.isna().mean()), 4),
                "mean": round(float(series.mean()), 6) if series.notna().any() else np.nan,
                "std": round(float(series.std()), 6) if series.notna().sum() > 1 else np.nan,
                "min": round(float(series.min()), 6) if series.notna().any() else np.nan,
                "p25": round(float(series.quantile(0.25)), 6) if series.notna().any() else np.nan,
                "median": round(float(series.median()), 6) if series.notna().any() else np.nan,
                "p75": round(float(series.quantile(0.75)), 6) if series.notna().any() else np.nan,
                "max": round(float(series.max()), 6) if series.notna().any() else np.nan,
            }
        )
    return pd.DataFrame(rows)


def build_data_dictionary(
    target_strict: pd.DataFrame,
    features_strict: pd.DataFrame,
    features_modeling: pd.DataFrame,
) -> pd.DataFrame:
    desc_map = {
        "week_start": "自然周起始日期（周一）",
        "week_end": "自然周结束日期（周日）",
        "target_price_median": "河南周度主价格，周内日均价中位数",
        "target_price_mean": "河南周度主价格，周内日均价均值",
        "target_price_last": "河南周度主价格，周内最后一个有效观测",
        "target_price_min": "河南周内最低有效价格",
        "target_price_max": "河南周内最高有效价格",
        "target_last_year_price_mean": "源四页面给出的去年同期价格周均值",
        "target_mom_pct_mean": "源四页面给出的周内环比均值",
        "obs_days": "该周抓到的原始文章天数",
        "valid_days": "该周有有效主价格的天数",
        "missing_ratio": "一周七天中目标变量缺失占比",
        "target_source": "目标变量来源标识",
        "target_status": "目标变量周级质量状态",
        "is_source_gap": "该周目标变量是否整周缺失",
        "is_holiday_gap": "该周是否位于春节/五一/国庆窗口",
        "is_boundary_week": "是否为序列边界周",
        "target_notes": "目标变量周级诊断说明",
        "month_key": "周所属月份，格式 YYYY-MM",
        "month_to_week_method": "月度特征映射到周度的方法",
        "is_monthly_carried": "是否由月度值平铺到周度",
        "nat_hog_price_weekly": "全国周度猪价背景变量",
        "nat_corn_price_weekly": "全国周度玉米价格背景变量",
        "nat_soymeal_price_weekly": "全国周度豆粕价格背景变量",
        "nat_piglet_price_weekly": "全国周度仔猪价格背景变量",
        "nat_feed_price_weekly": "全国周度配合饲料价格背景变量",
        "nat_price_source": "全国周度价格来源",
        "nat_price_date_role": "全国周度价格对齐时使用的日期字段",
        "nat_price_reference_date": "全国周度价格参考日期",
        "nat_corn_soymeal_source": "全国周度玉米与豆粕价格来源",
        "nat_corn_soymeal_date_role": "全国周度玉米与豆粕价格对齐时使用的日期字段",
        "nat_corn_soymeal_reference_date": "全国周度玉米与豆粕价格参考日期",
        "monthly_breeding_sow_inventory": "月度能繁母猪存栏绝对量",
        "monthly_breeding_sow_inventory_mom": "月度能繁母猪存栏环比",
        "monthly_breeding_sow_inventory_yoy": "月度能繁母猪存栏同比",
        "monthly_hog_exfarm_price": "月度全国生猪出场价",
        "monthly_hog_exfarm_price_mom": "月度全国生猪出场价环比",
        "monthly_hog_exfarm_price_yoy": "月度全国生猪出场价同比",
        "monthly_report_publish_date": "月度专题页代理发布日期",
        "monthly_core_validation_flag": "月度专题数据质量标记",
        "monthly_core_validation_notes": "月度专题数据质量说明",
        "monthly_hog_inventory_mom_pct": "市场与信息化司月度生猪存栏环比",
        "monthly_hog_inventory_yoy_pct": "市场与信息化司月度生猪存栏同比",
        "monthly_scs_breeding_sow_inventory_mom_pct": "市场与信息化司月度能繁母猪存栏环比",
        "monthly_scs_breeding_sow_inventory_yoy_pct": "市场与信息化司月度能繁母猪存栏同比",
        "monthly_scs_validation_flag": "市场与信息化司数据质量标记",
        "monthly_scs_validation_notes": "市场与信息化司数据质量说明",
        "nat_feature_available": "该周是否成功并入全国周度特征",
        "monthly_feature_available": "该周是否成功并入月度特征",
        "target_price_model": "建模版推荐目标值，仅对孤立缺口做线性填补",
        "imputed_flag": "该周建模版是否发生补值",
        "imputation_method": "补值方法说明",
        "imputation_scope": "补值字段范围",
        "target_log_price": "目标价格对数值",
        "target_pct_change_1w": "目标价格1周环比",
        "target_pct_change_4w": "目标价格4周环比",
        "target_yoy_52w": "目标价格52周同比",
        "target_ma_4w": "目标价格4周移动均值",
        "target_ma_12w": "目标价格12周移动均值",
        "target_volatility_4w": "目标价格1周环比的4周滚动波动率",
        "target_volatility_12w": "目标价格1周环比的12周滚动波动率",
        "spread_vs_nat_hog": "河南价格与全国周度猪价价差",
        "ratio_vs_corn": "河南价格与全国周度玉米价格比值",
        "ratio_vs_soymeal": "河南价格与全国周度豆粕价格比值",
        "event_asf": "非洲猪瘟事件后虚拟变量",
        "month_no": "月份序号",
        "week_of_year": "ISO周序号",
    }

    table_map = {
        "henan_weekly_target_strict": target_strict,
        "henan_weekly_features_strict": features_strict,
        "henan_weekly_features_modeling": features_modeling,
    }

    rows: list[dict[str, Any]] = []
    for table_name, df in table_map.items():
        for column in df.columns:
            rows.append(
                {
                    "table_name": table_name,
                    "column_name": column,
                    "dtype": str(df[column].dtype),
                    "description": desc_map.get(column, ""),
                }
            )
    return pd.DataFrame(rows)


def save_figure(path: Path) -> None:
    plt.tight_layout()
    plt.savefig(path, dpi=240, bbox_inches="tight", facecolor="white")
    plt.close()


def plot_target_trend(df: pd.DataFrame, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(15, 6))
    ax.plot(df["week_start"], df["target_price_model"], color="#c23b22", linewidth=2.2, label="河南周度价格")
    ax.plot(df["week_start"], df["target_ma_4w"], color="#2a6f97", linewidth=1.8, alpha=0.9, label="4周移动均值")
    missing = df.loc[df["target_price_median"].isna(), ["week_start"]]
    if not missing.empty:
        ax.scatter(
            missing["week_start"],
            np.full(len(missing), df["target_price_model"].min(skipna=True) * 0.92),
            color="#8b1e3f",
            s=36,
            label="原始缺失周",
            zorder=5,
        )
    ax.axvline(EVENT_ASF_START, color="#6a4c93", linestyle="--", linewidth=1.4, label="ASF事件")
    ax.set_title("河南周度生猪价格主序列")
    ax.set_xlabel("周起始日期")
    ax.set_ylabel("价格（元/公斤）")
    ax.legend(frameon=True, ncol=3)
    save_figure(output_path)


def plot_valid_days(df: pd.DataFrame, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(15, 4.8))
    colors = np.where(df["valid_days"].eq(0), "#b23a48", np.where(df["valid_days"].lt(4), "#f4a261", "#2a9d8f"))
    ax.plot(df["week_start"], df["valid_days"], color="#5c677d", linewidth=1.3, alpha=0.85, zorder=1)
    ax.scatter(df["week_start"], df["valid_days"], color=colors, s=18, alpha=0.95, zorder=2)
    ax.axhline(4, color="#264653", linestyle="--", linewidth=1.2, label="完整周阈值（4天）")
    ax.set_title("河南周度有效观测天数")
    ax.set_xlabel("周起始日期")
    ax.set_ylabel("有效天数")
    ax.set_ylim(0, 7.3)
    ax.legend(frameon=True)
    save_figure(output_path)


def plot_missing_weeks(df: pd.DataFrame, output_path: Path) -> None:
    plot_df = df.copy()
    plot_df["missing_flag"] = plot_df["target_price_median"].isna().astype(int)
    plot_df["year"] = plot_df["week_start"].dt.year
    plot_df["week_of_year"] = plot_df["week_start"].dt.isocalendar().week.astype(int)
    pivot = plot_df.pivot_table(index="year", columns="week_of_year", values="missing_flag", aggfunc="max")
    fig, ax = plt.subplots(figsize=(16, 4.8))
    sns.heatmap(pivot, cmap=["#e9ecef", "#d62828"], cbar=False, linewidths=0.2, linecolor="white", ax=ax)
    ax.set_title("河南周度目标变量缺失热力图")
    ax.set_xlabel("年内周序号")
    ax.set_ylabel("年份")
    save_figure(output_path)


def plot_distribution(df: pd.DataFrame, output_path: Path) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(13.5, 4.8))
    sns.histplot(df["target_price_model"].dropna(), bins=28, kde=True, color="#457b9d", ax=axes[0])
    axes[0].set_title("河南周度价格分布")
    axes[0].set_xlabel("价格（元/公斤）")
    sns.boxplot(x=df["target_price_model"], color="#e76f51", ax=axes[1], width=0.35)
    axes[1].set_title("河南周度价格箱线图")
    axes[1].set_xlabel("价格（元/公斤）")
    save_figure(output_path)


def plot_vs_national(df: pd.DataFrame, output_path: Path) -> None:
    full_df = df.sort_values("week_start").copy()
    subset = full_df[full_df["nat_hog_price_weekly"].notna()].copy()
    gap_days = subset["week_start"].diff().dt.days.fillna(7)
    subset["segment_id"] = gap_days.gt(21).cumsum()
    fig, ax = plt.subplots(figsize=(15, 6))
    ax.plot(
        full_df["week_start"],
        full_df["target_price_model"],
        color="#d62828",
        linewidth=2.2,
        label="河南周度价格",
        zorder=3,
    )

    missing_nat = full_df["nat_hog_price_weekly"].isna()
    if missing_nat.any():
        gap_start = None
        for idx, row in full_df.iterrows():
            if missing_nat.loc[idx] and gap_start is None:
                gap_start = row["week_start"]
            if gap_start is not None and (not missing_nat.loc[idx]):
                gap_end = row["week_start"]
                gap_days = (gap_end - gap_start).days
                if gap_days >= 60:
                    ax.axvspan(gap_start, gap_end, color="#adb5bd", alpha=0.12)
                if gap_days >= 365:
                    gap_mid = gap_start + (gap_end - gap_start) / 2
                    ax.text(
                        gap_mid,
                        full_df["target_price_model"].max(skipna=True) * 0.94,
                        "全国周度数据长缺口",
                        ha="center",
                        va="center",
                        color="#5c677d",
                        fontsize=14,
                    )
                gap_start = None
        if gap_start is not None:
            gap_end = full_df["week_start"].iloc[-1]
            gap_days = (gap_end - gap_start).days
            if gap_days >= 60:
                ax.axvspan(gap_start, gap_end, color="#adb5bd", alpha=0.12)

    first_segment = True
    for _, block in subset.groupby("segment_id"):
        ax.plot(
            block["week_start"],
            block["nat_hog_price_weekly"],
            color="#1d3557",
            linewidth=2.0,
            label="全国周度猪价" if first_segment else None,
            zorder=4,
        )
        ax.fill_between(
            block["week_start"],
            block["target_price_model"],
            block["nat_hog_price_weekly"],
            color="#fcbf49",
            alpha=0.14,
            zorder=2,
        )
        first_segment = False
    ax.set_title("河南周度价格与全国周度猪价对比")
    ax.set_xlabel("周起始日期")
    ax.set_ylabel("价格（元/公斤）")
    ax.legend(frameon=True)
    save_figure(output_path)


def plot_feature_scatter(df: pd.DataFrame, output_path: Path) -> None:
    fig, axes = plt.subplots(1, 3, figsize=(16, 4.8))
    pairs = [
        ("nat_hog_price_weekly", "全国周度猪价"),
        ("nat_corn_price_weekly", "全国周度玉米价"),
        ("nat_soymeal_price_weekly", "全国周度豆粕价"),
    ]
    colors = ["#9d0208", "#2a9d8f", "#577590"]
    for ax, (col, label), color in zip(axes, pairs, colors):
        subset = df[[col, "target_price_model"]].dropna()
        sns.regplot(
            data=subset,
            x=col,
            y="target_price_model",
            scatter_kws={"alpha": 0.65, "s": 24, "color": color},
            line_kws={"color": "#1d3557", "linewidth": 1.4},
            ax=ax,
        )
        ax.set_title(f"河南价格 vs {label}")
        ax.set_xlabel(label)
        ax.set_ylabel("河南周度价格")
    save_figure(output_path)


def plot_corr_heatmap(df: pd.DataFrame, output_path: Path) -> None:
    corr_cols = [
        "target_price_model",
        "nat_hog_price_weekly",
        "nat_corn_price_weekly",
        "nat_soymeal_price_weekly",
        "monthly_breeding_sow_inventory",
        "monthly_hog_exfarm_price",
        "target_pct_change_1w",
    ]
    corr_df = df[corr_cols].copy()
    corr = corr_df.corr(method="spearman")
    rename = {
        "target_price_model": "河南周度价格",
        "nat_hog_price_weekly": "全国周度猪价",
        "nat_corn_price_weekly": "全国周度玉米价",
        "nat_soymeal_price_weekly": "全国周度豆粕价",
        "monthly_breeding_sow_inventory": "能繁母猪存栏",
        "monthly_hog_exfarm_price": "全国生猪出场价",
        "target_pct_change_1w": "河南周环比",
    }
    corr = corr.rename(index=rename, columns=rename)
    fig, ax = plt.subplots(figsize=(9, 7))
    sns.heatmap(corr, cmap="RdBu_r", center=0, annot=True, fmt=".2f", linewidths=0.5, ax=ax)
    ax.set_title("关键变量 Spearman 相关性热力图")
    save_figure(output_path)


def plot_rolling_diagnostics(df: pd.DataFrame, output_path: Path) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(15, 8), sharex=True)
    axes[0].plot(df["week_start"], df["target_price_model"], color="#bc4749", linewidth=1.8, label="主价格")
    axes[0].plot(df["week_start"], df["target_ma_12w"], color="#386641", linewidth=2.1, label="12周移动均值")
    axes[0].set_title("河南周度价格与12周移动均值")
    axes[0].set_ylabel("价格（元/公斤）")
    axes[0].legend(frameon=True)

    axes[1].plot(df["week_start"], df["target_volatility_4w"], color="#6a4c93", linewidth=1.6, label="4周滚动波动率")
    axes[1].plot(df["week_start"], df["target_volatility_12w"], color="#1982c4", linewidth=1.6, label="12周滚动波动率")
    axes[1].set_title("河南周度价格滚动波动率")
    axes[1].set_xlabel("周起始日期")
    axes[1].set_ylabel("波动率")
    axes[1].legend(frameon=True)
    save_figure(output_path)


def create_figures(features_modeling: pd.DataFrame, paths: PreprocessPaths) -> list[tuple[str, str]]:
    figure_specs = [
        ("henan_weekly_target_trend.png", "河南周度价格主序列折线图", plot_target_trend),
        ("henan_weekly_valid_days.png", "周度有效观测天数柱状图", plot_valid_days),
        ("henan_weekly_missing_heatmap.png", "周度缺失热力图", plot_missing_weeks),
        ("henan_weekly_distribution.png", "周度价格分布图与箱线图", plot_distribution),
        ("henan_weekly_vs_national.png", "河南与全国周度猪价对比图", plot_vs_national),
        ("henan_weekly_feature_scatter.png", "河南价格与核心周度特征散点图", plot_feature_scatter),
        ("henan_weekly_corr_heatmap.png", "关键变量 Spearman 相关性热力图", plot_corr_heatmap),
        ("henan_weekly_rolling_diagnostics.png", "滚动均值与滚动波动率诊断图", plot_rolling_diagnostics),
    ]
    outputs: list[tuple[str, str]] = []
    for filename, description, func in figure_specs:
        output_path = paths.figures_root / filename
        func(features_modeling, output_path)
        outputs.append((filename, description))
    return outputs


def write_quality_report(
    target_strict: pd.DataFrame,
    features_strict: pd.DataFrame,
    features_modeling: pd.DataFrame,
    stats_df: pd.DataFrame,
    figure_records: list[tuple[str, str]],
    paths: PreprocessPaths,
) -> None:
    target_counts = target_strict["target_status"].value_counts().to_dict()
    important_cols = [
        "nat_hog_price_weekly",
        "nat_corn_price_weekly",
        "nat_soymeal_price_weekly",
        "monthly_breeding_sow_inventory",
        "monthly_hog_exfarm_price",
        "monthly_hog_inventory_mom_pct",
    ]
    feature_lines = []
    for col in important_cols:
        non_null = int(features_strict[col].notna().sum())
        total = len(features_strict)
        feature_lines.append(f"| `{col}` | {non_null} | {total} | {non_null / total:.2%} |")

    stats_table = ["| 字段 | 名称 | count | missing_count | missing_rate | mean | std | min | p25 | median | p75 | max |", "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for row in stats_df.to_dict("records"):
        stats_table.append(
            "| {column_name} | {display_name} | {count} | {missing_count} | {missing_rate} | {mean} | {std} | {min} | {p25} | {median} | {p75} | {max} |".format(
                **row
            )
        )

    lines = [
        "# 河南周度预处理质量报告",
        "",
        "## 1. 目标序列概况",
        "",
        f"- 周度覆盖范围：`{target_strict['week_start'].min().date()}` 到 `{target_strict['week_end'].max().date()}`",
        f"- 周度总行数：`{len(target_strict)}`",
        f"- 完整周数量：`{target_counts.get('complete_week', 0)}`",
        f"- 稀疏周数量：`{target_counts.get('partial_week', 0) + target_counts.get('single_point_week', 0)}`",
        f"- 整周缺失数量：`{target_counts.get('empty_week', 0)}`",
        f"- 建模版孤立缺口补值数量：`{int(features_modeling['imputed_flag'].sum())}`",
        "",
        "## 2. 关键特征可用性",
        "",
        "| 字段 | 非空行数 | 总行数 | 非空率 |",
        "|---|---:|---:|---:|",
        *feature_lines,
        "",
        "## 3. 重点说明",
        "",
        "- 河南周度主价格默认使用周内日均价中位数。",
        "- 全国周度背景价格优先使用 `collect_date`，没有时退回 `publish_date`。",
        "- 月度特征以日历月平铺到周度，不代表真实周频变化。",
        "- 建模版只对单个孤立缺口做线性补值，连续缺口保留空值。",
        "",
        "## 4. 输出文件",
        "",
        f"- 严格版目标表：`{rel_path(paths.target_csv, paths.project_root)}`",
        f"- 严格版特征表：`{rel_path(paths.features_strict_csv, paths.project_root)}`",
        f"- 建模版特征表：`{rel_path(paths.features_modeling_csv, paths.project_root)}`",
        f"- 字段字典：`{rel_path(paths.dictionary_csv, paths.project_root)}`",
        f"- 描述统计表：`{rel_path(paths.stats_csv, paths.project_root)}`",
        "",
        "## 5. 已生成图表",
        "",
    ]
    lines.extend([f"- `{filename}`：{description}" for filename, description in figure_records])
    lines.extend(
        [
            "",
        "## 6. 描述统计摘录",
        "",
        *stats_table,
        "",
    ]
    )
    paths.quality_report_md.write_text("\n".join(lines), encoding="utf-8")


def write_figure_checklist(
    figure_records: list[tuple[str, str]],
    paths: PreprocessPaths,
) -> None:
    lines = [
        "# 河南周度预处理图表清单",
        "",
        "图表默认基于 `henan_weekly_features_modeling.csv` 生成，导出分辨率为 240 dpi。",
        "",
        "| 文件名 | 用途 |",
        "|---|---|",
    ]
    for filename, description in figure_records:
        lines.append(f"| `{filename}` | {description} |")
    paths.figure_checklist_md.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="构建河南周度建模预处理结果。")
    parser.add_argument(
        "--project-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="项目根目录，默认使用脚本上级目录。",
    )
    args = parser.parse_args()

    project_root = args.project_root.resolve()
    paths = init_paths(project_root)
    logger = init_logger()
    configure_plot_style()

    logger.info("开始构建河南周度目标表与特征表。")
    target_strict = build_target_weekly(project_root)
    features_strict = build_features_strict(project_root, target_strict)
    features_modeling = build_features_modeling(features_strict)

    logger.info("写出 CSV 成果文件。")
    target_strict.to_csv(paths.target_csv, index=False, encoding="utf-8-sig")
    features_strict.to_csv(paths.features_strict_csv, index=False, encoding="utf-8-sig")
    features_modeling.to_csv(paths.features_modeling_csv, index=False, encoding="utf-8-sig")

    logger.info("生成字段字典、描述统计与质量说明。")
    dictionary_df = build_data_dictionary(target_strict, features_strict, features_modeling)
    stats_df = build_descriptive_stats(features_modeling)
    dictionary_df.to_csv(paths.dictionary_csv, index=False, encoding="utf-8-sig")
    stats_df.to_csv(paths.stats_csv, index=False, encoding="utf-8-sig")

    logger.info("生成基础图表。")
    figure_records = create_figures(features_modeling, paths)
    write_quality_report(target_strict, features_strict, features_modeling, stats_df, figure_records, paths)
    write_figure_checklist(figure_records, paths)

    logger.info("河南周度预处理完成。")
    logger.info("目标表：%s", rel_path(paths.target_csv, project_root))
    logger.info("建模版：%s", rel_path(paths.features_modeling_csv, project_root))


if __name__ == "__main__":
    main()
