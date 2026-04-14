#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import time
from datetime import datetime
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_URL = "https://www.moa.gov.cn/ztzl/szcpxx/jdsj/"
CATEGORY_GUESSES = ["生产", "价格", "消费", "进出口", "成本收益"]


def init_paths(project_root: Path) -> dict[str, Path]:
    raw_root = project_root / "data" / "raw" / "moa_hog_monthly"
    interim_root = project_root / "data" / "interim" / "moa_hog_monthly_parsed"
    logs_root = project_root / "data" / "logs"
    metadata_root = project_root / "data" / "metadata"
    paths = {
        "raw_root": raw_root,
        "index_root": raw_root / "index_pages",
        "html_root": raw_root / "page_html",
        "text_root": raw_root / "page_text",
        "meta_root": raw_root / "page_meta",
        "excel_root": raw_root / "excel_files",
        "interim_root": interim_root,
        "logs_root": logs_root,
        "metadata_root": metadata_root,
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    paths["page_index_csv"] = paths["index_root"] / "page_index.csv"
    paths["long_csv"] = interim_root / "moa_hog_indicators_long.csv"
    paths["core_csv"] = interim_root / "moa_hog_core_metrics.csv"
    return paths


def init_logger(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("moa_hog_monthly")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    return logger


def build_session() -> requests.Session:
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            )
        }
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def normalize_text(text: str) -> str:
    text = (
        text.replace("\u3000", " ")
        .replace("\xa0", " ")
        .replace("／", "/")
        .replace("\r\n", "\n")
        .replace("\r", "\n")
    )
    lines = []
    for raw_line in text.split("\n"):
        line = re.sub(r"[ \t]+", " ", raw_line).strip()
        if line:
            lines.append(line)
    return "\n".join(lines)


def rel_path(path: Path, project_root: Path) -> str:
    return path.relative_to(project_root).as_posix()


def build_page_specs() -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = [
        {
            "period_key": "2021Q1",
            "period_type": "quarter",
            "report_year": 2021,
            "report_month": 3,
            "report_period": "2021Q1",
            "page_url": urljoin(BASE_URL, "2021yjd/"),
        }
    ]

    for month in range(4, 12):
        specs.append(
            {
                "period_key": f"2021-{month:02d}",
                "period_type": "month",
                "report_year": 2021,
                "report_month": month,
                "report_period": f"2021-{month:02d}",
                "page_url": urljoin(BASE_URL, f"2021{month:02d}/"),
            }
        )

    for year in range(2021, 2027):
        for month in range(1, 13):
            if year == 2021 and month < 12:
                continue
            if year == 2026 and month > 2:
                continue
            specs.append(
                {
                    "period_key": f"{year}-{month:02d}",
                    "period_type": "month",
                    "report_year": year,
                    "report_month": month,
                    "report_period": f"{year}-{month:02d}",
                    "page_url": urljoin(BASE_URL, f"{year}/{year}{month:02d}/"),
                }
            )
    return specs


def period_sort_key(period_key: str) -> tuple[int, int, int]:
    quarter_match = re.fullmatch(r"(\d{4})Q([1-4])", period_key)
    if quarter_match:
        return int(quarter_match.group(1)), int(quarter_match.group(2)) * 3, 0
    month_match = re.fullmatch(r"(\d{4})-(\d{2})", period_key)
    if month_match:
        return int(month_match.group(1)), int(month_match.group(2)), 1
    return (9999, 99, 99)


def fetch_html(session: requests.Session, url: str) -> str:
    response = session.get(url, timeout=30)
    response.raise_for_status()
    response.encoding = response.apparent_encoding or response.encoding or "utf-8"
    return response.text


def infer_publish_date_from_excel_url(excel_url: str | None) -> tuple[str | None, str]:
    if not excel_url:
        return None, "missing"
    match = re.search(r"[A-Z]020(\d{6})\d+\.xlsx?$", excel_url, re.I)
    if not match:
        return None, "missing"
    token = match.group(1)
    return f"20{token[:2]}-{token[2:4]}-{token[4:6]}", "excel_url_token"


def find_excel_href(soup: BeautifulSoup) -> tuple[str | None, str | None]:
    for anchor in soup.find_all("a", href=True):
        text = anchor.get_text(" ", strip=True)
        if "表格下载" in text:
            return anchor["href"], text
    return None, None


def table_signature(df: pd.DataFrame) -> tuple[Any, ...]:
    return tuple(df["指标"].astype(str).tolist())


def extract_main_tables_from_html(html: str) -> list[pd.DataFrame]:
    try:
        dfs = pd.read_html(StringIO(html))
    except ValueError:
        return []
    except Exception:
        return []

    main_tables: list[pd.DataFrame] = []
    seen_signatures: set[tuple[Any, ...]] = set()
    for df in dfs:
        cols = [str(col).strip() for col in df.columns]
        required = {"指标序号", "指标", "数值", "环比", "同比"}
        if not required.issubset(set(cols)):
            continue
        table = df[["指标序号", "指标", "数值", "环比", "同比"]].copy()
        table.columns = ["indicator_seq", "indicator_name_raw", "indicator_value_raw", "mom_raw", "yoy_raw"]
        table = table.fillna("")
        table = table[table["indicator_name_raw"].astype(str).str.strip() != ""]
        if table.empty:
            continue
        signature = table_signature(table.rename(columns={"indicator_name_raw": "指标"}))
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)
        main_tables.append(table)
    return main_tables


def extract_main_tables_from_excel(file_bytes: bytes) -> list[pd.DataFrame]:
    try:
        workbook = pd.ExcelFile(BytesIO(file_bytes))
    except Exception:
        return []

    sheet_name = "发布稿" if "发布稿" in workbook.sheet_names else workbook.sheet_names[0]
    df = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet_name)
    cols = [str(col).strip() for col in df.columns]
    required = {"指标分类", "序号", "指标", "数值", "环比", "同比"}
    if required.issubset(set(cols)):
        grouped_tables: list[pd.DataFrame] = []
        current_category = None
        bucket: list[dict[str, Any]] = []
        for _, row in df.iterrows():
            category = str(row.get("指标分类", "")).strip() or current_category
            current_category = category
            indicator_name = str(row.get("指标", "")).strip()
            if not indicator_name:
                continue
            bucket.append(
                {
                    "indicator_seq": row.get("序号", ""),
                    "indicator_name_raw": indicator_name,
                    "indicator_value_raw": row.get("数值", ""),
                    "mom_raw": row.get("环比", ""),
                    "yoy_raw": row.get("同比", ""),
                    "category": category,
                }
            )
        if bucket:
            table = pd.DataFrame(bucket)
            grouped_tables.append(table)
        return grouped_tables
    return []


def extract_unit(indicator_name: str) -> str | None:
    matches = re.findall(r"[（(]([^（）()]+)[）)]", indicator_name)
    return matches[-1] if matches else None


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text == "—":
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text.replace(",", ""))
    if not match:
        return None
    return round(float(match.group()), 6)


def parse_percent(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace("％", "%")
    if not text or text == "—":
        return None
    match = re.search(r"(-?\d+(?:\.\d+)?)%", text)
    if match:
        return round(float(match.group(1)) / 100, 6)
    return to_float(text)


def normalize_indicator_name(name: str) -> str:
    return re.sub(r"\s+", "", str(name)).strip()


def parse_tables_to_long_rows(
    tables: list[pd.DataFrame],
    spec: dict[str, Any],
    page_url: str,
    excel_url: str | None,
    publish_date: str | None,
    publish_date_source: str,
    parsing_source: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for table_idx, table in enumerate(tables, start=1):
        category_guess = CATEGORY_GUESSES[table_idx - 1] if table_idx - 1 < len(CATEGORY_GUESSES) else None
        explicit_category = table["category"].iloc[0] if "category" in table.columns else None
        category = explicit_category or category_guess
        for _, row in table.iterrows():
            indicator_name = str(row.get("indicator_name_raw", "")).strip()
            if not indicator_name:
                continue
            rows.append(
                {
                    "period_key": spec["period_key"],
                    "report_period": spec["report_period"],
                    "period_type": spec["period_type"],
                    "report_year": spec["report_year"],
                    "report_month": spec["report_month"],
                    "page_url": page_url,
                    "excel_url": excel_url,
                    "publish_date": publish_date,
                    "publish_date_source": publish_date_source,
                    "table_index": table_idx,
                    "indicator_category": category,
                    "indicator_seq": str(row.get("indicator_seq", "")).strip(),
                    "indicator_name_raw": indicator_name,
                    "indicator_name_normalized": normalize_indicator_name(indicator_name),
                    "indicator_unit": extract_unit(indicator_name),
                    "indicator_value_raw": str(row.get("indicator_value_raw", "")).strip(),
                    "indicator_value": to_float(row.get("indicator_value_raw", "")),
                    "mom_raw": str(row.get("mom_raw", "")).strip(),
                    "mom": parse_percent(row.get("mom_raw", "")),
                    "yoy_raw": str(row.get("yoy_raw", "")).strip(),
                    "yoy": parse_percent(row.get("yoy_raw", "")),
                    "parsing_source": parsing_source,
                }
            )
    return rows


def pick_first(rows: list[dict[str, Any]], predicate) -> dict[str, Any] | None:
    for row in rows:
        if predicate(row):
            return row
    return None


def build_core_row(spec: dict[str, Any], long_rows: list[dict[str, Any]], page_url: str, excel_url: str | None, publish_date: str | None, publish_date_source: str, raw_html_path: str, raw_excel_path: str | None) -> dict[str, Any]:
    hog_row = pick_first(
        long_rows,
        lambda row: "全国生猪出场价格" in row["indicator_name_raw"],
    )
    sow_row = pick_first(
        long_rows,
        lambda row: "能繁母猪存栏" in row["indicator_name_raw"] and ("万头" in (row["indicator_unit"] or "")) and row["indicator_value"] is not None,
    )
    sow_fallback_row = pick_first(
        long_rows,
        lambda row: "能繁母猪存栏" in row["indicator_name_raw"],
    )

    validation_notes: list[str] = []
    if not hog_row:
        validation_flag = "missing_hog_exfarm_price"
        validation_notes.append("全国生猪出场价格缺失")
    elif not sow_row:
        validation_flag = "sow_inventory_missing"
        if sow_fallback_row:
            validation_notes.append(f"能繁母猪存栏未提供绝对值:{sow_fallback_row['indicator_name_raw']}")
        else:
            validation_notes.append("能繁母猪存栏缺失")
    else:
        validation_flag = "ok"

    return {
        "period_key": spec["period_key"],
        "report_period": spec["report_period"],
        "period_type": spec["period_type"],
        "report_year": spec["report_year"],
        "report_month": spec["report_month"],
        "page_url": page_url,
        "excel_url": excel_url,
        "publish_date": publish_date,
        "publish_date_source": publish_date_source,
        "breeding_sow_inventory": sow_row["indicator_value"] if sow_row else None,
        "breeding_sow_inventory_unit": sow_row["indicator_unit"] if sow_row else None,
        "breeding_sow_inventory_indicator_raw": sow_row["indicator_name_raw"] if sow_row else (sow_fallback_row["indicator_name_raw"] if sow_fallback_row else None),
        "breeding_sow_inventory_mom": sow_row["mom"] if sow_row else (sow_fallback_row["mom"] if sow_fallback_row else None),
        "breeding_sow_inventory_yoy": sow_row["yoy"] if sow_row else (sow_fallback_row["yoy"] if sow_fallback_row else None),
        "breeding_sow_inventory_period_type": "quarter_end_stock" if sow_row else "missing",
        "hog_exfarm_price": hog_row["indicator_value"] if hog_row else None,
        "hog_exfarm_price_unit": hog_row["indicator_unit"] if hog_row else None,
        "hog_exfarm_price_indicator_raw": hog_row["indicator_name_raw"] if hog_row else None,
        "hog_exfarm_price_mom": hog_row["mom"] if hog_row else None,
        "hog_exfarm_price_yoy": hog_row["yoy"] if hog_row else None,
        "parsing_source": hog_row["parsing_source"] if hog_row else (long_rows[0]["parsing_source"] if long_rows else None),
        "raw_html_path": raw_html_path,
        "raw_excel_path": raw_excel_path,
        "validation_flag": validation_flag,
        "validation_notes": "; ".join(validation_notes),
    }


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def dump_json(path: Path, payload: Any) -> None:
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)


def summarize(core_rows: list[dict[str, Any]], index_rows: list[dict[str, Any]], failed_pages: list[str], long_rows_count: int) -> dict[str, Any]:
    available_periods = [row["report_period"] for row in sorted(core_rows, key=lambda row: period_sort_key(row["period_key"]))]
    sow_count = sum(1 for row in core_rows if row.get("breeding_sow_inventory") is not None)
    hog_count = sum(1 for row in core_rows if row.get("hog_exfarm_price") is not None)
    review_rows = [
        {
            "period_key": row["period_key"],
            "report_period": row["report_period"],
            "page_url": row["page_url"],
            "validation_flag": row["validation_flag"],
            "validation_notes": row["validation_notes"],
        }
        for row in core_rows
        if row["validation_flag"] != "ok" or row["validation_notes"]
    ]
    return {
        "page_specs_total": len(index_rows),
        "pages_successfully_processed": len(core_rows),
        "failed_pages": failed_pages,
        "missing_pages": [row["page_url"] for row in index_rows if not row["page_exists"]],
        "time_coverage": {
            "start": available_periods[0] if available_periods else None,
            "end": available_periods[-1] if available_periods else None,
        },
        "long_table_row_count": long_rows_count,
        "core_table_row_count": len(core_rows),
        "breeding_sow_inventory_non_null_count": sow_count,
        "hog_exfarm_price_non_null_count": hog_count,
        "breeding_sow_inventory_non_null_rate": round(sow_count / len(core_rows), 4) if core_rows else 0.0,
        "hog_exfarm_price_non_null_rate": round(hog_count / len(core_rows), 4) if core_rows else 0.0,
        "manual_review_rows": review_rows,
    }


def crawl(args: argparse.Namespace) -> dict[str, Any]:
    project_root = Path(__file__).resolve().parents[1]
    paths = init_paths(project_root)
    log_path = paths["logs_root"] / f"moa_hog_monthly_crawl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = init_logger(log_path)
    session = build_session()
    discovered_at = now_iso()

    specs = build_page_specs()
    if args.max_pages:
        specs = specs[: args.max_pages]
        logger.info("启用样本限制，仅处理前 %s 个页面", args.max_pages)

    index_rows: list[dict[str, Any]] = []
    long_rows: list[dict[str, Any]] = []
    core_rows: list[dict[str, Any]] = []
    failed_pages: list[str] = []

    logger.info("抓取开始，页面数=%s", len(specs))
    for idx, spec in enumerate(specs, start=1):
        page_url = spec["page_url"]
        logger.info("处理页面 [%s/%s] %s", idx, len(specs), page_url)
        page_exists = False
        excel_href_raw = None
        excel_url = None
        html_table_count = 0
        parsing_source = None
        has_sow = False
        has_hog = False
        html_path = None
        text_path = None
        excel_path = None
        publish_date = None
        publish_date_source = "missing"
        notes: list[str] = []

        try:
            response = session.get(page_url, timeout=30)
            if response.status_code == 404:
                notes.append("page_404")
                index_rows.append(
                    {
                        **spec,
                        "page_url": page_url,
                        "page_exists": False,
                        "excel_href_raw": None,
                        "excel_url": None,
                        "discovered_at": discovered_at,
                        "html_table_count": 0,
                        "has_breeding_sow_inventory": False,
                        "has_hog_exfarm_price": False,
                        "notes": "; ".join(notes),
                    }
                )
                logger.warning("页面不存在：%s", page_url)
                continue

            response.raise_for_status()
            page_exists = True
            response.encoding = response.apparent_encoding or response.encoding or "utf-8"
            html = response.text
            soup = BeautifulSoup(html, "lxml")

            excel_href_raw, _ = find_excel_href(soup)
            excel_url = urljoin(page_url, excel_href_raw) if excel_href_raw else None
            publish_date, publish_date_source = infer_publish_date_from_excel_url(excel_url)

            file_stub = spec["period_key"].replace("Q", "_Q")
            html_path = paths["html_root"] / f"{file_stub}.html"
            text_path = paths["text_root"] / f"{file_stub}.txt"
            meta_path = paths["meta_root"] / f"{file_stub}.json"
            html_path.write_text(html, encoding="utf-8")
            text_path.write_text(normalize_text(soup.get_text("\n", strip=True)), encoding="utf-8")

            html_tables = extract_main_tables_from_html(html)
            html_table_count = len(html_tables)
            selected_tables = html_tables
            parsing_source = "html_table"

            excel_bytes = None
            if excel_url:
                excel_response = session.get(excel_url, timeout=60)
                excel_response.raise_for_status()
                excel_bytes = excel_response.content
                excel_ext = Path(excel_url).suffix or ".xlsx"
                excel_path = paths["excel_root"] / f"{file_stub}{excel_ext}"
                excel_path.write_bytes(excel_bytes)

            if not selected_tables and excel_bytes:
                selected_tables = extract_main_tables_from_excel(excel_bytes)
                parsing_source = "excel_fallback"
                notes.append("html_tables_missing_used_excel_fallback")

            page_long_rows = parse_tables_to_long_rows(
                tables=selected_tables,
                spec=spec,
                page_url=page_url,
                excel_url=excel_url,
                publish_date=publish_date,
                publish_date_source=publish_date_source,
                parsing_source=parsing_source or "missing",
            )
            long_rows.extend(page_long_rows)

            has_sow = any("能繁母猪存栏" in row["indicator_name_raw"] for row in page_long_rows)
            has_hog = any("全国生猪出场价格" in row["indicator_name_raw"] for row in page_long_rows)

            core_row = build_core_row(
                spec=spec,
                long_rows=page_long_rows,
                page_url=page_url,
                excel_url=excel_url,
                publish_date=publish_date,
                publish_date_source=publish_date_source,
                raw_html_path=rel_path(html_path, project_root),
                raw_excel_path=rel_path(excel_path, project_root) if excel_path else None,
            )
            core_rows.append(core_row)

            meta_payload = {
                "page_url": page_url,
                "report_period": spec["report_period"],
                "period_type": spec["period_type"],
                "report_year": spec["report_year"],
                "report_month": spec["report_month"],
                "publish_date": publish_date,
                "publish_date_source": publish_date_source,
                "excel_url": excel_url,
                "fetched_at": now_iso(),
                "html_path": rel_path(html_path, project_root),
                "text_path": rel_path(text_path, project_root),
                "excel_path": rel_path(excel_path, project_root) if excel_path else None,
                "html_table_count": html_table_count,
                "parsing_source": parsing_source,
                "notes": "; ".join(notes + ([core_row["validation_notes"]] if core_row["validation_notes"] else [])),
            }
            dump_json(meta_path, meta_payload)
        except Exception as exc:
            failed_pages.append(page_url)
            notes.append(f"error:{exc}")
            logger.exception("页面抓取失败：%s", page_url)

        index_rows.append(
            {
                **spec,
                "page_url": page_url,
                "page_exists": page_exists,
                "excel_href_raw": excel_href_raw,
                "excel_url": excel_url,
                "discovered_at": discovered_at,
                "html_table_count": html_table_count,
                "has_breeding_sow_inventory": has_sow,
                "has_hog_exfarm_price": has_hog,
                "notes": "; ".join(notes),
            }
        )
        time.sleep(args.sleep_seconds)

    index_rows.sort(key=lambda row: period_sort_key(row["period_key"]))
    long_rows.sort(key=lambda row: (period_sort_key(row["period_key"]), row["table_index"], row["indicator_seq"], row["indicator_name_raw"]))
    core_rows.sort(key=lambda row: period_sort_key(row["period_key"]))

    write_csv(
        paths["page_index_csv"],
        index_rows,
        [
            "period_key",
            "report_period",
            "period_type",
            "report_year",
            "report_month",
            "page_url",
            "page_exists",
            "excel_href_raw",
            "excel_url",
            "discovered_at",
            "html_table_count",
            "has_breeding_sow_inventory",
            "has_hog_exfarm_price",
            "notes",
        ],
    )
    write_csv(
        paths["long_csv"],
        long_rows,
        [
            "period_key",
            "report_period",
            "period_type",
            "report_year",
            "report_month",
            "page_url",
            "excel_url",
            "publish_date",
            "publish_date_source",
            "table_index",
            "indicator_category",
            "indicator_seq",
            "indicator_name_raw",
            "indicator_name_normalized",
            "indicator_unit",
            "indicator_value_raw",
            "indicator_value",
            "mom_raw",
            "mom",
            "yoy_raw",
            "yoy",
            "parsing_source",
        ],
    )
    write_csv(
        paths["core_csv"],
        core_rows,
        [
            "period_key",
            "report_period",
            "period_type",
            "report_year",
            "report_month",
            "page_url",
            "excel_url",
            "publish_date",
            "publish_date_source",
            "breeding_sow_inventory",
            "breeding_sow_inventory_unit",
            "breeding_sow_inventory_indicator_raw",
            "breeding_sow_inventory_mom",
            "breeding_sow_inventory_yoy",
            "breeding_sow_inventory_period_type",
            "hog_exfarm_price",
            "hog_exfarm_price_unit",
            "hog_exfarm_price_indicator_raw",
            "hog_exfarm_price_mom",
            "hog_exfarm_price_yoy",
            "parsing_source",
            "raw_html_path",
            "raw_excel_path",
            "validation_flag",
            "validation_notes",
        ],
    )

    summary = summarize(core_rows, index_rows, failed_pages, len(long_rows))
    summary.update(
        {
            "run_started_at": discovered_at,
            "run_finished_at": now_iso(),
            "page_index_csv": rel_path(paths["page_index_csv"], project_root),
            "long_csv": rel_path(paths["long_csv"], project_root),
            "core_csv": rel_path(paths["core_csv"], project_root),
            "log_path": rel_path(log_path, project_root),
        }
    )
    summary_path = paths["metadata_root"] / "moa_hog_monthly_summary.json"
    manual_review_path = paths["metadata_root"] / "moa_hog_monthly_manual_review.csv"
    dump_json(summary_path, summary)
    write_csv(
        manual_review_path,
        summary["manual_review_rows"],
        ["period_key", "report_period", "page_url", "validation_flag", "validation_notes"],
    )

    logger.info("页面索引已写入：%s", paths["page_index_csv"])
    logger.info("指标长表已写入：%s", paths["long_csv"])
    logger.info("核心宽表已写入：%s", paths["core_csv"])
    logger.info("摘要已写入：%s", summary_path)
    logger.info("人工复核清单已写入：%s", manual_review_path)
    logger.info(
        "抓取结束：页面=%s 成功=%s 长表=%s 核心宽表=%s 失败=%s",
        len(index_rows),
        len(core_rows),
        len(long_rows),
        len(core_rows),
        len(failed_pages),
    )
    return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="抓取农业农村部生猪专题月度数据")
    parser.add_argument("--max-pages", type=int, default=0, help="仅处理前 N 个页面，0 表示全量")
    parser.add_argument("--sleep-seconds", type=float, default=0.05, help="请求间隔秒数")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    summary = crawl(args)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
