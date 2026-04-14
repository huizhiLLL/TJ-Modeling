#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import re
import time
from dataclasses import dataclass
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_LIST_URL = "https://www.agri.cn/sj/jcyj/"
LIST_HOME = urljoin(BASE_LIST_URL, "index.htm")
TITLE_PATTERN = re.compile(
    r"^\s*(\d{1,2})(?:月份|月)第(\d{1,2})周畜产品和饲料集贸市场价格情况\s*$"
)
ARTICLE_HREF_PATTERN = re.compile(r"^(?:\./)?(\d{6})/(t\d{8}_\d+)\.htm$")
TARGET_ITEMS = {
    "hog_price": "生猪",
    "corn_price": "玉米",
    "soymeal_price": "豆粕",
    "piglet_price": "仔猪",
    "mixed_feed_price": "育肥猪配合饲料",
}
CORE_FIELDS = ("hog_price", "corn_price", "soymeal_price")


@dataclass
class CrawlPaths:
    project_root: Path
    data_root: Path
    raw_root: Path
    article_root: Path
    index_root: Path
    html_root: Path
    text_root: Path
    meta_root: Path
    interim_root: Path
    logs_root: Path
    metadata_root: Path
    article_index_csv: Path
    parsed_prices_csv: Path


def init_paths(project_root: Path) -> CrawlPaths:
    data_root = project_root / "data"
    raw_root = data_root / "raw"
    article_root = raw_root / "agri_weekly_articles"
    index_root = article_root / "index_pages"
    html_root = article_root / "article_html"
    text_root = article_root / "article_text"
    meta_root = article_root / "article_meta"
    interim_root = data_root / "interim" / "agri_weekly_parsed"
    logs_root = data_root / "logs"
    metadata_root = data_root / "metadata"

    for path in [
        index_root,
        html_root,
        text_root,
        meta_root,
        interim_root,
        logs_root,
        metadata_root,
    ]:
        path.mkdir(parents=True, exist_ok=True)

    return CrawlPaths(
        project_root=project_root,
        data_root=data_root,
        raw_root=raw_root,
        article_root=article_root,
        index_root=index_root,
        html_root=html_root,
        text_root=text_root,
        meta_root=meta_root,
        interim_root=interim_root,
        logs_root=logs_root,
        metadata_root=metadata_root,
        article_index_csv=index_root / "article_index.csv",
        parsed_prices_csv=interim_root / "agri_weekly_prices.csv",
    )


def init_logger(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("agri_weekly_crawler")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
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


def normalize_text(text: str) -> str:
    text = (
        text.replace("\u3000", " ")
        .replace("\xa0", " ")
        .replace("\r\n", "\n")
        .replace("\r", "\n")
        .replace("／", "/")
        .replace("　", " ")
    )
    lines = []
    for raw_line in text.split("\n"):
        line = re.sub(r"[ \t]+", " ", raw_line).strip()
        if line:
            lines.append(line)
    return "\n".join(lines)


def normalize_inline(text: str) -> str:
    return re.sub(r"\s+", "", text.replace("\u3000", " ").replace("\xa0", " ")).strip()


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def rel_path(path: Path, project_root: Path) -> str:
    return path.relative_to(project_root).as_posix()


def build_list_page_url(page_no: int) -> str:
    if page_no == 1:
        return LIST_HOME
    return urljoin(BASE_LIST_URL, f"index_{page_no - 1}.htm")


def fetch_html(session: requests.Session, url: str) -> str:
    response = session.get(url, timeout=30)
    response.raise_for_status()
    response.encoding = response.apparent_encoding or response.encoding or "utf-8"
    return response.text


def parse_list_page(html: str, list_page_url: str, page_no: int, discovered_at: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    rows: list[dict[str, Any]] = []
    seen_pairs: set[tuple[str, str]] = set()
    for anchor in soup.find_all("a", href=True):
        href_raw = anchor.get("href", "").strip()
        title = anchor.get_text(" ", strip=True).strip()
        if not href_raw or not title or title == "阅读更多 >":
            continue
        href_match = ARTICLE_HREF_PATTERN.match(href_raw)
        if not href_match:
            continue
        article_url = urljoin(list_page_url, href_raw)
        pair = (article_url, title)
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        rows.append(
            {
                "list_page_url": list_page_url,
                "page_no": page_no,
                "article_title": title,
                "article_href_raw": href_raw,
                "article_url": article_url,
                "title_match_flag": bool(TITLE_PATTERN.fullmatch(title)),
                "discovered_at": discovered_at,
            }
        )
    return rows


def week_label_from_title(title: str) -> str | None:
    match = TITLE_PATTERN.fullmatch(title.strip())
    if not match:
        return None
    return f"{int(match.group(1))}月第{int(match.group(2))}周"


def extract_article_year(article_url: str) -> int | None:
    match = re.search(r"/(\d{6})/t\d{8}_\d+\.htm$", article_url)
    if not match:
        return None
    return int(match.group(1)[:4])


def article_id_from_url(article_url: str) -> str:
    path = urlparse(article_url).path
    match = re.search(r"/(t\d{8}_\d+)\.htm$", path)
    if match:
        return match.group(1)
    return re.sub(r"[^0-9A-Za-z_-]+", "_", path.strip("/"))


def extract_article_meta(soup: BeautifulSoup, article_url: str) -> dict[str, Any]:
    title = None
    title_node = soup.select_one(".detailCon_info_tit")
    if title_node:
        title = title_node.get_text(" ", strip=True)
    if not title:
        title_text = soup.title.get_text(strip=True) if soup.title else ""
        title = title_text.split("_", 1)[-1].strip()

    publish_date = None
    publish_meta = soup.find("meta", attrs={"name": "publishdate"})
    if publish_meta and publish_meta.get("content"):
        publish_date = publish_meta["content"][:10]
    if not publish_date:
        time_text = soup.get_text("\n", strip=True)
        time_match = re.search(r"时间[:：]\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", time_text)
        if time_match:
            publish_date = time_match.group(1)

    source = "中国农业信息网"
    source_meta = soup.find("meta", attrs={"name": "source"})
    if source_meta and source_meta.get("content"):
        source = source_meta["content"].strip() or source

    content_node = soup.select_one(".content_body_box.ArticleDetails")
    content_html = str(content_node) if content_node else ""
    content_text = normalize_text(content_node.get_text("\n", strip=True) if content_node else soup.get_text("\n", strip=True))

    return {
        "article_url": article_url,
        "title": title.strip(),
        "publish_date": publish_date,
        "source_detail": source,
        "content_html": content_html,
        "content_text": content_text,
        "content_found": bool(content_node),
    }


def read_tables(content_html: str) -> list[pd.DataFrame]:
    if not content_html:
        return []
    try:
        return pd.read_html(StringIO(content_html))
    except ValueError:
        return []
    except Exception:
        return []


def normalize_unit(unit: str | None) -> str | None:
    if not unit:
        return unit
    unit = unit.replace("／", "/").replace(" ", "")
    unit = unit.replace("元/千克", "元/公斤")
    if "元/公斤" in unit and "元/只" in unit:
        return "元/公斤"
    return unit


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, float):
        return None if math.isnan(value) else round(value, 4)
    text = str(value).strip()
    if not text:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text.replace(",", ""))
    if not match:
        return None
    return round(float(match.group()), 4)


def parse_table_values(content_html: str) -> tuple[dict[str, float | None], dict[str, str | None], str | None]:
    values = {field: None for field in TARGET_ITEMS}
    units = {f"{field}_unit": None for field in TARGET_ITEMS}
    table_unit: str | None = None
    for table in read_tables(content_html):
        normalized_table = table.copy()
        normalized_table = normalized_table.fillna("")
        normalized_table = normalized_table.apply(
            lambda column: column.map(lambda x: str(x).replace("／", "/").strip())
        )

        flattened = " ".join(normalized_table.astype(str).fillna("").to_numpy().flatten())
        unit_match = re.search(r"单位[:：]\s*([^ ]+)", flattened)
        if unit_match:
            table_unit = normalize_unit(unit_match.group(1))

        header_row_index = None
        for idx in range(min(5, len(normalized_table))):
            row_text = "".join(normalized_table.iloc[idx].astype(str).tolist())
            if "项目" in row_text and "本周" in row_text:
                header_row_index = idx
                break

        data_rows = normalized_table.iloc[header_row_index + 1 :] if header_row_index is not None else normalized_table
        for _, row in data_rows.iterrows():
            cells = [str(cell).strip() for cell in row.tolist()]
            if not any(cells):
                continue
            item_name = normalize_inline(cells[0])
            for field, label in TARGET_ITEMS.items():
                if item_name == normalize_inline(label):
                    current_value = None
                    for cell in cells[1:]:
                        current_value = to_float(cell)
                        if current_value is not None:
                            break
                    if current_value is not None:
                        values[field] = current_value
                        units[f"{field}_unit"] = table_unit or "元/公斤"
    return values, units, table_unit


def parse_regex_values(text: str) -> tuple[dict[str, float | None], dict[str, str | None]]:
    values = {field: None for field in TARGET_ITEMS}
    units = {f"{field}_unit": None for field in TARGET_ITEMS}
    patterns = {
        "hog_price": r"全国生猪平均价格\s*([0-9.]+)\s*(元/[公斤只]+)",
        "corn_price": r"全国玉米平均价格\s*([0-9.]+)\s*(元/[公斤只]+)",
        "soymeal_price": r"全国豆粕平均价格\s*([0-9.]+)\s*(元/[公斤只]+)",
        "piglet_price": r"全国仔猪平均价格\s*([0-9.]+)\s*(元/[公斤只]+)",
        "mixed_feed_price": r"育肥猪配合饲料平均价格\s*([0-9.]+)\s*(元/[公斤只]+)",
    }
    compact_text = normalize_text(text).replace("\n", "")
    for field, pattern in patterns.items():
        match = re.search(pattern, compact_text)
        if match:
            values[field] = to_float(match.group(1))
            units[f"{field}_unit"] = normalize_unit(match.group(2))
    return values, units


def parse_collect_date(text: str, publish_date: str | None) -> str | None:
    match = re.search(r"采集日为(\d{1,2})月(\d{1,2})日", text.replace(" ", ""))
    if not match:
        return None
    if not publish_date:
        return None
    year = publish_date[:4]
    month = int(match.group(1))
    day = int(match.group(2))
    return f"{year}-{month:02d}-{day:02d}"


def parse_article_record(article_meta: dict[str, Any]) -> dict[str, Any]:
    title = article_meta["title"]
    publish_date = article_meta["publish_date"]
    content_html = article_meta["content_html"]
    content_text = article_meta["content_text"]
    week_label = week_label_from_title(title)
    collect_date = parse_collect_date(content_text, publish_date)

    table_values, table_units, table_unit = parse_table_values(content_html)
    regex_values, regex_units = parse_regex_values(content_text)

    parsed_values: dict[str, float | None] = {}
    parsed_units: dict[str, str | None] = {}
    used_table = False
    used_regex = False
    for field in TARGET_ITEMS:
        if table_values[field] is not None:
            parsed_values[field] = table_values[field]
            parsed_units[f"{field}_unit"] = normalize_unit(table_units[f"{field}_unit"])
            used_table = True
        else:
            parsed_values[field] = regex_values[field]
            parsed_units[f"{field}_unit"] = normalize_unit(regex_units[f"{field}_unit"])
            used_regex = used_regex or regex_values[field] is not None

    parsing_method = "table" if used_table and not used_regex else "regex" if used_regex and not used_table else "fallback"
    if not used_table and not used_regex:
        parsing_method = "fallback"

    validation_notes: list[str] = []
    missing_core = [field for field in CORE_FIELDS if parsed_values.get(field) is None]
    suspicious_unit = False
    for field in CORE_FIELDS:
        unit = parsed_units.get(f"{field}_unit")
        if unit and unit != "元/公斤":
            suspicious_unit = True
            validation_notes.append(f"{field}单位异常:{unit}")
    if not collect_date:
        validation_notes.append("collect_date_missing")
    if missing_core:
        validation_notes.append(f"missing_core_fields:{','.join(missing_core)}")

    if missing_core:
        validation_flag = "missing_core_fields"
    elif suspicious_unit:
        validation_flag = "suspicious_unit"
    else:
        validation_flag = "ok"

    record = {
        "article_url": article_meta["article_url"],
        "title": title,
        "publish_date": publish_date,
        "week_label": week_label,
        "collect_date": collect_date,
        **parsed_values,
        **parsed_units,
        "parsing_method": parsing_method,
        "validation_flag": validation_flag,
        "validation_notes": "; ".join(validation_notes),
    }
    if table_unit and not any(parsed_units.values()):
        for field in TARGET_ITEMS:
            record[f"{field}_unit"] = table_unit
    return record


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def dump_json(path: Path, payload: Any) -> None:
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)


def collect_target_articles(index_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for row in index_rows:
        if not row["title_match_flag"]:
            continue
        article_year = extract_article_year(row["article_url"])
        if article_year is not None and article_year < 2021:
            continue
        deduped[row["article_url"]] = row
    return list(deduped.values())


def add_duplicate_week_flags(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not row.get("publish_date") or not row.get("week_label"):
            continue
        key = f"{row['publish_date'][:4]}-{row['week_label']}"
        grouped.setdefault(key, []).append(row)

    duplicate_notes: list[str] = []
    filtered_rows = rows[:]
    for key, group in grouped.items():
        if len(group) <= 1:
            continue
        group.sort(key=lambda item: ((item.get("publish_date") or ""), item["article_url"]))
        for row in group[:-1]:
            notes = row.get("validation_notes", "")
            row["validation_flag"] = "duplicate_week"
            row["validation_notes"] = "; ".join(part for part in [notes, f"duplicate_of:{group[-1]['article_url']}"] if part)
            duplicate_notes.append(row["article_url"])
        latest = group[-1]
        latest_notes = latest.get("validation_notes", "")
        latest["validation_notes"] = "; ".join(part for part in [latest_notes, "kept_latest_duplicate_week"] if part)
    unique_final = [
        row
        for row in filtered_rows
        if row.get("validation_flag") != "duplicate_week"
    ]
    return unique_final, duplicate_notes


def detect_gaps(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    dated = []
    for row in rows:
        publish_date = row.get("publish_date")
        if not publish_date:
            continue
        try:
            dated.append((datetime.strptime(publish_date, "%Y-%m-%d"), row))
        except ValueError:
            continue
    dated.sort(key=lambda item: item[0])
    gaps: list[dict[str, Any]] = []
    for previous, current in zip(dated, dated[1:]):
        delta = (current[0] - previous[0]).days
        if delta > 14:
            gaps.append(
                {
                    "from": previous[0].strftime("%Y-%m-%d"),
                    "to": current[0].strftime("%Y-%m-%d"),
                    "gap_days": delta,
                    "from_title": previous[1]["title"],
                    "to_title": current[1]["title"],
                }
            )
    return gaps


def summarize_results(
    index_rows: list[dict[str, Any]],
    target_articles: list[dict[str, Any]],
    fetched_article_count: int,
    parsed_before_dedup_count: int,
    parsed_rows: list[dict[str, Any]],
    failed_urls: list[str],
    missing_core_urls: list[str],
    duplicate_urls: list[str],
) -> dict[str, Any]:
    publish_dates = sorted(row["publish_date"] for row in parsed_rows if row.get("publish_date"))
    min_date = publish_dates[0] if publish_dates else None
    max_date = publish_dates[-1] if publish_dates else None

    all_core_ok = sum(
        1
        for row in parsed_rows
        if all(row.get(field) is not None for field in CORE_FIELDS)
    )
    extraction_rate = round(all_core_ok / len(parsed_rows), 4) if parsed_rows else 0.0

    field_success = {}
    for field in CORE_FIELDS:
        success = sum(1 for row in parsed_rows if row.get(field) is not None)
        field_success[field] = {
            "count": success,
            "rate": round(success / len(parsed_rows), 4) if parsed_rows else 0.0,
        }

    review_articles = [
        {
            "article_url": row["article_url"],
            "title": row["title"],
            "publish_date": row["publish_date"],
            "validation_flag": row["validation_flag"],
            "validation_notes": row["validation_notes"],
        }
        for row in parsed_rows
        if row.get("validation_flag") != "ok" or row.get("validation_notes")
    ]

    return {
        "list_pages_visited": len({row["list_page_url"] for row in index_rows}),
        "articles_discovered": len(index_rows),
        "target_articles_matched": sum(1 for row in index_rows if row["title_match_flag"]),
        "target_articles_after_dedup": len(target_articles),
        "articles_fetched_successfully": fetched_article_count,
        "articles_parsed_before_duplicate_filter": parsed_before_dedup_count,
        "articles_parsed_final_unique": len(parsed_rows),
        "failed_count": len(failed_urls),
        "failed_urls": failed_urls,
        "missing_core_field_urls": missing_core_urls,
        "duplicate_week_urls": duplicate_urls,
        "time_coverage": {
            "start": min_date,
            "end": max_date,
        },
        "core_extraction_rate": {
            "all_core_fields": extraction_rate,
            **field_success,
        },
        "gaps_over_14_days": detect_gaps(parsed_rows),
        "manual_review_articles": review_articles,
    }


def crawl(args: argparse.Namespace) -> dict[str, Any]:
    project_root = Path(__file__).resolve().parents[1]
    paths = init_paths(project_root)
    run_started_at = now_iso()
    log_path = paths.logs_root / f"agri_weekly_crawl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = init_logger(log_path)
    session = build_session()

    logger.info("抓取开始，页码范围：%s-%s", args.start_page, args.end_page)
    index_rows: list[dict[str, Any]] = []
    failed_urls: list[str] = []
    page_failures = 0

    for page_no in range(args.start_page, args.end_page + 1):
        page_url = build_list_page_url(page_no)
        try:
            html = fetch_html(session, page_url)
            page_rows = parse_list_page(html, page_url, page_no, run_started_at)
            if not page_rows:
                page_failures += 1
                logger.warning("列表页未解析到文章链接：page_no=%s url=%s", page_no, page_url)
            else:
                page_failures = 0
                logger.info("列表页解析完成：page_no=%s 文章数=%s", page_no, len(page_rows))
                index_rows.extend(page_rows)
            if args.stop_after_empty_pages and page_failures >= args.stop_after_empty_pages:
                logger.warning("连续空页达到阈值，提前停止：%s", page_failures)
                break
        except Exception as exc:
            page_failures += 1
            failed_urls.append(page_url)
            logger.exception("列表页抓取失败：page_no=%s url=%s error=%s", page_no, page_url, exc)
            if page_failures >= args.stop_after_empty_pages:
                logger.warning("连续失败达到阈值，提前停止：%s", page_failures)
                break
        time.sleep(args.sleep_seconds)

    index_fieldnames = [
        "list_page_url",
        "page_no",
        "article_title",
        "article_href_raw",
        "article_url",
        "title_match_flag",
        "discovered_at",
    ]
    write_csv(paths.article_index_csv, index_rows, index_fieldnames)
    logger.info("列表索引已写入：%s", paths.article_index_csv)

    target_articles = collect_target_articles(index_rows)
    if args.max_target_articles:
        target_articles = target_articles[: args.max_target_articles]
        logger.info("启用样本限制，仅抓取前 %s 篇目标文章", args.max_target_articles)

    parsed_rows: list[dict[str, Any]] = []
    missing_core_urls: list[str] = []
    fetched_article_count = 0

    for idx, article in enumerate(target_articles, start=1):
        article_url = article["article_url"]
        article_id = article_id_from_url(article_url)
        logger.info("抓取文章 [%s/%s] %s", idx, len(target_articles), article_url)
        try:
            html = fetch_html(session, article_url)
            soup = BeautifulSoup(html, "lxml")
            article_meta = extract_article_meta(soup, article_url)
            article_publish_date = article_meta["publish_date"] or "unknown_date"
            file_prefix = f"{article_publish_date}_{article_id}"

            html_path = paths.html_root / f"{file_prefix}.html"
            text_path = paths.text_root / f"{file_prefix}.txt"
            meta_path = paths.meta_root / f"{file_prefix}.json"

            html_path.write_text(article_meta["content_html"] or html, encoding="utf-8")
            text_path.write_text(article_meta["content_text"], encoding="utf-8")

            parsed_record = parse_article_record(article_meta)
            parsed_record["raw_text_path"] = rel_path(text_path, project_root)
            parsed_record["raw_html_path"] = rel_path(html_path, project_root)

            parse_status = "ok" if parsed_record["validation_flag"] == "ok" else parsed_record["validation_flag"]
            notes = []
            if not article_meta["content_found"]:
                notes.append("content_container_missing_saved_full_html")
            if parsed_record["validation_notes"]:
                notes.append(parsed_record["validation_notes"])

            meta_payload = {
                "article_url": article_url,
                "title": article_meta["title"],
                "publish_date": article_meta["publish_date"],
                "week_label_from_title": week_label_from_title(article_meta["title"]),
                "source_name": "中国农业信息网",
                "source_channel": "监测预警/畜产品和饲料集贸市场价格情况",
                "fetched_at": now_iso(),
                "html_path": rel_path(html_path, project_root),
                "text_path": rel_path(text_path, project_root),
                "parse_status": parse_status,
                "notes": "; ".join(notes),
            }
            dump_json(meta_path, meta_payload)

            if parsed_record["validation_flag"] == "missing_core_fields":
                missing_core_urls.append(article_url)
            fetched_article_count += 1
            parsed_rows.append(parsed_record)
        except Exception as exc:
            failed_urls.append(article_url)
            logger.exception("文章抓取或解析失败：url=%s error=%s", article_url, exc)
        time.sleep(args.sleep_seconds)

    parsed_before_dedup_count = len(parsed_rows)
    parsed_rows, duplicate_urls = add_duplicate_week_flags(parsed_rows)
    parsed_fieldnames = [
        "article_url",
        "title",
        "publish_date",
        "week_label",
        "collect_date",
        "hog_price",
        "hog_price_unit",
        "corn_price",
        "corn_price_unit",
        "soymeal_price",
        "soymeal_price_unit",
        "piglet_price",
        "piglet_price_unit",
        "mixed_feed_price",
        "mixed_feed_price_unit",
        "parsing_method",
        "raw_text_path",
        "raw_html_path",
        "validation_flag",
        "validation_notes",
    ]
    write_csv(paths.parsed_prices_csv, parsed_rows, parsed_fieldnames)
    logger.info("结构化结果已写入：%s", paths.parsed_prices_csv)

    summary = summarize_results(
        index_rows=index_rows,
        target_articles=target_articles,
        fetched_article_count=fetched_article_count,
        parsed_before_dedup_count=parsed_before_dedup_count,
        parsed_rows=parsed_rows,
        failed_urls=failed_urls,
        missing_core_urls=missing_core_urls,
        duplicate_urls=duplicate_urls,
    )
    summary.update(
        {
            "run_started_at": run_started_at,
            "run_finished_at": now_iso(),
            "log_path": rel_path(log_path, project_root),
            "article_index_csv": rel_path(paths.article_index_csv, project_root),
            "parsed_prices_csv": rel_path(paths.parsed_prices_csv, project_root),
        }
    )
    summary_path = paths.metadata_root / "agri_weekly_summary.json"
    dump_json(summary_path, summary)
    review_csv_path = paths.metadata_root / "agri_weekly_manual_review.csv"
    write_csv(
        review_csv_path,
        summary["manual_review_articles"],
        ["article_url", "title", "publish_date", "validation_flag", "validation_notes"],
    )
    logger.info("摘要已写入：%s", summary_path)
    logger.info("人工复核清单已写入：%s", review_csv_path)
    logger.info(
        "抓取结束，目标文章=%s，抓取成功=%s，解析成功=%s，最终唯一=%s，失败=%s",
        len(target_articles),
        fetched_article_count,
        parsed_before_dedup_count,
        len(parsed_rows),
        len(failed_urls),
    )
    return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="抓取中国农业信息网畜产品和饲料集贸市场价格情况周报")
    parser.add_argument("--start-page", type=int, default=1, help="起始列表页编号，首页为 1")
    parser.add_argument("--end-page", type=int, default=50, help="结束列表页编号，默认到 50")
    parser.add_argument(
        "--stop-after-empty-pages",
        type=int,
        default=5,
        help="连续空页/失败页达到该阈值时提前停止",
    )
    parser.add_argument(
        "--max-target-articles",
        type=int,
        default=0,
        help="仅抓取前 N 篇目标文章，用于小样本验证；0 表示不限制",
    )
    parser.add_argument("--sleep-seconds", type=float, default=0.2, help="请求间隔秒数")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    summary = crawl(args)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
