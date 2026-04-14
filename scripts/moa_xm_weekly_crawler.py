#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import re
import time
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


BASE_LIST_URL = "https://www.moa.gov.cn/gk/jcyj/xm/"
TITLE_PATTERN = re.compile(
    r"^\s*(?:\d{4}年)?(\d{1,2})(?:月份|月)(第\d{1,2}周|最后(?:一|1)周)畜产品和饲料集贸市场价格情况\s*$"
)
ARTICLE_HREF_PATTERN = re.compile(r"^(?:\./)?(\d{6})/(t\d{8}_\d+)\.htm$")
TARGET_LABELS = {
    "live_hog_price": ["活猪", "生猪"],
    "corn_price": ["玉米"],
    "soymeal_price": ["豆粕"],
}
CORE_FIELDS = ("live_hog_price", "corn_price", "soymeal_price")


def init_paths(project_root: Path) -> dict[str, Path]:
    raw_root = project_root / "data" / "raw" / "moa_xm_weekly_articles"
    interim_root = project_root / "data" / "interim" / "moa_xm_weekly_parsed"
    logs_root = project_root / "data" / "logs"
    metadata_root = project_root / "data" / "metadata"
    paths = {
        "raw_root": raw_root,
        "index_root": raw_root / "index_pages",
        "html_root": raw_root / "article_html",
        "text_root": raw_root / "article_text",
        "meta_root": raw_root / "article_meta",
        "interim_root": interim_root,
        "logs_root": logs_root,
        "metadata_root": metadata_root,
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    paths["article_index_csv"] = paths["index_root"] / "article_index.csv"
    paths["long_csv"] = interim_root / "moa_xm_weekly_indicators_long.csv"
    paths["wide_csv"] = interim_root / "moa_xm_weekly_prices.csv"
    return paths


def init_logger(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("moa_xm_weekly")
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


def compact_text(text: str) -> str:
    return re.sub(r"\s+", "", normalize_text(text))


def normalize_label(text: str) -> str:
    return re.sub(r"\s+", "", str(text).replace("月份", "月").replace("最后1周", "最后一周")).strip()


def rel_path(path: Path, project_root: Path) -> str:
    return path.relative_to(project_root).as_posix()


def build_list_page_url(page_no: int) -> str:
    if page_no == 1:
        return urljoin(BASE_LIST_URL, "index.htm")
    return urljoin(BASE_LIST_URL, f"index_{page_no - 1}.htm")


def fetch_html(session: requests.Session, url: str) -> str:
    response = session.get(url, timeout=30)
    response.raise_for_status()
    response.encoding = response.apparent_encoding or response.encoding or "utf-8"
    return response.text


def title_match(title: str) -> bool:
    return bool(TITLE_PATTERN.fullmatch(title.strip()))


def extract_week_label(title: str) -> str | None:
    match = TITLE_PATTERN.fullmatch(title.strip())
    if not match:
        return None
    return f"{int(match.group(1))}月{match.group(2).replace('最后1周', '最后一周')}"


def parse_list_page(html: str, page_url: str, page_no: int, discovered_at: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for anchor in soup.find_all("a", href=True):
        title = anchor.get_text(" ", strip=True).strip()
        href_raw = anchor["href"].strip()
        if not title or "畜产品和饲料集贸市场价格情况" not in title:
            continue
        if not ARTICLE_HREF_PATTERN.match(href_raw):
            continue
        article_url = urljoin(page_url, href_raw)
        pair = (article_url, title)
        if pair in seen:
            continue
        seen.add(pair)
        rows.append(
            {
                "list_page_url": page_url,
                "page_no": page_no,
                "article_title": title,
                "article_href_raw": href_raw,
                "article_url": article_url,
                "title_match_flag": title_match(title),
                "discovered_at": discovered_at,
            }
        )
    return rows


def article_id_from_url(article_url: str) -> str:
    match = re.search(r"/(t\d{8}_\d+)\.htm$", urlparse(article_url).path)
    return match.group(1) if match else re.sub(r"[^0-9A-Za-z_-]+", "_", article_url)


def extract_article_meta(soup: BeautifulSoup, article_url: str) -> dict[str, Any]:
    title = None
    heading = soup.find(["h1", "h2"])
    if heading:
        title = heading.get_text(" ", strip=True)
    if not title:
        title_text = soup.title.get_text(strip=True) if soup.title else ""
        title = title_text.split("_", 1)[-1].strip()

    publish_date = None
    publish_meta = soup.find("meta", attrs={"name": "publishdate"})
    if publish_meta and publish_meta.get("content"):
        publish_date = publish_meta["content"][:10]
    if not publish_date:
        page_text = soup.get_text("\n", strip=True)
        match = re.search(r"日期[:：]\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", page_text)
        if match:
            publish_date = match.group(1)

    source = None
    source_meta = soup.find("meta", attrs={"name": "source"})
    if source_meta and source_meta.get("content"):
        source = source_meta["content"].strip()
    if not source:
        page_text = soup.get_text("\n", strip=True)
        match = re.search(r"来源[:：]\s*([^\n]+)", page_text)
        if match:
            source = match.group(1).strip()

    content_node = None
    for selector in [".TRS_Editor", "#zoom", ".content", ".fontzoom"]:
        node = soup.select_one(selector)
        if node and "畜产品和饲料集贸市场价格情况" in node.get_text(" ", strip=True):
            continue
        if node and len(node.get_text(" ", strip=True)) > 120:
            content_node = node
            break
    if content_node is None:
        tables = soup.find_all("table")
        if tables:
            content_node = soup
    content_html = str(content_node) if content_node else ""
    content_text = normalize_text(content_node.get_text("\n", strip=True) if content_node else soup.get_text("\n", strip=True))

    return {
        "article_url": article_url,
        "title": title,
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
    except Exception:
        return []


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, float):
        return None if math.isnan(value) else round(value, 6)
    text = str(value).strip()
    if not text or text == "—":
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text.replace(",", ""))
    if not match:
        return None
    return round(float(match.group()), 6)


def normalize_unit(unit: str | None) -> str | None:
    if not unit:
        return None
    unit = unit.replace("／", "/").replace(" ", "")
    if "元/公斤" in unit and "元/只" in unit:
        return "元/公斤"
    return unit


def parse_table_indicators(content_html: str) -> tuple[list[dict[str, Any]], str | None]:
    long_rows: list[dict[str, Any]] = []
    table_unit: str | None = None
    for table_idx, df in enumerate(read_tables(content_html), start=1):
        working = df.fillna("")
        flat = " ".join(working.astype(str).to_numpy().flatten())
        unit_match = re.search(r"单位[:：]\s*([^ ]+)", flat.replace("／", "/"))
        if unit_match:
            table_unit = normalize_unit(unit_match.group(1))

        header_row = None
        for idx in range(min(5, len(working))):
            row_text = "".join(working.iloc[idx].astype(str).tolist())
            if "项目" in row_text and "本周" in row_text:
                header_row = idx
                break
        if header_row is None:
            continue

        data_rows = working.iloc[header_row + 1 :]
        for _, row in data_rows.iterrows():
            cells = [str(cell).strip() for cell in row.tolist()]
            if not any(cells):
                continue
            indicator_name = cells[0].strip()
            value = None
            for cell in cells[1:]:
                value = to_float(cell)
                if value is not None:
                    break
            if value is None:
                continue
            long_rows.append(
                {
                    "table_index": table_idx,
                    "indicator_name_raw": indicator_name,
                    "indicator_name_normalized": normalize_label(indicator_name),
                    "indicator_value": value,
                    "indicator_value_raw": cells[1] if len(cells) > 1 else None,
                    "indicator_unit": table_unit or "元/公斤",
                }
            )
    return long_rows, table_unit


def parse_regex_values(text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    compact = compact_text(text)
    patterns = {
        "活猪": r"全国活猪(?:平均)?价格[^0-9]{0,20}([0-9.]+)元/公斤",
        "生猪": r"全国生猪(?:平均)?价格[^0-9]{0,20}([0-9.]+)元/公斤",
        "玉米": r"全国玉米(?:平均)?价格[^0-9]{0,20}([0-9.]+)元/公斤",
        "豆粕": r"全国豆粕(?:平均)?价格[^0-9]{0,20}([0-9.]+)元/公斤",
    }
    for label, pattern in patterns.items():
        match = re.search(pattern, compact)
        if match:
            rows.append(
                {
                    "table_index": 0,
                    "indicator_name_raw": label,
                    "indicator_name_normalized": normalize_label(label),
                    "indicator_value": to_float(match.group(1)),
                    "indicator_value_raw": match.group(1),
                    "indicator_unit": "元/公斤",
                }
            )
    return rows


def pick_indicator(rows: list[dict[str, Any]], labels: list[str]) -> dict[str, Any] | None:
    normalized_labels = {normalize_label(label) for label in labels}
    for row in rows:
        if row["indicator_name_normalized"] in normalized_labels:
            return row
    return None


def parse_collect_date(text: str, publish_date: str | None) -> str | None:
    match = re.search(r"采集日为(\d{1,2})月(\d{1,2})日", compact_text(text))
    if not match or not publish_date:
        return None
    return f"{publish_date[:4]}-{int(match.group(1)):02d}-{int(match.group(2)):02d}"


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def dump_json(path: Path, payload: Any) -> None:
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)


def summarize(index_rows: list[dict[str, Any]], wide_rows: list[dict[str, Any]], failed_urls: list[str], page_count: int) -> dict[str, Any]:
    dates = sorted(row["publish_date"] for row in wide_rows if row.get("publish_date"))
    review_rows = [
        {
            "article_url": row["article_url"],
            "title": row["title"],
            "publish_date": row["publish_date"],
            "validation_flag": row["validation_flag"],
            "validation_notes": row["validation_notes"],
        }
        for row in wide_rows
        if row["validation_flag"] != "ok" or row["validation_notes"]
    ]
    return {
        "list_pages_visited": page_count,
        "articles_discovered": len(index_rows),
        "target_articles_matched": sum(1 for row in index_rows if row["title_match_flag"]),
        "articles_processed": len(wide_rows),
        "failed_urls": failed_urls,
        "time_coverage": {
            "start": dates[0] if dates else None,
            "end": dates[-1] if dates else None,
        },
        "live_hog_price_non_null_count": sum(1 for row in wide_rows if row.get("live_hog_price") is not None),
        "corn_price_non_null_count": sum(1 for row in wide_rows if row.get("corn_price") is not None),
        "soymeal_price_non_null_count": sum(1 for row in wide_rows if row.get("soymeal_price") is not None),
        "manual_review_rows": review_rows,
    }


def crawl(args: argparse.Namespace) -> dict[str, Any]:
    project_root = Path(__file__).resolve().parents[1]
    paths = init_paths(project_root)
    log_path = paths["logs_root"] / f"moa_xm_weekly_crawl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = init_logger(log_path)
    session = build_session()
    discovered_at = now_iso()

    index_rows: list[dict[str, Any]] = []
    for page_no in range(1, args.end_page + 1):
        page_url = build_list_page_url(page_no)
        html = fetch_html(session, page_url)
        page_rows = parse_list_page(html, page_url, page_no, discovered_at)
        index_rows.extend(page_rows)
        logger.info("列表页解析完成：page_no=%s 目标候选=%s", page_no, len(page_rows))
        time.sleep(args.sleep_seconds)

    dedup_index = {}
    for row in index_rows:
        if row["title_match_flag"]:
            dedup_index[row["article_url"]] = row
    target_articles = list(dedup_index.values())
    if args.max_articles:
        target_articles = target_articles[: args.max_articles]
        logger.info("启用样本限制，仅抓取前 %s 篇目标文章", args.max_articles)

    write_csv(
        paths["article_index_csv"],
        index_rows,
        ["list_page_url", "page_no", "article_title", "article_href_raw", "article_url", "title_match_flag", "discovered_at"],
    )

    long_rows: list[dict[str, Any]] = []
    wide_rows: list[dict[str, Any]] = []
    failed_urls: list[str] = []

    for idx, article in enumerate(target_articles, start=1):
        article_url = article["article_url"]
        logger.info("处理文章 [%s/%s] %s", idx, len(target_articles), article_url)
        try:
            html = fetch_html(session, article_url)
            soup = BeautifulSoup(html, "lxml")
            meta = extract_article_meta(soup, article_url)

            publish_date = meta["publish_date"] or "unknown_date"
            article_id = article_id_from_url(article_url)
            file_stub = f"{publish_date}_{article_id}"
            html_path = paths["html_root"] / f"{file_stub}.html"
            text_path = paths["text_root"] / f"{file_stub}.txt"
            meta_path = paths["meta_root"] / f"{file_stub}.json"
            html_path.write_text(meta["content_html"] or html, encoding="utf-8")
            text_path.write_text(meta["content_text"], encoding="utf-8")

            table_rows, _ = parse_table_indicators(meta["content_html"])
            regex_rows = parse_regex_values(meta["content_text"])
            selected_rows = table_rows if table_rows else regex_rows
            parsing_method = "table" if table_rows else "regex" if regex_rows else "fallback"

            for row in selected_rows:
                long_rows.append(
                    {
                        "article_url": article_url,
                        "title": meta["title"],
                        "publish_date": meta["publish_date"],
                        "week_label": extract_week_label(meta["title"]),
                        "collect_date": parse_collect_date(meta["content_text"], meta["publish_date"]),
                        "table_index": row["table_index"],
                        "indicator_name_raw": row["indicator_name_raw"],
                        "indicator_name_normalized": row["indicator_name_normalized"],
                        "indicator_value_raw": row["indicator_value_raw"],
                        "indicator_value": row["indicator_value"],
                        "indicator_unit": row["indicator_unit"],
                        "parsing_method": parsing_method,
                        "raw_text_path": rel_path(text_path, project_root),
                        "raw_html_path": rel_path(html_path, project_root),
                    }
                )

            live_hog = pick_indicator(selected_rows, TARGET_LABELS["live_hog_price"])
            corn = pick_indicator(selected_rows, TARGET_LABELS["corn_price"])
            soymeal = pick_indicator(selected_rows, TARGET_LABELS["soymeal_price"])

            missing_core = [field for field, row in [("live_hog_price", live_hog), ("corn_price", corn), ("soymeal_price", soymeal)] if row is None]
            validation_flag = "missing_core_fields" if missing_core else "ok"
            validation_notes = []
            if missing_core:
                validation_notes.append(f"missing_core_fields:{','.join(missing_core)}")
            collect_date = parse_collect_date(meta["content_text"], meta["publish_date"])
            if not collect_date:
                validation_notes.append("collect_date_missing")

            wide_row = {
                "article_url": article_url,
                "title": meta["title"],
                "publish_date": meta["publish_date"],
                "week_label": extract_week_label(meta["title"]),
                "collect_date": collect_date,
                "live_hog_price": live_hog["indicator_value"] if live_hog else None,
                "live_hog_price_unit": live_hog["indicator_unit"] if live_hog else None,
                "corn_price": corn["indicator_value"] if corn else None,
                "corn_price_unit": corn["indicator_unit"] if corn else None,
                "soymeal_price": soymeal["indicator_value"] if soymeal else None,
                "soymeal_price_unit": soymeal["indicator_unit"] if soymeal else None,
                "parsing_method": parsing_method,
                "raw_text_path": rel_path(text_path, project_root),
                "raw_html_path": rel_path(html_path, project_root),
                "validation_flag": validation_flag,
                "validation_notes": "; ".join(validation_notes),
            }
            wide_rows.append(wide_row)

            dump_json(
                meta_path,
                {
                    "article_url": article_url,
                    "title": meta["title"],
                    "publish_date": meta["publish_date"],
                    "week_label_from_title": extract_week_label(meta["title"]),
                    "source_name": "农业农村部网站",
                    "source_channel": "公开/监测预警/畜牧",
                    "fetched_at": now_iso(),
                    "html_path": rel_path(html_path, project_root),
                    "text_path": rel_path(text_path, project_root),
                    "parse_status": validation_flag,
                    "notes": wide_row["validation_notes"],
                },
            )
        except Exception as exc:
            failed_urls.append(article_url)
            logger.exception("文章处理失败：%s error=%s", article_url, exc)
        time.sleep(args.sleep_seconds)

    write_csv(
        paths["long_csv"],
        long_rows,
        [
            "article_url",
            "title",
            "publish_date",
            "week_label",
            "collect_date",
            "table_index",
            "indicator_name_raw",
            "indicator_name_normalized",
            "indicator_value_raw",
            "indicator_value",
            "indicator_unit",
            "parsing_method",
            "raw_text_path",
            "raw_html_path",
        ],
    )
    write_csv(
        paths["wide_csv"],
        wide_rows,
        [
            "article_url",
            "title",
            "publish_date",
            "week_label",
            "collect_date",
            "live_hog_price",
            "live_hog_price_unit",
            "corn_price",
            "corn_price_unit",
            "soymeal_price",
            "soymeal_price_unit",
            "parsing_method",
            "raw_text_path",
            "raw_html_path",
            "validation_flag",
            "validation_notes",
        ],
    )

    summary = summarize(index_rows, wide_rows, failed_urls, args.end_page)
    summary.update(
        {
            "run_started_at": discovered_at,
            "run_finished_at": now_iso(),
            "article_index_csv": rel_path(paths["article_index_csv"], project_root),
            "long_csv": rel_path(paths["long_csv"], project_root),
            "wide_csv": rel_path(paths["wide_csv"], project_root),
            "log_path": rel_path(log_path, project_root),
        }
    )
    summary_path = paths["metadata_root"] / "moa_xm_weekly_summary.json"
    manual_review_path = paths["metadata_root"] / "moa_xm_weekly_manual_review.csv"
    dump_json(summary_path, summary)
    write_csv(
        manual_review_path,
        summary["manual_review_rows"],
        ["article_url", "title", "publish_date", "validation_flag", "validation_notes"],
    )
    logger.info("抓取结束：文章=%s 失败=%s", len(wide_rows), len(failed_urls))
    return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="抓取农业农村部公开栏目畜牧周报")
    parser.add_argument("--end-page", type=int, default=25, help="列表页结束页，默认 25")
    parser.add_argument("--max-articles", type=int, default=0, help="仅抓取前 N 篇文章，0 表示全量")
    parser.add_argument("--sleep-seconds", type=float, default=0.05, help="请求间隔秒数")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    summary = crawl(args)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
