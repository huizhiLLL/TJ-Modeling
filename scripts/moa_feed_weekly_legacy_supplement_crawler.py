#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


SEED_ARTICLES = [
    {"article_url": "https://www.moa.gov.cn/gk/jcyj/201603/t20160309_5045861.htm", "segment": "gk_jcyj", "discovery_note": "user_provided_example"},
    {"article_url": "https://www.moa.gov.cn/ztzl/nybrl/rlxx/201604/t20160427_5110252.htm", "segment": "nybrl_rlxx", "discovery_note": "user_provided_example"},
    {"article_url": "https://www.moa.gov.cn/ztzl/nybrl/rlxx/201606/t20160623_5184952.htm", "segment": "nybrl_rlxx", "discovery_note": "user_provided_example"},
    {"article_url": "https://www.moa.gov.cn/ztzl/nybrl/rlxx/201701/t20170112_5429594.htm", "segment": "nybrl_rlxx", "discovery_note": "user_provided_example"},
    {"article_url": "https://www.moa.gov.cn/ztzl/nybrl/rlxx/201702/t20170209_5471799.htm", "segment": "nybrl_rlxx", "discovery_note": "user_provided_example"},
    {"article_url": "https://www.moa.gov.cn/ztzl/nybrl/rlxx/201707/t20170703_5733564.htm", "segment": "nybrl_rlxx", "discovery_note": "user_provided_example"},
    {"article_url": "https://www.moa.gov.cn/ztzl/nybrl/rlxx/201708/t20170810_5780964.htm", "segment": "nybrl_rlxx", "discovery_note": "user_provided_example"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/201802/t20180207_6410340.htm", "segment": "scs_jcyj", "discovery_note": "user_provided_example"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/201807/t20180725_6410439.htm", "segment": "scs_jcyj", "discovery_note": "user_provided_example"},
    {"article_url": "https://scs.moa.gov.cn/jcyj/201812/t20181213_6410495.htm", "segment": "scs_jcyj", "discovery_note": "user_provided_example"},
]


TITLE_PATTERNS = [
    re.compile(r"^\s*(\d{1,2})月份第(\d{1,2})周畜产品和饲料集贸市场价格情况\s*$"),
    re.compile(r"^\s*(\d{1,2})月份最后(?:一|1)周畜产品和饲料集贸市场价格情况\s*$"),
]


def init_paths(project_root: Path) -> dict[str, Path]:
    raw_root = project_root / "data" / "raw" / "moa_feed_weekly_legacy_supplement"
    interim_root = project_root / "data" / "interim" / "moa_feed_weekly_legacy_supplement_parsed"
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
    paths["wide_csv"] = interim_root / "moa_feed_weekly_legacy_supplement_prices.csv"
    return paths


def init_logger(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("moa_feed_weekly_legacy_supplement")
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
    retry = Retry(total=3, connect=3, read=3, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504), allowed_methods=("GET",))
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
    text = text.replace("\u3000", " ").replace("\xa0", " ").replace("\r\n", "\n").replace("\r", "\n")
    lines = []
    for raw_line in text.split("\n"):
        line = re.sub(r"[ \t]+", " ", raw_line).strip()
        if line:
            lines.append(line)
    return "\n".join(lines)


def rel_path(path: Path, project_root: Path) -> str:
    return path.relative_to(project_root).as_posix()


def extract_week_label(title: str) -> str | None:
    for pattern in TITLE_PATTERNS:
        match = pattern.fullmatch(title)
        if match:
            if "最后" in title:
                return f"{int(match.group(1))}月最后一周"
            return f"{int(match.group(1))}月第{int(match.group(2))}周"
    return None


def to_float(value: str | None) -> float | None:
    if value is None:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", str(value))
    if not match:
        return None
    return round(float(match.group(0)), 6)


def parse_collect_date(compact: str, publish_date: str | None) -> str | None:
    match = re.search(r"采集日为(\d{1,2})月(\d{1,2})日", compact)
    if not match or not publish_date:
        return None
    return f"{publish_date[:4]}-{int(match.group(1)):02d}-{int(match.group(2)):02d}"


def parse_article(article_url: str, html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    title_node = soup.find(["h1", "h2"])
    title = title_node.get_text(" ", strip=True) if title_node else (soup.title.get_text(strip=True).split("_", 1)[-1].strip() if soup.title else "")
    publish_meta = soup.find("meta", attrs={"name": "publishdate"})
    publish_date = publish_meta.get("content", "")[:10] if publish_meta else None
    compact = re.sub(r"\s+", "", soup.get_text(" ", strip=True))

    corn = re.search(r"全国玉米平均价格([0-9.]+)元/公斤", compact)
    soy = re.search(r"全国豆粕平均价格([0-9.]+)元/公斤", compact)

    return {
        "article_url": article_url,
        "title": title,
        "publish_date": publish_date,
        "week_label": extract_week_label(title),
        "collect_date": parse_collect_date(compact, publish_date),
        "corn_price": to_float(corn.group(1)) if corn else None,
        "corn_price_unit": "元/公斤" if corn else None,
        "soymeal_price": to_float(soy.group(1)) if soy else None,
        "soymeal_price_unit": "元/公斤" if soy else None,
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


def summarize(index_rows: list[dict[str, Any]], rows: list[dict[str, Any]], failed_urls: list[str]) -> dict[str, Any]:
    dates = sorted(row["publish_date"] for row in rows if row.get("publish_date"))
    review_rows = [
        {
            "article_url": row["article_url"],
            "title": row["title"],
            "publish_date": row["publish_date"],
            "validation_flag": row["validation_flag"],
            "validation_notes": row["validation_notes"],
        }
        for row in rows
        if row["validation_flag"] != "ok" or row["validation_notes"]
    ]
    return {
        "seed_article_count": len(index_rows),
        "articles_processed": len(rows),
        "failed_urls": failed_urls,
        "time_coverage": {"start": dates[0] if dates else None, "end": dates[-1] if dates else None},
        "corn_price_non_null_count": sum(1 for row in rows if row.get("corn_price") is not None),
        "soymeal_price_non_null_count": sum(1 for row in rows if row.get("soymeal_price") is not None),
        "manual_review_rows": review_rows,
    }


def crawl(args: argparse.Namespace) -> dict[str, Any]:
    project_root = Path(__file__).resolve().parents[1]
    paths = init_paths(project_root)
    log_path = paths["logs_root"] / f"moa_feed_weekly_legacy_supplement_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = init_logger(log_path)
    session = build_session()
    started_at = now_iso()

    index_rows = SEED_ARTICLES[: args.max_articles] if args.max_articles else SEED_ARTICLES[:]
    write_csv(paths["article_index_csv"], index_rows, ["article_url", "segment", "discovery_note"])

    rows: list[dict[str, Any]] = []
    failed_urls: list[str] = []

    for idx, seed in enumerate(index_rows, start=1):
        article_url = seed["article_url"]
        logger.info("处理文章 [%s/%s] %s", idx, len(index_rows), article_url)
        try:
            response = session.get(article_url, timeout=30)
            response.raise_for_status()
            response.encoding = response.apparent_encoding or response.encoding or "utf-8"
            html = response.text
            parsed = parse_article(article_url, html)

            file_stub = f"{parsed['publish_date'] or 'unknown'}_{idx:03d}"
            html_path = paths["html_root"] / f"{file_stub}.html"
            text_path = paths["text_root"] / f"{file_stub}.txt"
            meta_path = paths["meta_root"] / f"{file_stub}.json"
            html_path.write_text(html, encoding="utf-8")
            text_path.write_text(normalize_text(BeautifulSoup(html, "lxml").get_text("\n", strip=True)), encoding="utf-8")

            missing = [field for field in ["corn_price", "soymeal_price"] if parsed.get(field) is None]
            validation_flag = "missing_core_fields" if missing else "ok"
            validation_notes = []
            if missing:
                validation_notes.append(f"missing:{','.join(missing)}")
            if not parsed.get("collect_date"):
                validation_notes.append("collect_date_missing")

            row = {
                **parsed,
                "source_segment": seed["segment"],
                "raw_html_path": rel_path(html_path, project_root),
                "raw_text_path": rel_path(text_path, project_root),
                "parsing_method": "regex_text",
                "validation_flag": validation_flag,
                "validation_notes": "; ".join(validation_notes),
            }
            rows.append(row)

            dump_json(
                meta_path,
                {
                    "article_url": article_url,
                    "title": parsed["title"],
                    "publish_date": parsed["publish_date"],
                    "week_label": parsed["week_label"],
                    "source_segment": seed["segment"],
                    "fetched_at": now_iso(),
                    "html_path": rel_path(html_path, project_root),
                    "text_path": rel_path(text_path, project_root),
                    "parse_status": validation_flag,
                    "notes": row["validation_notes"],
                },
            )
        except Exception as exc:
            failed_urls.append(article_url)
            logger.exception("文章处理失败：%s error=%s", article_url, exc)
        time.sleep(args.sleep_seconds)

    rows.sort(key=lambda row: (row["publish_date"] or "", row["article_url"]))
    write_csv(
        paths["wide_csv"],
        rows,
        [
            "article_url",
            "title",
            "publish_date",
            "week_label",
            "collect_date",
            "corn_price",
            "corn_price_unit",
            "soymeal_price",
            "soymeal_price_unit",
            "source_segment",
            "raw_html_path",
            "raw_text_path",
            "parsing_method",
            "validation_flag",
            "validation_notes",
        ],
    )

    summary = summarize(index_rows, rows, failed_urls)
    summary.update(
        {
            "run_started_at": started_at,
            "run_finished_at": now_iso(),
            "article_index_csv": rel_path(paths["article_index_csv"], project_root),
            "wide_csv": rel_path(paths["wide_csv"], project_root),
            "log_path": rel_path(log_path, project_root),
        }
    )
    summary_path = paths["metadata_root"] / "moa_feed_weekly_legacy_supplement_summary.json"
    review_path = paths["metadata_root"] / "moa_feed_weekly_legacy_supplement_manual_review.csv"
    dump_json(summary_path, summary)
    write_csv(
        review_path,
        summary["manual_review_rows"],
        ["article_url", "title", "publish_date", "validation_flag", "validation_notes"],
    )
    logger.info("抓取结束：文章=%s 失败=%s", len(rows), len(failed_urls))
    return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="抓取 2016-2018 农业农村部周报补充段")
    parser.add_argument("--max-articles", type=int, default=0, help="仅处理前 N 篇，0 表示全量")
    parser.add_argument("--sleep-seconds", type=float, default=0.02, help="请求间隔秒数")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    summary = crawl(args)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
