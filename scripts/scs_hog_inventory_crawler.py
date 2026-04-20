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
    {
        "title": "2018年1月份400个监测县生猪存栏信息",
        "article_url": "https://scs.moa.gov.cn/jcyj/201802/t20180213_6410348.htm",
        "discovery_query": "2018年1—6月份 400个监测县 生猪存栏信息 site:scs.moa.gov.cn/jcyj",
    },
    {
        "title": "2018年2月份生猪存栏信息",
        "article_url": "https://scs.moa.gov.cn/jcyj/201803/t20180315_6410360.htm",
        "discovery_query": "2018年2月份 生猪存栏信息 site:scs.moa.gov.cn/jcyj",
    },
    {
        "title": "2018年1—3月份400个监测县生猪存栏信息",
        "article_url": "https://scs.moa.gov.cn/jcyj/201804/t20180418_6410380.htm",
        "discovery_query": "2018年1—3月份 400个监测县 生猪存栏信息 site:scs.moa.gov.cn/jcyj",
    },
    {
        "title": "2018年4月份生猪存栏信息",
        "article_url": "https://www.scs.moa.gov.cn/jcyj/201805/t20180515_6410397.htm",
        "discovery_query": "2018年4月份 生猪存栏信息 site:scs.moa.gov.cn/jcyj",
    },
    {
        "title": "2018年5月份400个监测县生猪存栏信息",
        "article_url": "https://www.scs.moa.gov.cn/jcyj/201806/t20180615_6410418.htm",
        "discovery_query": "2018年5月份 生猪存栏信息 site:scs.moa.gov.cn/jcyj",
    },
    {
        "title": "2018年6月份400个监测县生猪存栏信息",
        "article_url": "https://scs.moa.gov.cn/jcyj/201807/t20180719_6410437.htm",
        "discovery_query": "2018年6月份 生猪存栏信息 site:scs.moa.gov.cn/jcyj",
    },
    {
        "title": "2018年7月份400个监测县生猪存栏信息",
        "article_url": "https://www.scs.moa.gov.cn/jcyj/201808/t20180816_6410452.htm",
        "discovery_query": "2018年7月份 生猪存栏信息 site:scs.moa.gov.cn/jcyj",
    },
    {
        "title": "2018年8月份400个监测县生猪存栏信息",
        "article_url": "https://scs.moa.gov.cn/jcyj/201809/t20180919_6410464.htm",
        "discovery_query": "2018年8月份 生猪存栏信息 site:scs.moa.gov.cn/jcyj",
    },
    {
        "title": "2018年9月份生猪存栏信息",
        "article_url": "https://scs.moa.gov.cn/jcyj/201810/t20181017_6410470.htm",
        "discovery_query": "2018年9月份 生猪存栏信息 site:scs.moa.gov.cn/jcyj",
    },
    {
        "title": "2018年11月份400个监测县生猪存栏信息",
        "article_url": "https://www.scs.moa.gov.cn/jcyj/201812/t20181214_6410497.htm",
        "discovery_query": "2018年11月份 生猪存栏信息 site:scs.moa.gov.cn/jcyj",
    },
    {
        "title": "2018年12月份400个监测县生猪存栏信息",
        "article_url": "https://scs.moa.gov.cn/jcyj/201901/t20190115_6410508.htm",
        "discovery_query": "2018年12月份 生猪存栏信息 site:scs.moa.gov.cn/jcyj",
    },
    {
        "title": "2019年1月份400个监测县生猪存栏信息",
        "article_url": "https://scs.moa.gov.cn/jcyj/201902/t20190219_6410534.htm",
        "discovery_query": "2019年1月份 400个监测县 生猪存栏信息 site:scs.moa.gov.cn/jcyj",
    },
    {
        "title": "2019年2月份400个监测县生猪存栏信息",
        "article_url": "https://www.scs.moa.gov.cn/jcyj/201903/t20190315_6410553.htm",
        "discovery_query": "2019年2月份 生猪存栏信息 site:scs.moa.gov.cn/jcyj",
    },
    {
        "title": "2019年3月份400个监测县生猪存栏信息",
        "article_url": "https://scs.moa.gov.cn/jcyj/201904/t20190412_6410579.htm",
        "discovery_query": "2019年3月份 生猪存栏信息 site:scs.moa.gov.cn/jcyj",
    },
    {
        "title": "2019年4月份400个监测县生猪存栏信息",
        "article_url": "https://scs.moa.gov.cn/jcyj/201905/t20190515_6410605.htm",
        "discovery_query": "2019年4月份 生猪存栏信息 site:scs.moa.gov.cn/jcyj",
    },
    {
        "title": "2019年5月份400个监测县生猪存栏信息",
        "article_url": "https://scs.moa.gov.cn/jcyj/201906/t20190612_6410627.htm",
        "discovery_query": "2019年5月份 生猪存栏信息 site:scs.moa.gov.cn/jcyj",
    },
    {
        "title": "2019年8月份400个监测县生猪存栏信息",
        "article_url": "https://scs.moa.gov.cn/jcyj/201909/t20190912_6410693.htm",
        "discovery_query": "2019年8月份 生猪存栏信息 site:scs.moa.gov.cn/jcyj",
    },
    {
        "title": "2019年10月份400个监测县生猪存栏信息",
        "article_url": "https://scs.moa.gov.cn/jcyj/201911/t20191129_6410753.htm",
        "discovery_query": "2019年10月份 生猪存栏信息 site:scs.moa.gov.cn/jcyj",
    },
]


TITLE_PATTERN = re.compile(r"^(201[89])年(\d{1,2}|1—3)月份(400个监测县)?生猪存栏信息$")


def init_paths(project_root: Path) -> dict[str, Path]:
    raw_root = project_root / "data" / "raw" / "scs_hog_inventory_articles"
    interim_root = project_root / "data" / "interim" / "scs_hog_inventory_parsed"
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
    paths["wide_csv"] = interim_root / "scs_hog_inventory_changes.csv"
    return paths


def init_logger(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("scs_hog_inventory")
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


def parse_title(title: str) -> dict[str, Any]:
    match = TITLE_PATTERN.fullmatch(title)
    if not match:
        return {
            "report_year": None,
            "report_month": None,
            "report_period": None,
            "period_type": None,
            "sample_scope": None,
        }
    year = int(match.group(1))
    month_or_range = match.group(2)
    sample_scope = "400个监测县" if match.group(3) else "unknown_scope"
    if "—" in month_or_range:
        return {
            "report_year": year,
            "report_month": None,
            "report_period": f"{year}年{month_or_range}月份",
            "period_type": "quarter_range",
            "sample_scope": sample_scope,
        }
    month = int(month_or_range)
    return {
        "report_year": year,
        "report_month": month,
        "report_period": f"{year}-{month:02d}",
        "period_type": "month",
        "sample_scope": sample_scope,
    }


def to_percent(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace("％", "%")
    match = re.search(r"(-?\d+(?:\.\d+)?)%", text)
    if not match:
        return None
    return round(float(match.group(1)) / 100, 6)


def normalize_seed_url(url: str) -> str:
    return url.replace("https://www.scs.moa.gov.cn/", "https://scs.moa.gov.cn/")


def parse_article(article_url: str, html: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    soup = BeautifulSoup(html, "lxml")
    title_node = soup.find(["h1", "h2"])
    title = title_node.get_text(" ", strip=True) if title_node else (soup.title.get_text(strip=True) if soup.title else "")
    publish_meta = soup.find("meta", attrs={"name": "publishdate"})
    publish_date = publish_meta.get("content", "")[:10] if publish_meta else None

    title_meta = parse_title(title)

    rows = []
    for table in soup.find_all("table"):
        texts = [t.strip() for t in table.stripped_strings if t.strip()]
        if "生猪存栏" in texts and "能繁母猪存栏" in texts and "比上月增减" in texts and "比去年同期增减" in texts:
            rows = texts
            break

    quarter_rows = []
    for table in soup.find_all("table"):
        texts = [t.strip() for t in table.stripped_strings if t.strip()]
        if "生猪存栏环比" in texts and "生猪存栏同比" in texts and "能繁母猪存栏环比" in texts and "能繁母猪存栏同比" in texts:
            quarter_rows = texts
            break

    parsed_rows: list[dict[str, Any]] = []
    if rows:
        hog_mom = hog_yoy = sow_mom = sow_yoy = None
        try:
            idx_mom = rows.index("比上月增减")
            idx_yoy = rows.index("比去年同期增减")
            hog_mom = to_percent(rows[idx_mom + 1]) if idx_mom + 2 < len(rows) else None
            sow_mom = to_percent(rows[idx_mom + 2]) if idx_mom + 2 < len(rows) else None
            hog_yoy = to_percent(rows[idx_yoy + 1]) if idx_yoy + 2 < len(rows) else None
            sow_yoy = to_percent(rows[idx_yoy + 2]) if idx_yoy + 2 < len(rows) else None
        except Exception:
            pass
        parsed_rows.append(
            {
                **title_meta,
                "hog_inventory_mom_pct": hog_mom,
                "hog_inventory_yoy_pct": hog_yoy,
                "breeding_sow_inventory_mom_pct": sow_mom,
                "breeding_sow_inventory_yoy_pct": sow_yoy,
                "sub_period_label": None,
            }
        )
    elif quarter_rows and title_meta["report_year"]:
        try:
            start = quarter_rows.index("生猪存栏环比")
            # Typical order after headers: 1, 月, —, -0.1%, —, -1.2%, 2, 月, ...
            data_tokens = quarter_rows[start + 4 :]
            pointer = 0
            while pointer + 5 < len(data_tokens):
                month_token = data_tokens[pointer]
                if not re.fullmatch(r"\d{1,2}", month_token):
                    pointer += 1
                    continue
                month = int(month_token)
                parsed_rows.append(
                    {
                        "report_year": title_meta["report_year"],
                        "report_month": month,
                        "report_period": f"{title_meta['report_year']}-{month:02d}",
                        "period_type": "month_from_quarter_range",
                        "sample_scope": title_meta["sample_scope"],
                        "hog_inventory_mom_pct": to_percent(data_tokens[pointer + 2]),
                        "hog_inventory_yoy_pct": to_percent(data_tokens[pointer + 3]),
                        "breeding_sow_inventory_mom_pct": to_percent(data_tokens[pointer + 4]),
                        "breeding_sow_inventory_yoy_pct": to_percent(data_tokens[pointer + 5]),
                        "sub_period_label": f"{month}月",
                    }
                )
                pointer += 6
        except Exception:
            pass

    meta = {
        "article_url": article_url,
        "title": title,
        "publish_date": publish_date,
    }
    return meta, parsed_rows


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def dump_json(path: Path, payload: Any) -> None:
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)


def summarize(index_rows: list[dict[str, Any]], wide_rows: list[dict[str, Any]], failed_urls: list[str]) -> dict[str, Any]:
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
        if row["validation_flag"] != "ok"
    ]
    return {
        "seed_article_count": len(index_rows),
        "articles_processed": len(wide_rows),
        "failed_urls": failed_urls,
        "time_coverage": {
            "start": dates[0] if dates else None,
            "end": dates[-1] if dates else None,
        },
        "hog_inventory_mom_non_null_count": sum(1 for row in wide_rows if row.get("hog_inventory_mom_pct") is not None),
        "hog_inventory_yoy_non_null_count": sum(1 for row in wide_rows if row.get("hog_inventory_yoy_pct") is not None),
        "breeding_sow_inventory_mom_non_null_count": sum(1 for row in wide_rows if row.get("breeding_sow_inventory_mom_pct") is not None),
        "breeding_sow_inventory_yoy_non_null_count": sum(1 for row in wide_rows if row.get("breeding_sow_inventory_yoy_pct") is not None),
        "manual_review_rows": review_rows,
    }


def crawl(args: argparse.Namespace) -> dict[str, Any]:
    project_root = Path(__file__).resolve().parents[1]
    paths = init_paths(project_root)
    log_path = paths["logs_root"] / f"scs_hog_inventory_crawl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = init_logger(log_path)
    session = build_session()
    started_at = now_iso()

    index_rows = SEED_ARTICLES[: args.max_articles] if args.max_articles else SEED_ARTICLES[:]
    write_csv(
        paths["article_index_csv"],
        index_rows,
        ["title", "article_url", "discovery_query"],
    )

    wide_rows: list[dict[str, Any]] = []
    failed_urls: list[str] = []

    for idx, seed in enumerate(index_rows, start=1):
        article_url = normalize_seed_url(seed["article_url"])
        logger.info("处理文章 [%s/%s] %s", idx, len(index_rows), article_url)
        try:
            response = session.get(article_url, timeout=30)
            response.raise_for_status()
            response.encoding = response.apparent_encoding or response.encoding or "utf-8"
            html = response.text

            meta, parsed_rows = parse_article(article_url, html)
            file_stub = f"{meta['publish_date'] or 'unknown'}_{idx:02d}"
            html_path = paths["html_root"] / f"{file_stub}.html"
            text_path = paths["text_root"] / f"{file_stub}.txt"
            meta_path = paths["meta_root"] / f"{file_stub}.json"
            html_path.write_text(html, encoding="utf-8")
            text_path.write_text(normalize_text(BeautifulSoup(html, "lxml").get_text("\n", strip=True)), encoding="utf-8")

            if not parsed_rows:
                parsed_rows = [
                    {
                        "report_year": None,
                        "report_month": None,
                        "report_period": None,
                        "period_type": None,
                        "sample_scope": None,
                        "hog_inventory_mom_pct": None,
                        "hog_inventory_yoy_pct": None,
                        "breeding_sow_inventory_mom_pct": None,
                        "breeding_sow_inventory_yoy_pct": None,
                        "sub_period_label": None,
                    }
                ]

            article_validation_notes = []
            article_validation_flag = "ok"
            for parsed in parsed_rows:
                missing_fields = [
                    field
                    for field in [
                        "hog_inventory_mom_pct",
                        "hog_inventory_yoy_pct",
                        "breeding_sow_inventory_mom_pct",
                        "breeding_sow_inventory_yoy_pct",
                    ]
                    if parsed.get(field) is None
                ]
                validation_flag = "missing_metrics" if missing_fields else "ok"
                validation_notes = f"missing:{','.join(missing_fields)}" if missing_fields else ""
                if validation_flag != "ok":
                    article_validation_flag = "missing_metrics"
                    article_validation_notes.append(validation_notes)

                row = {
                    "article_url": article_url,
                    "title": meta["title"],
                    "publish_date": meta["publish_date"],
                    **parsed,
                    "raw_html_path": rel_path(html_path, project_root),
                    "raw_text_path": rel_path(text_path, project_root),
                    "validation_flag": validation_flag,
                    "validation_notes": validation_notes,
                }
                wide_rows.append(row)

            dump_json(
                meta_path,
                {
                    "article_url": article_url,
                    "title": meta["title"],
                    "publish_date": meta["publish_date"],
                    "report_periods": [row.get("report_period") for row in parsed_rows],
                    "period_types": [row.get("period_type") for row in parsed_rows],
                    "sample_scopes": [row.get("sample_scope") for row in parsed_rows],
                    "fetched_at": now_iso(),
                    "html_path": rel_path(html_path, project_root),
                    "text_path": rel_path(text_path, project_root),
                    "parse_status": article_validation_flag,
                    "notes": "; ".join([note for note in article_validation_notes if note]),
                    "discovery_query": seed["discovery_query"],
                },
            )
        except Exception as exc:
            failed_urls.append(article_url)
            logger.exception("文章处理失败：%s error=%s", article_url, exc)
        time.sleep(args.sleep_seconds)

    wide_rows.sort(key=lambda row: (row["publish_date"] or "", row["article_url"]))
    write_csv(
        paths["wide_csv"],
        wide_rows,
        [
            "article_url",
            "title",
            "publish_date",
            "report_period",
            "report_year",
            "report_month",
            "period_type",
            "sample_scope",
            "sub_period_label",
            "hog_inventory_mom_pct",
            "hog_inventory_yoy_pct",
            "breeding_sow_inventory_mom_pct",
            "breeding_sow_inventory_yoy_pct",
            "raw_html_path",
            "raw_text_path",
            "validation_flag",
            "validation_notes",
        ],
    )

    summary = summarize(index_rows, wide_rows, failed_urls)
    summary.update(
        {
            "run_started_at": started_at,
            "run_finished_at": now_iso(),
            "article_index_csv": rel_path(paths["article_index_csv"], project_root),
            "wide_csv": rel_path(paths["wide_csv"], project_root),
            "log_path": rel_path(log_path, project_root),
        }
    )
    summary_path = paths["metadata_root"] / "scs_hog_inventory_summary.json"
    review_path = paths["metadata_root"] / "scs_hog_inventory_manual_review.csv"
    dump_json(summary_path, summary)
    write_csv(
        review_path,
        summary["manual_review_rows"],
        ["article_url", "title", "publish_date", "validation_flag", "validation_notes"],
    )
    logger.info("抓取结束：文章=%s 失败=%s", len(wide_rows), len(failed_urls))
    return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="抓取市场与信息化司生猪存栏信息变化率文章")
    parser.add_argument("--max-articles", type=int, default=0, help="仅处理前 N 篇 seed 文章，0 表示全量")
    parser.add_argument("--sleep-seconds", type=float, default=0.05, help="请求间隔秒数")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    summary = crawl(args)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
