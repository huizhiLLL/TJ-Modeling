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
from urllib.parse import quote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


SEARCH_KEYWORD = "河南省瘦肉型肉猪(110kg左右)价格"
SEARCH_BASE_URL = "https://www.soozhu.com/site/search/"
ARTICLE_BASE_URL = "https://www.soozhu.com"


def init_paths(project_root: Path) -> dict[str, Path]:
    raw_root = project_root / "data" / "raw" / "soozhu_henan_hog_daily"
    interim_root = project_root / "data" / "interim" / "soozhu_henan_hog_daily_parsed"
    logs_root = project_root / "data" / "logs"
    metadata_root = project_root / "data" / "metadata"
    paths = {
        "raw_root": raw_root,
        "search_root": raw_root / "search_pages",
        "html_root": raw_root / "article_html",
        "text_root": raw_root / "article_text",
        "meta_root": raw_root / "article_meta",
        "interim_root": interim_root,
        "logs_root": logs_root,
        "metadata_root": metadata_root,
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    paths["search_index_csv"] = paths["search_root"] / "search_index.csv"
    paths["article_daily_csv"] = interim_root / "soozhu_henan_hog_daily_prices.csv"
    paths["local_detail_csv"] = interim_root / "soozhu_henan_hog_local_prices.csv"
    return paths


def init_logger(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("soozhu_henan_hog_daily")
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


def search_page_url(page_no: int) -> str:
    return (
        f"{SEARCH_BASE_URL}?key={quote(SEARCH_KEYWORD)}"
        f"&PageNo={page_no}&ListCountPerPage=20&timestamp={int(time.time()*1000)}"
    )


def bootstrap_search(session: requests.Session) -> None:
    url = f"{SEARCH_BASE_URL}?key={quote(SEARCH_KEYWORD)}"
    response = session.get(url, timeout=30)
    response.raise_for_status()


def fetch_search_json(session: requests.Session, page_no: int) -> dict[str, Any]:
    url = search_page_url(page_no)
    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": f"{SEARCH_BASE_URL}?key={quote(SEARCH_KEYWORD)}",
    }
    response = session.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def article_id_from_url(article_url: str) -> str:
    match = re.search(r"/article/(\d+)/", article_url)
    return match.group(1) if match else re.sub(r"[^0-9A-Za-z_-]+", "_", article_url)


def to_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text.replace(",", ""))
    if not match:
        return None
    return round(float(match.group()), 6)


def to_percent(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace("％", "%")
    if not text:
        return None
    match = re.search(r"(-?\d+(?:\.\d+)?)%", text)
    if match:
        return round(float(match.group(1)) / 100, 6)
    return to_float(text)


def parse_article(article_url: str, html: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    soup = BeautifulSoup(html, "lxml")
    title_node = soup.find("h1")
    title = title_node.get_text(" ", strip=True) if title_node else (soup.title.get_text(strip=True) if soup.title else "")

    text_nodes = list(soup.stripped_strings)
    publish_datetime = None
    for text in text_nodes:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}", text):
            publish_datetime = text
            break

    avg_price = None
    last_year_price = None
    mom_pct = None
    yoy_pct = None
    if "日均价格" in text_nodes and "各地均价：" in text_nodes:
        start_idx = text_nodes.index("日均价格")
        end_idx = text_nodes.index("各地均价：")
        summary_tokens = text_nodes[start_idx:end_idx]
        numeric_tokens = [token for token in summary_tokens if re.fullmatch(r"-?\d+(?:\.\d+)?%?", token)]
        if len(numeric_tokens) >= 3:
            avg_price = to_float(numeric_tokens[0])
            last_year_price = to_float(numeric_tokens[1])
            pct_tokens = [token for token in numeric_tokens[2:] if token.endswith("%")]
            if len(pct_tokens) >= 2:
                mom_pct = to_percent(pct_tokens[0])
                yoy_pct = to_percent(pct_tokens[1])
            elif len(pct_tokens) == 1:
                mom_pct = to_percent(pct_tokens[0])

    local_rows: list[dict[str, Any]] = []
    if "各地均价：" in text_nodes:
        start_idx = text_nodes.index("各地均价：")
        sliced = text_nodes[start_idx + 1 :]
        # 跳过表头
        pointer = 0
        while pointer < len(sliced) and sliced[pointer] in {"省", "市", "县", "价格", "单位"}:
            pointer += 1
        while pointer + 4 < len(sliced):
            if sliced[pointer].startswith("以上只是部分数据"):
                break
            province = sliced[pointer]
            city = sliced[pointer + 1]
            county = sliced[pointer + 2]
            price = sliced[pointer + 3]
            unit = sliced[pointer + 4]
            if unit != "元/公斤":
                break
            local_rows.append(
                {
                    "province": province,
                    "city": city,
                    "county": county,
                    "price": to_float(price),
                    "unit": unit,
                }
            )
            pointer += 5

    article_id = article_id_from_url(article_url)
    main_row = {
        "article_id": article_id,
        "article_url": article_url,
        "title": title,
        "publish_datetime": publish_datetime,
        "province": "河南省",
        "product_name": "瘦肉型肉猪(110kg左右)",
        "avg_price": avg_price,
        "avg_price_unit": "元/公斤" if avg_price is not None else None,
        "last_year_price": last_year_price,
        "last_year_price_unit": "元/公斤" if last_year_price is not None else None,
        "mom_pct": mom_pct,
        "yoy_pct": yoy_pct,
    }
    return main_row, local_rows


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def dump_json(path: Path, payload: Any) -> None:
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)


def summarize(search_rows: list[dict[str, Any]], article_rows: list[dict[str, Any]], local_rows: list[dict[str, Any]], failed_urls: list[str]) -> dict[str, Any]:
    dates = sorted(row["publish_datetime"] for row in article_rows if row.get("publish_datetime"))
    review_rows = [
        {
            "article_url": row["article_url"],
            "title": row["title"],
            "publish_datetime": row["publish_datetime"],
            "validation_flag": row["validation_flag"],
            "validation_notes": row["validation_notes"],
        }
        for row in article_rows
        if row["validation_flag"] != "ok"
    ]
    return {
        "search_pages_fetched": len({row["search_page_no"] for row in search_rows}),
        "search_results_discovered": len(search_rows),
        "articles_processed": len(article_rows),
        "local_price_rows": len(local_rows),
        "failed_urls": failed_urls,
        "time_coverage": {
            "start": dates[0] if dates else None,
            "end": dates[-1] if dates else None,
        },
        "avg_price_non_null_count": sum(1 for row in article_rows if row.get("avg_price") is not None),
        "last_year_price_non_null_count": sum(1 for row in article_rows if row.get("last_year_price") is not None),
        "mom_pct_non_null_count": sum(1 for row in article_rows if row.get("mom_pct") is not None),
        "yoy_pct_non_null_count": sum(1 for row in article_rows if row.get("yoy_pct") is not None),
        "manual_review_rows": review_rows,
    }


def crawl(args: argparse.Namespace) -> dict[str, Any]:
    project_root = Path(__file__).resolve().parents[1]
    paths = init_paths(project_root)
    log_path = paths["logs_root"] / f"soozhu_henan_hog_daily_crawl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = init_logger(log_path)
    session = build_session()
    bootstrap_search(session)
    started_at = now_iso()

    search_rows: list[dict[str, Any]] = []
    page_no = 1
    max_pages = args.max_pages or 9999
    total_pages = None
    while page_no <= max_pages:
        payload = fetch_search_json(session, page_no)
        if total_pages is None:
            total_pages = payload.get("pageCount")
            if args.max_pages == 0:
                max_pages = total_pages
        obj_list = payload.get("obj_list", [])
        if not obj_list:
            break

        search_json_path = paths["search_root"] / f"search_page_{page_no:04d}.json"
        dump_json(search_json_path, payload)
        for obj in obj_list:
            article_url = urljoin(ARTICLE_BASE_URL, obj.get("viewurl", ""))
            search_rows.append(
                {
                    "search_keyword": SEARCH_KEYWORD,
                    "search_page_no": page_no,
                    "search_rank_in_page": len(search_rows) + 1,
                    "article_id": obj.get("id"),
                    "article_url": article_url,
                    "title": obj.get("title"),
                    "summary": obj.get("summary"),
                    "pubdate": obj.get("pubdate"),
                    "author": obj.get("author"),
                    "clicks": obj.get("clicks"),
                    "category_name": obj.get("cname"),
                    "category_url": urljoin(ARTICLE_BASE_URL, obj.get("curl", "")),
                    "search_json_path": rel_path(search_json_path, project_root),
                    "discovered_at": started_at,
                }
            )
        logger.info("搜索结果页完成：page_no=%s 当前累计文章=%s", page_no, len(search_rows))
        page_no += 1
        time.sleep(args.sleep_seconds)

    dedup_map = {}
    for row in search_rows:
        dedup_map[row["article_url"]] = row
    target_rows = list(dedup_map.values())
    if args.max_articles:
        target_rows = target_rows[: args.max_articles]
        logger.info("启用样本限制，仅处理前 %s 篇文章", args.max_articles)

    write_csv(
        paths["search_index_csv"],
        search_rows,
        [
            "search_keyword",
            "search_page_no",
            "search_rank_in_page",
            "article_id",
            "article_url",
            "title",
            "summary",
            "pubdate",
            "author",
            "clicks",
            "category_name",
            "category_url",
            "search_json_path",
            "discovered_at",
        ],
    )

    article_rows: list[dict[str, Any]] = []
    local_rows: list[dict[str, Any]] = []
    failed_urls: list[str] = []

    for idx, row in enumerate(target_rows, start=1):
        article_url = row["article_url"]
        logger.info("处理文章 [%s/%s] %s", idx, len(target_rows), article_url)
        try:
            response = session.get(article_url, timeout=30)
            response.raise_for_status()
            response.encoding = response.apparent_encoding or response.encoding or "utf-8"
            html = response.text
            main_row, detail_rows = parse_article(article_url, html)

            article_id = main_row["article_id"]
            publish_date_for_name = (main_row["publish_datetime"] or row.get("pubdate") or "unknown").replace(":", "-").replace(" ", "_")
            file_stub = f"{publish_date_for_name}_{article_id}"
            html_path = paths["html_root"] / f"{file_stub}.html"
            text_path = paths["text_root"] / f"{file_stub}.txt"
            meta_path = paths["meta_root"] / f"{file_stub}.json"
            html_path.write_text(html, encoding="utf-8")
            text_path.write_text(normalize_text(BeautifulSoup(html, "lxml").get_text("\n", strip=True)), encoding="utf-8")

            validation_notes = []
            missing_fields = [field for field in ["avg_price", "last_year_price", "mom_pct"] if main_row.get(field) is None]
            if missing_fields:
                validation_flag = "missing_key_metrics"
                validation_notes.append(f"missing:{','.join(missing_fields)}")
            else:
                validation_flag = "ok"
            if main_row.get("yoy_pct") is None:
                validation_notes.append("yoy_pct_not_displayed_on_page")

            article_row = {
                "search_keyword": SEARCH_KEYWORD,
                "search_page_no": row["search_page_no"],
                "article_id": article_id,
                "article_url": article_url,
                "title": main_row["title"],
                "publish_datetime": main_row["publish_datetime"] or row.get("pubdate"),
                "province": main_row["province"],
                "product_name": main_row["product_name"],
                "avg_price": main_row["avg_price"],
                "avg_price_unit": main_row["avg_price_unit"],
                "last_year_price": main_row["last_year_price"],
                "last_year_price_unit": main_row["last_year_price_unit"],
                "mom_pct": main_row["mom_pct"],
                "yoy_pct": main_row["yoy_pct"],
                "raw_html_path": rel_path(html_path, project_root),
                "parsing_method": "html_text",
                "validation_flag": validation_flag,
                "validation_notes": "; ".join(validation_notes),
            }
            article_rows.append(article_row)

            for detail in detail_rows:
                local_rows.append(
                    {
                        "article_id": article_id,
                        "article_url": article_url,
                        "publish_datetime": article_row["publish_datetime"],
                        "province": detail["province"],
                        "city": detail["city"],
                        "county": detail["county"],
                        "price": detail["price"],
                        "unit": detail["unit"],
                    }
                )

            dump_json(
                meta_path,
                {
                    "article_url": article_url,
                    "title": article_row["title"],
                    "publish_datetime": article_row["publish_datetime"],
                    "search_keyword": SEARCH_KEYWORD,
                    "search_page_no": row["search_page_no"],
                    "fetched_at": now_iso(),
                    "html_path": rel_path(html_path, project_root),
                    "text_path": rel_path(text_path, project_root),
                    "parse_status": validation_flag,
                    "notes": article_row["validation_notes"],
                },
            )
        except Exception as exc:
            failed_urls.append(article_url)
            logger.exception("文章处理失败：%s error=%s", article_url, exc)
        time.sleep(args.sleep_seconds)

    write_csv(
        paths["article_daily_csv"],
        article_rows,
        [
            "search_keyword",
            "search_page_no",
            "article_id",
            "article_url",
            "title",
            "publish_datetime",
            "province",
            "product_name",
            "avg_price",
            "avg_price_unit",
            "last_year_price",
            "last_year_price_unit",
            "mom_pct",
            "yoy_pct",
            "raw_html_path",
            "parsing_method",
            "validation_flag",
            "validation_notes",
        ],
    )
    write_csv(
        paths["local_detail_csv"],
        local_rows,
        ["article_id", "article_url", "publish_datetime", "province", "city", "county", "price", "unit"],
    )

    summary = summarize(search_rows, article_rows, local_rows, failed_urls)
    summary.update(
        {
            "run_started_at": started_at,
            "run_finished_at": now_iso(),
            "search_index_csv": rel_path(paths["search_index_csv"], project_root),
            "article_daily_csv": rel_path(paths["article_daily_csv"], project_root),
            "local_detail_csv": rel_path(paths["local_detail_csv"], project_root),
            "log_path": rel_path(log_path, project_root),
        }
    )
    summary_path = paths["metadata_root"] / "soozhu_henan_hog_daily_summary.json"
    review_path = paths["metadata_root"] / "soozhu_henan_hog_daily_manual_review.csv"
    dump_json(summary_path, summary)
    write_csv(
        review_path,
        summary["manual_review_rows"],
        ["article_url", "title", "publish_datetime", "validation_flag", "validation_notes"],
    )
    logger.info("抓取结束：文章=%s 本地明细=%s 失败=%s", len(article_rows), len(local_rows), len(failed_urls))
    return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="抓取搜猪网河南省瘦肉型肉猪日度价格")
    parser.add_argument("--max-pages", type=int, default=0, help="最多抓取多少个搜索结果页，0 表示直到末页")
    parser.add_argument("--max-articles", type=int, default=0, help="仅处理前 N 篇文章，0 表示不限制")
    parser.add_argument("--sleep-seconds", type=float, default=0.05, help="请求间隔秒数")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    summary = crawl(args)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
