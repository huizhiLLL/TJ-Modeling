#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def to_key(row: dict[str, str]) -> tuple[str, str]:
    return (row.get("publish_date", ""), row.get("week_label", ""))


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    source3 = read_csv(root / "data" / "interim" / "moa_xm_weekly_parsed" / "moa_xm_weekly_prices.csv")
    source6 = read_csv(root / "data" / "interim" / "scs_feed_weekly_parsed" / "scs_feed_weekly_prices.csv")
    source7 = read_csv(root / "data" / "interim" / "moa_feed_weekly_legacy_supplement_parsed" / "moa_feed_weekly_legacy_supplement_prices.csv")

    merged: dict[tuple[str, str], dict[str, str]] = {}

    for row in source3:
        merged[to_key(row)] = {
            "article_url": row["article_url"],
            "title": row["title"],
            "publish_date": row["publish_date"],
            "week_label": row["week_label"],
            "collect_date": row["collect_date"],
            "corn_price": row["corn_price"],
            "corn_price_unit": row["corn_price_unit"],
            "soymeal_price": row["soymeal_price"],
            "soymeal_price_unit": row["soymeal_price_unit"],
            "source_segment": "moa_xm_weekly",
            "raw_html_path": row["raw_html_path"],
            "raw_text_path": row["raw_text_path"],
            "parsing_method": row["parsing_method"],
            "validation_flag": row["validation_flag"],
            "validation_notes": row["validation_notes"],
        }

    for row in source7:
        merged[to_key(row)] = {
            "article_url": row["article_url"],
            "title": row["title"],
            "publish_date": row["publish_date"],
            "week_label": row["week_label"],
            "collect_date": row["collect_date"],
            "corn_price": row["corn_price"],
            "corn_price_unit": row["corn_price_unit"],
            "soymeal_price": row["soymeal_price"],
            "soymeal_price_unit": row["soymeal_price_unit"],
            "source_segment": "moa_feed_weekly_legacy_supplement",
            "raw_html_path": row["raw_html_path"],
            "raw_text_path": row["raw_text_path"],
            "parsing_method": row["parsing_method"],
            "validation_flag": row["validation_flag"],
            "validation_notes": row["validation_notes"],
        }

    for row in source6:
        merged[to_key(row)] = {
            "article_url": row["article_url"],
            "title": row["title"],
            "publish_date": row["publish_date"],
            "week_label": row["week_label"],
            "collect_date": row["collect_date"],
            "corn_price": row["corn_price"],
            "corn_price_unit": row["corn_price_unit"],
            "soymeal_price": row["soymeal_price"],
            "soymeal_price_unit": row["soymeal_price_unit"],
            "source_segment": "scs_feed_weekly",
            "raw_html_path": row["raw_html_path"],
            "raw_text_path": row["raw_text_path"],
            "parsing_method": row["parsing_method"],
            "validation_flag": row["validation_flag"],
            "validation_notes": row["validation_notes"],
        }

    rows = sorted(merged.values(), key=lambda r: (r["publish_date"], r["article_url"]))

    out_dir = root / "data" / "interim" / "moa_feed_weekly_chain_parsed"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "moa_feed_weekly_chain_prices.csv"
    write_csv(
        out_path,
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

    metadata_dir = root / "data" / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "rows": len(rows),
        "time_coverage": {
            "start": rows[0]["publish_date"] if rows else None,
            "end": rows[-1]["publish_date"] if rows else None,
        },
        "segments": {
            "moa_xm_weekly": sum(1 for r in rows if r["source_segment"] == "moa_xm_weekly"),
            "moa_feed_weekly_legacy_supplement": sum(1 for r in rows if r["source_segment"] == "moa_feed_weekly_legacy_supplement"),
            "scs_feed_weekly": sum(1 for r in rows if r["source_segment"] == "scs_feed_weekly"),
        },
        "corn_price_non_null_count": sum(1 for r in rows if r["corn_price"]),
        "soymeal_price_non_null_count": sum(1 for r in rows if r["soymeal_price"]),
        "generated_at": now_iso(),
        "output_csv": "data/interim/moa_feed_weekly_chain_parsed/moa_feed_weekly_chain_prices.csv",
    }
    (metadata_dir / "moa_feed_weekly_chain_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
