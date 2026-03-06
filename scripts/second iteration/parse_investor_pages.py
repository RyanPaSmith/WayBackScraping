"""
Parse downloaded investor pages into:
1. parsed_data       -> page-level metadata
2. disclosure_items  -> repeated disclosure rows from index pages
3. linked_targets    -> PDFs / detail pages to resolve and download
"""

import csv
import json
import os
import re
import sqlite3
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup


DATE_PATTERNS = [
    "%B %d, %Y",
    "%b %d, %Y",
    "%B %d %Y",
    "%b %d %Y",
]

PAGE_TYPE_HINTS = {
    "earnings_index": ["earnings", "results.cfm", "quarterly-results", "financial-results", "results"],
    "sec_index": ["sec-filings", "sec.cfm", "/sec/", "/filings"],
    "press_index": ["press-release", "press-releases", "newsroom", "/news/", "/press/", "/pr/"],
    "annual_reports_index": ["annual-report", "annual-reports", "proxy", "financial-history", "financials.cfm"],
    "investor_home": ["investor-relations/default", "investor-relations/index", "/investor-relations/", "/investor/", "/investors/"],
    "governance_index": ["governance", "leadership-and-governance", "corporate-governance"],
    "esg_index": ["/esg/", "environment social governance", "sustainability", "responsibility"],
}

LINK_TYPE_HINTS = {
    "earnings_release": ["results", "earnings", "quarterly results", "financial results"],
    "sec_filing": ["10-k", "10-q", "8-k", "sec", "filing", "edgar"],
    "annual_report": ["annual report", "proxy", "financial history"],
    "presentation": ["presentation", "slides", "webcast"],
    "press_release": ["press release", "news release", "announcement", "newsroom"],
    "esg_doc": ["esg", "sustainability", "responsibility"],
}

HIGH_VALUE_LINK_HINTS = [
    "download",
    ".pdf",
    "results",
    "earnings",
    "annual report",
    "proxy",
    "presentation",
    "release",
    "10-k",
    "10-q",
    "8-k",
]


def init_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS parsed_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            capture_id INTEGER NOT NULL UNIQUE,

            page_title TEXT,
            page_type TEXT,
            page_class TEXT,

            links_json TEXT,
            priority_links_json TEXT,
            main_text TEXT,

            item_count INTEGER DEFAULT 0,
            parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            FOREIGN KEY (capture_id) REFERENCES captures(id)
        );
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_parsed_capture_id
        ON parsed_data(capture_id);
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_parsed_page_type
        ON parsed_data(page_type);
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_parsed_page_class
        ON parsed_data(page_class);
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS disclosure_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            capture_id INTEGER NOT NULL,
            firm TEXT NOT NULL,
            snapshot_year INTEGER NOT NULL,

            page_type TEXT,
            item_date TEXT,
            item_date_raw TEXT,
            headline TEXT,
            linked_url TEXT,
            source_page_url TEXT,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            UNIQUE (capture_id, item_date_raw, headline, linked_url),
            FOREIGN KEY (capture_id) REFERENCES captures(id)
        );
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_disclosure_items_capture_id
        ON disclosure_items(capture_id);
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_disclosure_items_year
        ON disclosure_items(snapshot_year);
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS linked_targets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            source_capture_id INTEGER NOT NULL,
            snapshot_year INTEGER NOT NULL,

            target_url TEXT NOT NULL,
            link_text TEXT,
            inferred_type TEXT,
            wants_download INTEGER NOT NULL DEFAULT 1,

            status TEXT NOT NULL DEFAULT 'pending',
            capture_id INTEGER,
            resolved_wayback_timestamp TEXT,
            error_message TEXT,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            UNIQUE (source_capture_id, target_url),
            FOREIGN KEY (source_capture_id) REFERENCES captures(id),
            FOREIGN KEY (capture_id) REFERENCES captures(id)
        );
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_linked_targets_status
        ON linked_targets(status);
    """)

    conn.commit()


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def parse_date_from_text(text: str) -> tuple[Optional[str], Optional[str]]:
    if not text:
        return None, None

    text = normalize_whitespace(text)

    date_regexes = [
        r"([A-Z][a-z]+ \d{1,2}, \d{4})",
        r"([A-Z][a-z]{2} \d{1,2}, \d{4})",
        r"([A-Z][a-z]+ \d{1,2} \d{4})",
        r"([A-Z][a-z]{2} \d{1,2} \d{4})",
    ]

    for regex in date_regexes:
        match = re.search(regex, text)
        if not match:
            continue

        raw_date = match.group(1).strip()

        for date_format in DATE_PATTERNS:
            try:
                parsed = datetime.strptime(raw_date, date_format).strftime("%Y-%m-%d")
                return raw_date, parsed
            except ValueError:
                continue

    return None, None


def infer_page_type(url: str, title: str, text: str) -> str:
    lower_url = (url or "").lower()
    lower_title = (title or "").lower()
    lower_text = (text or "").lower()[:2000]
    combined = f"{lower_url} {lower_title} {lower_text}"

    for page_type, hints in PAGE_TYPE_HINTS.items():
        if any(hint in combined for hint in hints):
            return page_type

    if "10-k" in combined or "10-q" in combined or "8-k" in combined:
        return "sec_detail"

    return "other"


def infer_link_type(url: str, text: str) -> str:
    combined = f"{(url or '').lower()} {(text or '').lower()}"

    for link_type, hints in LINK_TYPE_HINTS.items():
        if any(hint in combined for hint in hints):
            return link_type

    if ".pdf" in combined or "download" in combined:
        return "document"

    return "other"


def is_document_url(url: str) -> bool:
    lower_url = (url or "").lower()
    return any(lower_url.endswith(ext) for ext in [".pdf", ".doc", ".docx", ".xls", ".xlsx"]) or "download" in lower_url


def should_queue_link(url: str, text: str) -> bool:
    combined = f"{(url or '').lower()} {(text or '').lower()}"
    return any(hint in combined for hint in HIGH_VALUE_LINK_HINTS)


def extract_main_text(soup: BeautifulSoup) -> str:
    working_soup = BeautifulSoup(str(soup), "html.parser")

    for tag in working_soup(["script", "style", "noscript", "header", "footer"]):
        tag.decompose()

    text = working_soup.get_text(separator="\n", strip=True)
    lines = [normalize_whitespace(line) for line in text.split("\n")]
    lines = [line for line in lines if line]
    return "\n".join(lines)


def extract_all_links(soup: BeautifulSoup, base_url: str) -> list[dict]:
    links = []

    for anchor in soup.find_all("a", href=True):
        href = normalize_whitespace(anchor.get("href", ""))
        text = normalize_whitespace(anchor.get_text(" ", strip=True))

        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue

        absolute_url = urljoin(base_url, href)
        link_type = infer_link_type(absolute_url, text)

        links.append({
            "url": absolute_url,
            "text": text,
            "href": href,
            "link_type": link_type,
            "is_document": is_document_url(absolute_url),
        })

    deduped = []
    seen = set()

    for link in links:
        key = (link["url"], link["text"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(link)

    return deduped


def extract_priority_links(all_links: list[dict]) -> list[dict]:
    return [link for link in all_links if should_queue_link(link["url"], link["text"])]


def extract_disclosure_items(soup: BeautifulSoup, base_url: str, page_type: str) -> list[dict]:
    items = []

    candidate_tags = soup.find_all(["li", "tr", "p", "div"])

    for tag in candidate_tags:
        row_text = normalize_whitespace(tag.get_text(" ", strip=True))
        if len(row_text) < 12:
            continue

        raw_date, normalized_date = parse_date_from_text(row_text)
        if not raw_date:
            continue

        anchor = tag.find("a", href=True)
        if not anchor:
            continue

        href = normalize_whitespace(anchor.get("href", ""))
        headline = normalize_whitespace(anchor.get_text(" ", strip=True))
        if not headline:
            continue

        linked_url = urljoin(base_url, href)

        items.append({
            "page_type": page_type,
            "item_date": normalized_date,
            "item_date_raw": raw_date,
            "headline": headline,
            "linked_url": linked_url,
        })

    deduped = []
    seen = set()

    for item in items:
        key = (item["item_date_raw"], item["headline"], item["linked_url"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    return deduped


def classify_page_class(page_type: str, disclosure_items: list[dict], all_links: list[dict], main_text: str) -> str:
    if disclosure_items and len(disclosure_items) >= 2:
        return "index_page"

    if page_type in {"earnings_index", "sec_index", "press_index", "annual_reports_index"} and len(all_links) >= 5:
        return "index_page"

    if len(main_text) > 1200 and len(disclosure_items) <= 1:
        return "detail_page"

    if page_type in {"investor_home", "governance_index", "esg_index"}:
        return "nav_page"

    return "nav_page"


def parse_html_file(file_path: str, url: str) -> dict:
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as file_handle:
            html = file_handle.read()
    except Exception as exc:
        return {"error": f"Failed to read HTML file: {exc}"}

    soup = BeautifulSoup(html, "html.parser")
    title_tag = soup.find("title")
    page_title = normalize_whitespace(title_tag.get_text(" ", strip=True)) if title_tag else ""

    main_text = extract_main_text(soup)
    page_type = infer_page_type(url, page_title, main_text)
    all_links = extract_all_links(soup, url)
    priority_links = extract_priority_links(all_links)
    disclosure_items = extract_disclosure_items(soup, url, page_type)
    page_class = classify_page_class(page_type, disclosure_items, all_links, main_text)

    return {
        "error": None,
        "page_title": page_title,
        "page_type": page_type,
        "page_class": page_class,
        "main_text": main_text[:15000],
        "all_links": all_links,
        "priority_links": priority_links,
        "disclosure_items": disclosure_items,
    }


def insert_parsed_pdf_stub(cur: sqlite3.Cursor, capture_row: sqlite3.Row) -> None:
    page_type = "document_pdf"
    lower_page_key = (capture_row["page_key"] or "").lower()

    if "earnings" in lower_page_key:
        page_type = "earnings_document"
    elif "sec" in lower_page_key:
        page_type = "sec_document"
    elif "annual" in lower_page_key:
        page_type = "annual_report_document"
    elif "presentation" in lower_page_key:
        page_type = "presentation_document"

    cur.execute("""
        INSERT OR IGNORE INTO parsed_data (
            capture_id, page_title, page_type, page_class,
            links_json, priority_links_json, main_text, item_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        capture_row["id"],
        os.path.basename(capture_row["local_path"] or capture_row["url"]),
        page_type,
        "detail_page",
        "[]",
        "[]",
        None,
        0,
    ))


def insert_disclosure_items(
    cur: sqlite3.Cursor,
    capture_row: sqlite3.Row,
    items: list[dict]
) -> None:
    for item in items:
        cur.execute("""
            INSERT OR IGNORE INTO disclosure_items (
                capture_id, firm, snapshot_year, page_type,
                item_date, item_date_raw, headline, linked_url, source_page_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            capture_row["id"],
            capture_row["firm"],
            capture_row["snapshot_year"],
            item["page_type"],
            item["item_date"],
            item["item_date_raw"],
            item["headline"],
            item["linked_url"],
            capture_row["url"],
        ))


def insert_linked_targets(
    cur: sqlite3.Cursor,
    capture_row: sqlite3.Row,
    priority_links: list[dict],
    disclosure_items: list[dict]
) -> None:
    queued_urls = set()

    for link in priority_links:
        if not link["url"]:
            continue
        queued_urls.add((link["url"], link["text"], link["link_type"]))

    for item in disclosure_items:
        if item["linked_url"]:
            queued_urls.add((item["linked_url"], item["headline"], item["page_type"]))

    for target_url, link_text, inferred_type in queued_urls:
        cur.execute("""
            INSERT OR IGNORE INTO linked_targets (
                source_capture_id, snapshot_year, target_url, link_text, inferred_type, wants_download, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            capture_row["id"],
            capture_row["snapshot_year"],
            target_url,
            link_text,
            inferred_type,
            1,
            "pending",
        ))


def parse_all_downloaded_captures(db_path: str) -> None:
    conn = sqlite3.connect(db_path, timeout=60)
    conn.row_factory = sqlite3.Row

    try:
        init_tables(conn)
        cur = conn.cursor()

        cur.execute("""
            SELECT *
            FROM captures
            WHERE fetch_status = 'downloaded'
              AND local_path IS NOT NULL
              AND id NOT IN (SELECT capture_id FROM parsed_data)
            ORDER BY snapshot_year, source_type, id
        """)

        captures = cur.fetchall()
        if not captures:
            print("No new downloaded captures to parse")
            return

        print(f"\nParsing {len(captures)} downloaded captures...")

        for index, capture in enumerate(captures, 1):
            print(f"\n[{index}/{len(captures)}] Capture {capture['id']}")
            print(f"  Source type: {capture['source_type']}")
            print(f"  URL: {capture['url']}")

            local_path = capture["local_path"]
            mime_type = (capture["mime_type"] or "").lower()

            if not local_path or not os.path.exists(local_path):
                print("  ✗ File missing; skipping")
                continue

            if "pdf" in mime_type or local_path.lower().endswith(".pdf"):
                insert_parsed_pdf_stub(cur, capture)
                conn.commit()
                print("  ✓ Registered PDF stub in parsed_data")
                continue

            result = parse_html_file(local_path, capture["url"])
            if result["error"]:
                print(f"  ✗ Parse error: {result['error']}")
                continue

            cur.execute("""
                INSERT OR IGNORE INTO parsed_data (
                    capture_id, page_title, page_type, page_class,
                    links_json, priority_links_json, main_text, item_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                capture["id"],
                result["page_title"],
                result["page_type"],
                result["page_class"],
                json.dumps(result["all_links"], indent=2),
                json.dumps(result["priority_links"], indent=2),
                result["main_text"],
                len(result["disclosure_items"]),
            ))

            insert_disclosure_items(cur, capture, result["disclosure_items"])
            insert_linked_targets(cur, capture, result["priority_links"], result["disclosure_items"])

            conn.commit()

            print(f"  ✓ Parsed page")
            print(f"    Title: {result['page_title'][:80]}")
            print(f"    Page type: {result['page_type']}")
            print(f"    Page class: {result['page_class']}")
            print(f"    Disclosure items: {len(result['disclosure_items'])}")
            print(f"    Priority links queued: {len(result['priority_links'])}")

    finally:
        conn.close()


def generate_summary_report(db_path: str, output_file: str = "parsing_summary.txt") -> None:
    conn = sqlite3.connect(db_path, timeout=60)
    conn.row_factory = sqlite3.Row

    try:
        cur = conn.cursor()

        with open(output_file, "w", encoding="utf-8") as file_handle:
            file_handle.write("=" * 80 + "\n")
            file_handle.write("INVESTOR DISCLOSURE PIPELINE SUMMARY\n")
            file_handle.write("=" * 80 + "\n\n")

            cur.execute("SELECT COUNT(*) AS cnt FROM parsed_data")
            file_handle.write(f"Parsed pages: {cur.fetchone()['cnt']}\n")

            cur.execute("SELECT COUNT(*) AS cnt FROM disclosure_items")
            file_handle.write(f"Disclosure items extracted: {cur.fetchone()['cnt']}\n")

            cur.execute("SELECT COUNT(*) AS cnt FROM linked_targets")
            file_handle.write(f"Linked targets queued: {cur.fetchone()['cnt']}\n\n")

            file_handle.write("Parsed pages by class:\n")
            file_handle.write("-" * 40 + "\n")
            cur.execute("""
                SELECT page_class, COUNT(*) AS cnt
                FROM parsed_data
                GROUP BY page_class
                ORDER BY cnt DESC
            """)
            for row in cur.fetchall():
                file_handle.write(f"  {row['page_class']:20s}: {row['cnt']:3d}\n")

            file_handle.write("\nParsed pages by type:\n")
            file_handle.write("-" * 40 + "\n")
            cur.execute("""
                SELECT page_type, COUNT(*) AS cnt
                FROM parsed_data
                GROUP BY page_type
                ORDER BY cnt DESC
            """)
            for row in cur.fetchall():
                file_handle.write(f"  {row['page_type']:25s}: {row['cnt']:3d}\n")

            file_handle.write("\nLinked target status:\n")
            file_handle.write("-" * 40 + "\n")
            cur.execute("""
                SELECT status, COUNT(*) AS cnt
                FROM linked_targets
                GROUP BY status
                ORDER BY cnt DESC
            """)
            for row in cur.fetchall():
                file_handle.write(f"  {row['status']:20s}: {row['cnt']:3d}\n")

    finally:
        conn.close()

    print(f"\n✓ Summary report written to {output_file}")


def export_disclosure_items_to_csv(db_path: str, output_file: str = "disclosure_items.csv") -> None:
    conn = sqlite3.connect(db_path, timeout=60)
    conn.row_factory = sqlite3.Row

    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                firm,
                snapshot_year,
                page_type,
                item_date,
                item_date_raw,
                headline,
                linked_url,
                source_page_url
            FROM disclosure_items
            ORDER BY snapshot_year, item_date, headline
        """)

        with open(output_file, "w", newline="", encoding="utf-8") as file_handle:
            writer = csv.writer(file_handle)
            writer.writerow([
                "Firm",
                "Snapshot_Year",
                "Page_Type",
                "Item_Date",
                "Item_Date_Raw",
                "Headline",
                "Linked_URL",
                "Source_Page_URL",
            ])

            for row in cur.fetchall():
                writer.writerow([
                    row["firm"],
                    row["snapshot_year"],
                    row["page_type"],
                    row["item_date"],
                    row["item_date_raw"],
                    row["headline"],
                    row["linked_url"],
                    row["source_page_url"],
                ])

    finally:
        conn.close()

    print(f"✓ Disclosure items exported to {output_file}")


def export_linked_targets_to_csv(db_path: str, output_file: str = "linked_targets.csv") -> None:
    conn = sqlite3.connect(db_path, timeout=60)
    conn.row_factory = sqlite3.Row

    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                lt.snapshot_year,
                c.firm,
                lt.target_url,
                lt.link_text,
                lt.inferred_type,
                lt.status,
                lt.capture_id
            FROM linked_targets lt
            JOIN captures c ON lt.source_capture_id = c.id
            ORDER BY lt.snapshot_year, lt.id
        """)

        with open(output_file, "w", newline="", encoding="utf-8") as file_handle:
            writer = csv.writer(file_handle)
            writer.writerow([
                "Snapshot_Year",
                "Firm",
                "Target_URL",
                "Link_Text",
                "Inferred_Type",
                "Status",
                "Capture_ID",
            ])

            for row in cur.fetchall():
                writer.writerow([
                    row["snapshot_year"],
                    row["firm"],
                    row["target_url"],
                    row["link_text"],
                    row["inferred_type"],
                    row["status"],
                    row["capture_id"],
                ])

    finally:
        conn.close()

    print(f"✓ Linked targets exported to {output_file}")


if __name__ == "__main__":
    DB_PATH = "apple_investor_pages.db"

    print("=" * 70)
    print("PARSING DOWNLOADED INVESTOR CAPTURES")
    print("=" * 70)

    parse_all_downloaded_captures(DB_PATH)
    generate_summary_report(DB_PATH, "parsing_summary.txt")
    export_disclosure_items_to_csv(DB_PATH, "disclosure_items.csv")
    export_linked_targets_to_csv(DB_PATH, "linked_targets.csv")

    print("\n✓ Parsing stage complete")