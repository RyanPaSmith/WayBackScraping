import os
import time
import sqlite3
from datetime import datetime
from pathlib import Path
from urllib.parse import quote, urlsplit, urlunsplit

import requests


CDX_ENDPOINT = "https://web.archive.org/cdx/search/cdx"
WAYBACK_FETCH = "https://web.archive.org/web/{timestamp}id_/{url}"

USER_AGENT = "wayback-disclosure-research/2.0 (contact: your_email@example.com)"
CDX_CONNECT_TIMEOUT = 15
CDX_READ_TIMEOUT = 120
DOWNLOAD_TIMEOUT = 60

DISCOVERY_PREFIX_TEMPLATES = [
    "https://investor.{d}/",
    "https://ir.{d}/",
    "https://www.{d}/investor",
    "https://www.{d}/investors",
    "https://www.{d}/investor-relations",
    "http://investor.{d}/",
    "http://ir.{d}/",
]

KEY_INDEX_RULES = {
    "investor_home": [
        "investor-relations/default",
        "investor-relations/index",
        "/investor-relations/",
        "/investor/",
        "/investors/",
    ],
    "earnings_index": [
        "results.cfm",
        "earnings",
        "quarterly-results",
        "financial-results",
        "results",
    ],
    "sec_index": [
        "sec.cfm",
        "sec-filings",
        "/sec/",
        "/filings",
    ],
    "press_index": [
        "press-release",
        "press-releases",
        "newsroom",
        "/news/",
        "/press/",
        "/pr/",
    ],
    "annual_reports_index": [
        "annual-report",
        "annual-reports",
        "financial-history",
        "financials.cfm",
        "proxy",
    ],
    "governance_index": [
        "governance",
        "leadership-and-governance",
        "corporate-governance",
    ],
    "esg_index": [
        "/esg/",
        "environment-social-governance",
        "sustainability",
        "responsibility",
    ],
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


def init_db(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA busy_timeout=60000;")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS captures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            firm TEXT NOT NULL,
            base_domain TEXT,
            url TEXT NOT NULL,

            snapshot_year INTEGER NOT NULL,
            wayback_timestamp TEXT NOT NULL,

            mime_type TEXT,
            status_code INTEGER,
            content_bytes INTEGER,
            digest TEXT,

            source_type TEXT NOT NULL DEFAULT 'seed',
            page_key TEXT,
            parent_capture_id INTEGER,

            fetch_status TEXT NOT NULL,
            error_message TEXT,

            local_path TEXT,
            downloaded_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            UNIQUE (firm, url, wayback_timestamp)
        );
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_captures_fetch_status
        ON captures(fetch_status);
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_captures_firm_year
        ON captures(firm, snapshot_year);
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_captures_source_type
        ON captures(source_type);
    """)

    conn.commit()


def canonicalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return url

    if "://" not in url:
        url = "http://" + url

    parts = urlsplit(url)
    scheme = parts.scheme.lower() if parts.scheme else "http"
    netloc = parts.netloc.lower()
    path = parts.path or "/"

    if path != "/" and path.endswith("/"):
        path = path[:-1]

    return urlunsplit((scheme, netloc, path, "", ""))


def safe_filename(value: str) -> str:
    return quote(value, safe="").replace("%", "_")[:200]


def cdx_get_json_with_retries(
    session: requests.Session,
    params: dict,
    max_attempts: int = 5
) -> list:
    base_sleep = 3.0

    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(
                CDX_ENDPOINT,
                params=params,
                timeout=(CDX_CONNECT_TIMEOUT, CDX_READ_TIMEOUT),
            )
            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status in (429, 500, 502, 503, 504):
                sleep_s = base_sleep * (2 ** (attempt - 1))
                print(f"CDX HTTP {status}; retrying in {sleep_s:.1f}s")
                time.sleep(sleep_s)
                continue
            raise

        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout,
                requests.exceptions.RequestException) as exc:
            sleep_s = base_sleep * (2 ** (attempt - 1))
            print(f"CDX request error ({exc}); retrying in {sleep_s:.1f}s")
            time.sleep(sleep_s)

    return []


def build_discovery_prefixes(base_domain: str) -> list[str]:
    domain = base_domain.lower().strip()
    seen = set()
    prefixes = []

    for template in DISCOVERY_PREFIX_TEMPLATES:
        value = template.format(d=domain)
        if value not in seen:
            seen.add(value)
            prefixes.append(value)

    return prefixes


def infer_page_key(url: str) -> str:
    lower_url = (url or "").lower()

    for page_key, hints in KEY_INDEX_RULES.items():
        for hint in hints:
            if hint in lower_url:
                return page_key

    return "other"


def is_key_index_candidate(url: str) -> bool:
    return infer_page_key(url) != "other"


def score_seed_url(url: str, page_key: str) -> tuple:
    lower_url = url.lower()
    hint_hits = sum(1 for hint in KEY_INDEX_RULES.get(page_key, []) if hint in lower_url)
    shorter_is_better = -len(lower_url)
    https_bonus = 1 if lower_url.startswith("https://") else 0
    return (hint_hits, https_bonus, shorter_is_better)


def cdx_discover_urls_for_year(
    session: requests.Session,
    base_domain: str,
    year: int,
    per_prefix_limit: int = 100
) -> list[str]:
    originals_all = []
    prefixes = build_discovery_prefixes(base_domain)

    for i, prefix in enumerate(prefixes, 1):
        print(f"  Checking prefix {i}/{len(prefixes)}: {prefix}")

        params = {
            "url": prefix,
            "matchType": "prefix",
            "from": str(year),
            "to": str(year),
            "output": "json",
            "fl": "original",
            "filter": ["statuscode:200"],
            "collapse": "urlkey",
            "limit": str(per_prefix_limit),
        }

        data = cdx_get_json_with_retries(session, params)
        if not data or len(data) < 2:
            time.sleep(0.75)
            continue

        originals = [row[0] for row in data[1:] if row and row[0]]
        originals_all.extend(originals)
        print(f"    Found {len(originals)} URLs")
        time.sleep(1.0)

    seen = set()
    deduped = []

    for url in originals_all:
        canonical = canonicalize_url(url)
        if canonical not in seen:
            seen.add(canonical)
            deduped.append(canonical)

    return deduped


def pick_seed_urls_for_year(discovered_urls: list[str]) -> list[tuple[str, str]]:
    grouped = {}

    for url in discovered_urls:
        page_key = infer_page_key(url)
        if page_key == "other":
            continue
        grouped.setdefault(page_key, []).append(url)

    selected = []
    for page_key, urls in sorted(grouped.items()):
        best_url = sorted(urls, key=lambda u: score_seed_url(u, page_key), reverse=True)[0]
        selected.append((page_key, best_url))

    return selected


def cdx_get_captures_for_url_year(
    session: requests.Session,
    url: str,
    year: int,
    limit: int = 100
) -> list[dict]:
    params = {
        "url": url,
        "from": str(year),
        "to": str(year),
        "output": "json",
        "fl": "timestamp,original,mimetype,statuscode,digest,length",
        "filter": [
            "statuscode:200",
            "mimetype:(text/html|application/pdf)"
        ],
        "collapse": "digest",
        "limit": str(limit),
    }

    data = cdx_get_json_with_retries(session, params)
    if not data or len(data) < 2:
        return []

    header = data[0]
    rows = data[1:]
    return [dict(zip(header, row)) for row in rows]


def choose_capture_closest_to_midyear(captures: list[dict], year: int) -> dict | None:
    if not captures:
        return None

    target = datetime(year, 7, 1, 12, 0, 0)

    def parse_timestamp(timestamp: str) -> datetime:
        return datetime.strptime(timestamp, "%Y%m%d%H%M%S")

    return min(
        captures,
        key=lambda capture: abs((parse_timestamp(capture["timestamp"]) - target).total_seconds())
    )


def get_or_create_capture(
    cur: sqlite3.Cursor,
    firm: str,
    base_domain: str,
    row: dict,
    source_type: str,
    page_key: str,
    parent_capture_id: int | None = None
) -> int:
    timestamp = row["timestamp"]
    year = int(timestamp[:4])
    original = canonicalize_url(row["original"])

    mime_type = row.get("mimetype")
    status_code = int(row["statuscode"]) if row.get("statuscode") else None
    content_bytes = int(row["length"]) if row.get("length") else None
    digest = row.get("digest")

    cur.execute("""
        INSERT OR IGNORE INTO captures (
            firm, base_domain, url, snapshot_year, wayback_timestamp,
            mime_type, status_code, content_bytes, digest,
            source_type, page_key, parent_capture_id, fetch_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        firm,
        base_domain,
        original,
        year,
        timestamp,
        mime_type,
        status_code,
        content_bytes,
        digest,
        source_type,
        page_key,
        parent_capture_id,
        "pending"
    ))

    cur.execute("""
        SELECT id
        FROM captures
        WHERE firm = ? AND url = ? AND wayback_timestamp = ?
    """, (firm, original, timestamp))

    row_id = cur.fetchone()
    return row_id[0]


def build_output_path(
    output_dir: str,
    firm: str,
    year: int,
    source_type: str,
    url: str,
    timestamp: str,
    mime_type: str | None
) -> str:
    extension = ".html"
    if mime_type and "pdf" in mime_type.lower():
        extension = ".pdf"

    safe_url = safe_filename(url)
    filename = f"{firm}_{year}_{source_type}_{timestamp}_{safe_url}{extension}"
    return os.path.join(output_dir, firm, str(year), source_type, filename)


def download_wayback_capture(
    session: requests.Session,
    timestamp: str,
    url: str,
    output_path: str,
    max_attempts: int = 3
) -> tuple[bool, str | None]:
    wayback_url = WAYBACK_FETCH.format(timestamp=timestamp, url=url)

    for attempt in range(1, max_attempts + 1):
        try:
            response = session.get(wayback_url, timeout=DOWNLOAD_TIMEOUT, stream=True)
            response.raise_for_status()

            Path(output_path).parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, "wb") as file_handle:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file_handle.write(chunk)

            return True, None

        except requests.exceptions.RequestException as exc:
            print(f"  Download attempt {attempt}/{max_attempts} failed: {exc}")
            if attempt < max_attempts:
                time.sleep(2 ** attempt)
            else:
                return False, str(exc)

    return False, "Max attempts exceeded"


def download_pending_captures(
    db_path: str,
    output_dir: str,
    batch_size: int = 100,
    delay_between_downloads: float = 1.5
) -> None:
    conn = sqlite3.connect(db_path, timeout=60)
    conn.row_factory = sqlite3.Row

    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT id, firm, url, wayback_timestamp, snapshot_year, mime_type, source_type
            FROM captures
            WHERE fetch_status = 'pending'
            ORDER BY snapshot_year, source_type, id
            LIMIT ?
        """, (batch_size,))

        pending_rows = cur.fetchall()
        if not pending_rows:
            print("No pending captures to download")
            return

        print(f"\nDownloading {len(pending_rows)} captures...")

        with requests.Session() as session:
            session.headers.update({"User-Agent": USER_AGENT})

            for index, row in enumerate(pending_rows, 1):
                capture_id = row["id"]
                local_path = build_output_path(
                    output_dir=output_dir,
                    firm=row["firm"],
                    year=row["snapshot_year"],
                    source_type=row["source_type"],
                    url=row["url"],
                    timestamp=row["wayback_timestamp"],
                    mime_type=row["mime_type"],
                )

                print(f"\n[{index}/{len(pending_rows)}] Downloading capture {capture_id}")
                print(f"  URL: {row['url']}")
                print(f"  Source type: {row['source_type']}")
                print(f"  Timestamp: {row['wayback_timestamp']}")

                success, error_message = download_wayback_capture(
                    session=session,
                    timestamp=row["wayback_timestamp"],
                    url=row["url"],
                    output_path=local_path,
                )

                if success:
                    cur.execute("""
                        UPDATE captures
                        SET fetch_status = 'downloaded',
                            local_path = ?,
                            downloaded_at = CURRENT_TIMESTAMP,
                            error_message = NULL
                        WHERE id = ?
                    """, (local_path, capture_id))
                    print(f"  ✓ Saved to: {local_path}")
                else:
                    cur.execute("""
                        UPDATE captures
                        SET fetch_status = 'failed',
                            error_message = ?
                        WHERE id = ?
                    """, (error_message, capture_id))
                    print(f"  ✗ Failed: {error_message}")

                conn.commit()
                time.sleep(delay_between_downloads)

    finally:
        conn.close()


def run_seed_discovery(
    db_path: str,
    firm: str,
    base_domain: str,
    start_year: int,
    end_year: int,
    per_prefix_limit: int = 100
) -> None:
    conn = sqlite3.connect(db_path, timeout=60)

    try:
        init_db(conn)
        cur = conn.cursor()

        with requests.Session() as session:
            session.headers.update({"User-Agent": USER_AGENT})

            for year in range(start_year, end_year + 1):
                print(f"\n{'=' * 70}")
                print(f"SEED DISCOVERY FOR {firm} - {year}")
                print(f"{'=' * 70}")

                discovered_urls = cdx_discover_urls_for_year(
                    session=session,
                    base_domain=base_domain,
                    year=year,
                    per_prefix_limit=per_prefix_limit,
                )
                print(f"\nDiscovered {len(discovered_urls)} URLs total")

                selected_seed_urls = pick_seed_urls_for_year(discovered_urls)
                print(f"Selected {len(selected_seed_urls)} key index pages")

                inserted_count = 0
                for page_key, url in selected_seed_urls:
                    print(f"  [{page_key}] {url}")
                    captures = cdx_get_captures_for_url_year(session, url, year)
                    chosen = choose_capture_closest_to_midyear(captures, year)

                    if not chosen:
                        print("    No suitable annual capture found")
                        continue

                    get_or_create_capture(
                        cur=cur,
                        firm=firm,
                        base_domain=base_domain,
                        row=chosen,
                        source_type="seed",
                        page_key=page_key,
                        parent_capture_id=None,
                    )
                    inserted_count += 1
                    print(f"    ✓ Added capture {chosen['timestamp']}")
                    conn.commit()
                    time.sleep(0.75)

                print(f"\n✓ Inserted {inserted_count} seed captures for {year}")

    finally:
        conn.close()


def resolve_and_download_pending_linked_targets(
    db_path: str,
    output_dir: str,
    batch_size: int = 100,
    delay_between_items: float = 1.0
) -> None:
    conn = sqlite3.connect(db_path, timeout=60)
    conn.row_factory = sqlite3.Row

    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT
                lt.id,
                lt.source_capture_id,
                lt.snapshot_year,
                lt.target_url,
                lt.inferred_type,
                c.firm,
                c.base_domain
            FROM linked_targets lt
            JOIN captures c ON lt.source_capture_id = c.id
            WHERE lt.status = 'pending'
            ORDER BY lt.snapshot_year, lt.id
            LIMIT ?
        """, (batch_size,))

        pending_targets = cur.fetchall()
        if not pending_targets:
            print("No pending linked targets to resolve")
            return

        print(f"\nResolving {len(pending_targets)} linked targets...")

        with requests.Session() as session:
            session.headers.update({"User-Agent": USER_AGENT})

            for index, row in enumerate(pending_targets, 1):
                print(f"\n[{index}/{len(pending_targets)}] {row['target_url']}")
                captures = cdx_get_captures_for_url_year(
                    session=session,
                    url=row["target_url"],
                    year=row["snapshot_year"],
                    limit=50
                )
                chosen = choose_capture_closest_to_midyear(captures, row["snapshot_year"])

                if not chosen:
                    cur.execute("""
                        UPDATE linked_targets
                        SET status = 'failed',
                            error_message = 'No archived capture found for target URL in target year'
                        WHERE id = ?
                    """, (row["id"],))
                    conn.commit()
                    print("  ✗ No archived capture found")
                    time.sleep(delay_between_items)
                    continue

                capture_id = get_or_create_capture(
                    cur=cur,
                    firm=row["firm"],
                    base_domain=row["base_domain"],
                    row=chosen,
                    source_type="linked_target",
                    page_key=row["inferred_type"],
                    parent_capture_id=row["source_capture_id"],
                )
                conn.commit()

                cur.execute("""
                    SELECT id, firm, url, wayback_timestamp, snapshot_year, mime_type, source_type, fetch_status
                    FROM captures
                    WHERE id = ?
                """, (capture_id,))
                capture_row = cur.fetchone()

                if capture_row["fetch_status"] != "downloaded":
                    local_path = build_output_path(
                        output_dir=output_dir,
                        firm=capture_row["firm"],
                        year=capture_row["snapshot_year"],
                        source_type=capture_row["source_type"],
                        url=capture_row["url"],
                        timestamp=capture_row["wayback_timestamp"],
                        mime_type=capture_row["mime_type"],
                    )

                    success, error_message = download_wayback_capture(
                        session=session,
                        timestamp=capture_row["wayback_timestamp"],
                        url=capture_row["url"],
                        output_path=local_path,
                    )

                    if success:
                        cur.execute("""
                            UPDATE captures
                            SET fetch_status = 'downloaded',
                                local_path = ?,
                                downloaded_at = CURRENT_TIMESTAMP,
                                error_message = NULL
                            WHERE id = ?
                        """, (local_path, capture_id))
                        cur.execute("""
                            UPDATE linked_targets
                            SET status = 'downloaded',
                                capture_id = ?,
                                resolved_wayback_timestamp = ?,
                                error_message = NULL
                            WHERE id = ?
                        """, (capture_id, capture_row["wayback_timestamp"], row["id"]))
                        print(f"  ✓ Downloaded linked target into capture {capture_id}")
                    else:
                        cur.execute("""
                            UPDATE captures
                            SET fetch_status = 'failed',
                                error_message = ?
                            WHERE id = ?
                        """, (error_message, capture_id))
                        cur.execute("""
                            UPDATE linked_targets
                            SET status = 'failed',
                                capture_id = ?,
                                error_message = ?
                            WHERE id = ?
                        """, (capture_id, error_message, row["id"]))
                        print(f"  ✗ Failed to download linked target: {error_message}")
                else:
                    cur.execute("""
                        UPDATE linked_targets
                        SET status = 'downloaded',
                            capture_id = ?,
                            resolved_wayback_timestamp = ?
                        WHERE id = ?
                    """, (capture_id, capture_row["wayback_timestamp"], row["id"]))
                    print(f"  ✓ Already downloaded as capture {capture_id}")

                conn.commit()
                time.sleep(delay_between_items)

    finally:
        conn.close()


if __name__ == "__main__":
    DB_PATH = "apple_investor_pages.db"
    FIRM = "AAPL"
    BASE_DOMAIN = "apple.com"
    OUTPUT_DIR = "downloads"

    print("STEP 1: Discover annual key index pages")
    run_seed_discovery(
        db_path=DB_PATH,
        firm=FIRM,
        base_domain=BASE_DOMAIN,
        start_year=2010,
        end_year=2025,
        per_prefix_limit=100,
    )

    print("\nSTEP 2: Download seed captures")
    download_pending_captures(
        db_path=DB_PATH,
        output_dir=OUTPUT_DIR,
        batch_size=200,
        delay_between_downloads=1.5,
    )

    print("\nSTEP 3: If linked_targets already exist, resolve/download them too")
    try:
        resolve_and_download_pending_linked_targets(
            db_path=DB_PATH,
            output_dir=OUTPUT_DIR,
            batch_size=200,
            delay_between_items=1.0,
        )
    except sqlite3.OperationalError:
        print("linked_targets table not created yet. Run parse_investor_pages.py first.")