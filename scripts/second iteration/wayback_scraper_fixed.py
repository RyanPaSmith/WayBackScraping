import os
import re
import time
import sqlite3
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit, quote
from pathlib import Path

import requests


CDX_ENDPOINT = "https://web.archive.org/cdx/search/cdx"
WAYBACK_FETCH = "https://web.archive.org/web/{timestamp}id_/{url}"
USER_AGENT = "wayback-disclosure-research/1.0 (contact: your_email@example.com)"
CDX_CONNECT_TIMEOUT = 15
CDX_READ_TIMEOUT = 180
DOWNLOAD_TIMEOUT = 60


INVESTOR_KEYWORDS = [
    "investor", "investors", "investor-relations", "investorrelations", "ir",
    "sec", "filing", "filings", "10-k", "10q", "10-q", "8-k",
    "earnings", "results", "quarterly", "annual", "report",
    "presentation", "presentations", "events", "event",
    "press", "newsroom", "media",
]

CDX_HIGH_SIGNAL = [
    "investor",
    "investor-relations",
    "sec",
    "filing",
    "filings",
    "10-k",
    "10-q",
    "10q",
    "8-k",
    "earnings",
    "presentation",
    "presentations",
    "stock",
    "stock-price",
    "shareholder",
    "events",
    "event",
]

# Simplified discovery - focus on high-value prefixes only
DISCOVERY_PREFIX_TEMPLATES = [
    "https://www.{d}/investor",
    "https://www.{d}/investors",
    "https://www.{d}/investor-relations",
    "https://investor.{d}/",
    "https://ir.{d}/",
    "http://investor.{d}/",
    "http://ir.{d}/",
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
            fetch_status TEXT NOT NULL,
            error_message TEXT,

            local_path TEXT,
            downloaded_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            UNIQUE (firm, wayback_timestamp, url)
        );
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_fetch_status 
        ON captures(fetch_status);
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_firm_year 
        ON captures(firm, snapshot_year);
    """)
    
    conn.commit()


def canonicalize_url(url: str) -> str:
    url = url.strip()
    if "://" not in url:
        url = "http://" + url

    parts = urlsplit(url)
    scheme = parts.scheme.lower() if parts.scheme else "http"
    netloc = parts.netloc.lower()
    path = parts.path or "/"

    if path != "/" and path.endswith("/"):
        path = path[:-1]

    return urlunsplit((scheme, netloc, path, "", ""))


def safe_filename(s: str) -> str:
    """Create a safe filename from a string"""
    return quote(s, safe="").replace("%", "_")[:200]  # Limit length


def is_investor_related(url: str, keywords: list[str]) -> bool:
    u = url.lower()
    return any(k in u for k in keywords)


def cdx_get_json_with_retries(session: requests.Session, params: dict, max_attempts: int = 5) -> list:
    """Query CDX API with exponential backoff"""
    base_sleep = 5.0

    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(
                CDX_ENDPOINT,
                params=params,
                timeout=(CDX_CONNECT_TIMEOUT, CDX_READ_TIMEOUT),
            )
            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status in (429, 500, 502, 503, 504):
                sleep_s = base_sleep * (2 ** (attempt - 1))
                print(f"CDX HTTP {status}. Retry {attempt}/{max_attempts} after {sleep_s:.1f}s")
                time.sleep(sleep_s)
                continue
            raise

        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout) as e:
            sleep_s = base_sleep * (2 ** (attempt - 1))
            print(f"CDX timeout ({e}). Retry {attempt}/{max_attempts} after {sleep_s:.1f}s")
            time.sleep(sleep_s)

        except requests.exceptions.RequestException as e:
            sleep_s = base_sleep * (2 ** (attempt - 1))
            print(f"CDX request error ({e}). Retry {attempt}/{max_attempts} after {sleep_s:.1f}s")
            time.sleep(sleep_s)

    return []


def build_discovery_prefixes(base_domain: str) -> list[str]:
    d = base_domain.lower().strip()
    prefixes = [t.format(d=d) for t in DISCOVERY_PREFIX_TEMPLATES]
    seen = set()
    out = []
    for p in prefixes:
        if p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out


def cdx_discover_urls_for_year(
    session: requests.Session,
    base_domain: str,
    year: int,
    per_prefix_limit: int = 100,
) -> list[str]:
    """Discover investor-related URLs using prefix-based search (more reliable)"""
    originals_all: list[str] = []

    prefixes = build_discovery_prefixes(base_domain)
    
    for i, pref in enumerate(prefixes, 1):
        print(f"  Checking prefix {i}/{len(prefixes)}: {pref}")
        
        params = {
            "url": pref,
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
            time.sleep(1.0)  # Rate limit between prefixes
            continue

        originals = [row[0] for row in data[1:] if row and row[0]]
        originals_all.extend(originals)
        print(f"    Found {len(originals)} URLs")

        time.sleep(1.5)  # Important: rate limit between requests

    # Deduplicate
    seen = set()
    deduped = []
    for u in originals_all:
        cu = canonicalize_url(u)
        if cu in seen:
            continue
        seen.add(cu)
        deduped.append(cu)

    return deduped


def cdx_get_captures_for_url_year(
    session: requests.Session,
    url: str,
    year: int,
    limit: int = 100
) -> list[dict]:
    """Get all captures for a URL in a given year"""
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
    """Choose the capture closest to July 1st of the given year"""
    if not captures:
        return None

    target = datetime(year, 7, 1, 12, 0, 0)

    def parse_ts(ts: str) -> datetime:
        return datetime.strptime(ts, "%Y%m%d%H%M%S")

    return min(captures, key=lambda c: abs((parse_ts(c["timestamp"]) - target).total_seconds()))


def insert_capture_row(cur: sqlite3.Cursor, firm: str, base_domain: str, row: dict) -> None:
    """Insert a capture record into the database"""
    ts = row["timestamp"]
    year = int(ts[:4])
    original = canonicalize_url(row["original"])

    mime_type = row.get("mimetype")
    status_code = int(row["statuscode"]) if row.get("statuscode") else None
    content_bytes = int(row["length"]) if row.get("length") else None
    digest = row.get("digest")

    cur.execute("""
        INSERT OR IGNORE INTO captures
        (firm, base_domain, url, snapshot_year, wayback_timestamp, mime_type, status_code, content_bytes, digest, fetch_status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        firm,
        base_domain,
        original,
        year,
        ts,
        mime_type,
        status_code,
        content_bytes,
        digest,
        "pending"
    ))


def download_wayback_capture(
    session: requests.Session,
    timestamp: str,
    url: str,
    output_path: str,
    max_attempts: int = 3
) -> tuple[bool, str | None]:
    """Download a single capture from the Wayback Machine"""
    wayback_url = WAYBACK_FETCH.format(timestamp=timestamp, url=url)
    
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(wayback_url, timeout=DOWNLOAD_TIMEOUT, stream=True)
            resp.raise_for_status()
            
            # Create directory if needed
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Write content
            with open(output_path, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            return True, None
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Download attempt {attempt}/{max_attempts} failed: {e}"
            print(f"  {error_msg}")
            
            if attempt < max_attempts:
                time.sleep(2 ** attempt)
            else:
                return False, str(e)
    
    return False, "Max attempts exceeded"


def download_pending_captures(
    db_path: str,
    output_dir: str,
    batch_size: int = 50,
    delay_between_downloads: float = 2.0
) -> None:
    """Download all pending captures from the database"""
    conn = sqlite3.connect(db_path, timeout=60)
    conn.row_factory = sqlite3.Row
    
    try:
        cur = conn.cursor()
        
        # Get pending captures
        cur.execute("""
            SELECT id, firm, url, wayback_timestamp, snapshot_year, mime_type
            FROM captures
            WHERE fetch_status = 'pending'
            ORDER BY snapshot_year, id
            LIMIT ?
        """, (batch_size,))
        
        pending = cur.fetchall()
        
        if not pending:
            print("No pending captures to download")
            return
        
        print(f"\nDownloading {len(pending)} captures...")
        
        with requests.Session() as session:
            session.headers.update({"User-Agent": USER_AGENT})
            
            for i, row in enumerate(pending, 1):
                capture_id = row['id']
                firm = row['firm']
                url = row['url']
                timestamp = row['wayback_timestamp']
                year = row['snapshot_year']
                mime_type = row['mime_type'] or 'unknown'
                
                # Determine file extension
                ext = '.html'
                if 'pdf' in mime_type.lower():
                    ext = '.pdf'
                
                # Create filename
                url_safe = safe_filename(url)
                filename = f"{firm}_{year}_{timestamp}_{url_safe}{ext}"
                local_path = os.path.join(output_dir, firm, str(year), filename)
                
                print(f"\n[{i}/{len(pending)}] Downloading capture {capture_id}")
                print(f"  URL: {url}")
                print(f"  Timestamp: {timestamp}")
                
                success, error = download_wayback_capture(session, timestamp, url, local_path)
                
                if success:
                    cur.execute("""
                        UPDATE captures
                        SET fetch_status = 'downloaded',
                            local_path = ?,
                            downloaded_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (local_path, capture_id))
                    print(f"  ✓ Saved to: {local_path}")
                else:
                    cur.execute("""
                        UPDATE captures
                        SET fetch_status = 'failed',
                            error_message = ?
                        WHERE id = ?
                    """, (error, capture_id))
                    print(f"  ✗ Failed: {error}")
                
                conn.commit()
                
                # Rate limiting
                time.sleep(delay_between_downloads)
        
    finally:
        conn.close()


def run_scalable_discovery(
    db_path: str,
    firm: str,
    base_domain: str,
    start_year: int,
    end_year: int,
    keywords: list[str],
    per_prefix_limit: int = 100,
    max_filtered_urls_per_year: int = 50,
    commit_every: int = 25,
) -> None:
    """Discover investor-related pages and add them to the database"""
    conn = sqlite3.connect(db_path, timeout=60)
    try:
        init_db(conn)
        cur = conn.cursor()

        with requests.Session() as session:
            session.headers.update({"User-Agent": USER_AGENT})

            for year in range(start_year, end_year + 1):
                print(f"\n{'='*60}")
                print(f"Discovering URLs for {firm} - {year}")
                print(f"{'='*60}")

                # Discover URLs
                urls = cdx_discover_urls_for_year(
                    session=session,
                    base_domain=base_domain,
                    year=year,
                    per_prefix_limit=per_prefix_limit,
                )
                print(f"\nDiscovered {len(urls)} unique URLs")

                # Filter for investor-related content
                filtered = [u for u in urls if is_investor_related(u, keywords)]
                print(f"Filtered to {len(filtered)} investor-related URLs")

                if len(filtered) > max_filtered_urls_per_year:
                    filtered = filtered[:max_filtered_urls_per_year]
                    print(f"Capped to {len(filtered)} URLs")

                inserted = 0
                pending_since_commit = 0

                print("\nFinding captures for each URL...")
                for j, u in enumerate(filtered, 1):
                    print(f"  [{j}/{len(filtered)}] {u}")
                    
                    captures = cdx_get_captures_for_url_year(session, u, year)
                    chosen = choose_capture_closest_to_midyear(captures, year)
                    
                    if not chosen:
                        print(f"    No suitable capture found")
                        continue

                    insert_capture_row(cur, firm, base_domain, chosen)
                    inserted += 1
                    pending_since_commit += 1
                    print(f"    ✓ Added capture from {chosen['timestamp']}")

                    if pending_since_commit >= commit_every:
                        conn.commit()
                        pending_since_commit = 0

                    time.sleep(1.0)  # Rate limit

                if pending_since_commit > 0:
                    conn.commit()

                print(f"\n✓ Inserted {inserted} captures for {year}")

    finally:
        conn.close()


if __name__ == "__main__":
    DB_PATH = "apple_investor_pages.db"
    FIRM = "AAPL"
    BASE_DOMAIN = "apple.com"
    OUTPUT_DIR = "downloads"

    # Step 1: Discover URLs and add to database
    print("STEP 1: Discovering investor-related pages...")
    run_scalable_discovery(
        db_path=DB_PATH,
        firm=FIRM,
        base_domain=BASE_DOMAIN,
        start_year=2010,
        end_year=2025,  # Now covering 2010-2025
        keywords=INVESTOR_KEYWORDS,
        per_prefix_limit=100,
        max_filtered_urls_per_year=100,  # Increased from 50
        commit_every=10,
    )

    # Step 2: Download the discovered pages
    print("\n\nSTEP 2: Downloading discovered pages...")
    download_pending_captures(
        db_path=DB_PATH,
        output_dir=OUTPUT_DIR,
        batch_size=100,  # Process more at once
        delay_between_downloads=2.0,
    )
    
    print("\n✓ Complete!")
