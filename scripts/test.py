import os
import re
import time
import sqlite3
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit, quote

import requests


CDX_ENDPOINT = "https://web.archive.org/cdx/search/cdx"
WAYBACK_FETCH = "https://web.archive.org/web/{timestamp}id_/{url}"
USER_AGENT = "wayback-disclosure-research/1.0 (contact: your_email@example.com)"
CDX_CONNECT_TIMEOUT = 10
CDX_READ_TIMEOUT = 120


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
    # Option B additions
    "stock",
    "stock-price",
    "shareholder",
    "events",
    "event",
]

DISCOVERY_PREFIX_TEMPLATES = [
    "https://www.{d}/investor",
    "https://www.{d}/investors",
    "https://www.{d}/investor-relations",
    "https://www.{d}/investorrelations",
    "https://www.{d}/sec",
    "https://www.{d}/filings",
    "https://www.{d}/earnings",
    "https://www.{d}/newsroom",
    "https://www.{d}/press",
    "http://www.{d}/investor",
    "http://www.{d}/investors",
    "http://www.{d}/investor-relations",
    "http://www.{d}/investorrelations",
    "http://www.{d}/sec",
    "http://www.{d}/filings",
    "http://www.{d}/earnings",
    "http://www.{d}/newsroom",
    "http://www.{d}/press",
    "http://www.{d}:80/investor",
    "http://www.{d}:80/investors",
    "http://www.{d}:80/investor-relations",
    "http://www.{d}:80/investorrelations",

    "https://investor.{d}/",
    "https://ir.{d}/",
    "http://investor.{d}/",
    "http://ir.{d}/",

    "https://investor.{d}/stock",
    "https://investor.{d}/stock-price",
    "https://investor.{d}/events",
    "https://investor.{d}/events-and-presentations",
    "https://investor.{d}/investor-relations",
    "https://investor.{d}/investor-relations/default.aspx",
    "https://investor.{d}/investor-relations/sec-filings",
    "http://investor.{d}/stock",
    "http://investor.{d}/stock-price",
    "http://investor.{d}/events",
    "http://investor.{d}/events-and-presentations",
    "http://investor.{d}/investor-relations",
    "http://investor.{d}/investor-relations/default.aspx",
    "http://investor.{d}/investor-relations/sec-filings",
]


def init_db(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    # IMPORTANT: WAL + busy timeout helps a lot on Windows
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA busy_timeout=60000;")  # wait up to 60s if locked

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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            UNIQUE (firm, wayback_timestamp, url)
        );
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
    return quote(s, safe="").replace("%", "_")


def is_investor_related(url: str, keywords: list[str]) -> bool:
    u = url.lower()
    return any(k in u for k in keywords)


def cdx_get_json_with_retries(session: requests.Session, params: dict, max_attempts: int = 8) -> list:
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

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status in (429, 500, 502, 503, 504):
                sleep_s = base_sleep * (2 ** (attempt - 1))
                sleep_s += (attempt * 0.3)
                print(f"CDX HTTP {status}. Retry {attempt}/{max_attempts} after {sleep_s:.1f}s")
                time.sleep(sleep_s)
                continue
            raise

        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout):
            sleep_s = base_sleep * (2 ** (attempt - 1))
            sleep_s += (attempt * 0.3)
            print(f"CDX timeout. Retry {attempt}/{max_attempts} after {sleep_s:.1f}s")
            time.sleep(sleep_s)

        except requests.exceptions.RequestException as e:
            sleep_s = base_sleep * (2 ** (attempt - 1))
            sleep_s += (attempt * 0.3)
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
    per_prefix_limit: int = 200,
    use_cdx_keyword_filter: bool = True,
    use_domain_wide_discovery: bool = False,
    domain_wide_limit: int = 5000,
) -> list[str]:
    originals_all: list[str] = []

    keyword_filter = None
    if use_cdx_keyword_filter:
        pattern = "|".join([re.escape(k) for k in CDX_HIGH_SIGNAL])
        keyword_filter = f"original:.*({pattern}).*"

    if use_domain_wide_discovery:
        params = {
            "url": base_domain,
            "matchType": "domain",
            "from": str(year),
            "to": str(year),
            "output": "json",
            "fl": "original",
            "filter": ["statuscode:200"],
            "collapse": "urlkey",
            "limit": str(domain_wide_limit),
        }
        if keyword_filter:
            params["filter"].append(keyword_filter)

        data = cdx_get_json_with_retries(session, params)
        if data and len(data) >= 2:
            originals = [row[0] for row in data[1:] if row and row[0]]
            originals_all.extend(originals)
    else:
        prefixes = build_discovery_prefixes(base_domain)
        for pref in prefixes:
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
            if keyword_filter:
                params["filter"].append(keyword_filter)

            data = cdx_get_json_with_retries(session, params)
            if not data or len(data) < 2:
                time.sleep(0.15)
                continue

            originals = [row[0] for row in data[1:] if row and row[0]]
            originals_all.extend(originals)

            time.sleep(0.25)

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
    limit: int = 2000
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

    def parse_ts(ts: str) -> datetime:
        return datetime.strptime(ts, "%Y%m%d%H%M%S")

    return min(captures, key=lambda c: abs((parse_ts(c["timestamp"]) - target).total_seconds()))


def insert_capture_row(cur: sqlite3.Cursor, firm: str, base_domain: str, row: dict) -> None:
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


def run_scalable_discovery(
    db_path: str,
    firm: str,
    base_domain: str,
    start_year: int,
    end_year: int,
    keywords: list[str],
    per_prefix_limit: int = 200,
    max_filtered_urls_per_year: int = 200,
    use_cdx_keyword_filter: bool = True,
    use_domain_wide_discovery: bool = False,
    domain_wide_limit: int = 5000,
    commit_every: int = 50,
) -> None:
    # One connection for the entire run -> avoids "database is locked"
    conn = sqlite3.connect(db_path, timeout=60)
    try:
        init_db(conn)
        cur = conn.cursor()

        with requests.Session() as session:
            session.headers.update({"User-Agent": USER_AGENT})

            for year in range(start_year, end_year + 1):
                print(f"\n=== {firm} {year} ===")

                urls = cdx_discover_urls_for_year(
                    session=session,
                    base_domain=base_domain,
                    year=year,
                    per_prefix_limit=per_prefix_limit,
                    use_cdx_keyword_filter=use_cdx_keyword_filter,
                    use_domain_wide_discovery=use_domain_wide_discovery,
                    domain_wide_limit=domain_wide_limit,
                )
                print(f"Discovered {len(urls)} deduped URLs (post-discovery)")

                filtered = [u for u in urls if is_investor_related(u, keywords)]
                print(f"Filtered to {len(filtered)} investor-related URLs")

                if len(filtered) > max_filtered_urls_per_year:
                    filtered = filtered[:max_filtered_urls_per_year]
                    print(f"Capped to first {len(filtered)} filtered URLs for safety/testing")

                inserted = 0
                pending_since_commit = 0

                for u in filtered:
                    captures = cdx_get_captures_for_url_year(session, u, year)
                    chosen = choose_capture_closest_to_midyear(captures, year)
                    if not chosen:
                        continue

                    insert_capture_row(cur, firm, base_domain, chosen)
                    inserted += 1
                    pending_since_commit += 1

                    if pending_since_commit >= commit_every:
                        conn.commit()
                        pending_since_commit = 0

                    time.sleep(0.2)

                if pending_since_commit > 0:
                    conn.commit()

                print(f"Inserted {inserted} chosen captures for {year}")

    finally:
        conn.close()


if __name__ == "__main__":
    DB_PATH = "first-database.db"
    FIRM = "AAPL"
    BASE_DOMAIN = "apple.com"

    run_scalable_discovery(
        db_path=DB_PATH,
        firm=FIRM,
        base_domain=BASE_DOMAIN,
        start_year=2010,
        end_year=2010,
        keywords=INVESTOR_KEYWORDS,
        per_prefix_limit=200,
        max_filtered_urls_per_year=200,
        use_cdx_keyword_filter=True,
        use_domain_wide_discovery=True,
        domain_wide_limit=1000,
        commit_every=25,  # smaller commits while testing
    )
