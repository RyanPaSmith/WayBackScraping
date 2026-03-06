"""
Analyze parsed investor/disclosure data from the updated pipeline
"""
import sqlite3


def analyze_investor_data(db_path: str) -> None:
    conn = sqlite3.connect(db_path, timeout=60)
    conn.row_factory = sqlite3.Row

    try:
        cur = conn.cursor()

        print("=" * 80)
        print("INVESTOR DISCLOSURE ANALYSIS")
        print("=" * 80)

        cur.execute("SELECT COUNT(*) AS cnt FROM captures")
        print(f"\nTotal captures in DB: {cur.fetchone()['cnt']}")

        cur.execute("SELECT COUNT(*) AS cnt FROM parsed_data")
        print(f"Parsed captures:      {cur.fetchone()['cnt']}")

        cur.execute("SELECT COUNT(*) AS cnt FROM disclosure_items")
        disclosure_count = cur.fetchone()['cnt']
        print(f"Disclosure items:     {disclosure_count}")

        cur.execute("SELECT COUNT(*) AS cnt FROM linked_targets")
        linked_count = cur.fetchone()['cnt']
        print(f"Linked targets:       {linked_count}")

        print("\n📄 Parsed pages by class")
        print("-" * 60)
        cur.execute("""
            SELECT page_class, COUNT(*) AS cnt
            FROM parsed_data
            GROUP BY page_class
            ORDER BY cnt DESC
        """)
        for row in cur.fetchall():
            print(f"  {row['page_class']:20s}: {row['cnt']:4d}")

        print("\n📚 Parsed pages by type")
        print("-" * 60)
        cur.execute("""
            SELECT page_type, COUNT(*) AS cnt
            FROM parsed_data
            GROUP BY page_type
            ORDER BY cnt DESC
        """)
        for row in cur.fetchall():
            print(f"  {row['page_type']:25s}: {row['cnt']:4d}")

        print("\n🔗 Linked target status")
        print("-" * 60)
        cur.execute("""
            SELECT status, COUNT(*) AS cnt
            FROM linked_targets
            GROUP BY status
            ORDER BY cnt DESC
        """)
        for row in cur.fetchall():
            print(f"  {row['status']:20s}: {row['cnt']:4d}")

        print("\n🧾 Disclosure items by year")
        print("-" * 60)
        cur.execute("""
            SELECT snapshot_year, COUNT(*) AS cnt
            FROM disclosure_items
            GROUP BY snapshot_year
            ORDER BY snapshot_year
        """)
        for row in cur.fetchall():
            print(f"  {row['snapshot_year']}: {row['cnt']:4d}")

        print("\n📝 Sample disclosure items")
        print("-" * 60)
        cur.execute("""
            SELECT snapshot_year, page_type, item_date, headline, linked_url
            FROM disclosure_items
            ORDER BY snapshot_year, item_date, headline
            LIMIT 15
        """)
        rows = cur.fetchall()
        if rows:
            for row in rows:
                print(f"  [{row['snapshot_year']}] {row['page_type']}")
                print(f"    Date: {row['item_date'] or row['snapshot_year']}")
                print(f"    Headline: {row['headline']}")
                print(f"    URL: {row['linked_url'][:100]}")
        else:
            print("  No disclosure items extracted yet")

        print("\n📦 Downloaded linked documents/pages")
        print("-" * 60)
        cur.execute("""
            SELECT c.snapshot_year, c.source_type, c.page_key, c.url, c.local_path
            FROM captures c
            WHERE c.source_type = 'linked_target'
              AND c.fetch_status = 'downloaded'
            ORDER BY c.snapshot_year, c.id
            LIMIT 15
        """)
        rows = cur.fetchall()
        if rows:
            for row in rows:
                print(f"  [{row['snapshot_year']}] {row['page_key']} ({row['source_type']})")
                print(f"    {row['url'][:100]}")
                print(f"    {row['local_path']}")
        else:
            print("  No linked targets downloaded yet")

        print("\n" + "=" * 80)

    finally:
        conn.close()


if __name__ == "__main__":
    DB_PATH = "apple_investor_pages.db"
    analyze_investor_data(DB_PATH)