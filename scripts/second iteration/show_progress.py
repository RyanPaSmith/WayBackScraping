import sqlite3


def show_progress(db_path: str = "apple_investor_pages.db") -> None:
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        print("=" * 80)
        print("INVESTOR DISCLOSURE PIPELINE - PROGRESS REPORT")
        print("=" * 80)

        cur.execute("SELECT COUNT(*) AS cnt FROM captures")
        total_captures = cur.fetchone()["cnt"]

        cur.execute("SELECT COUNT(*) AS cnt FROM captures WHERE fetch_status = 'downloaded'")
        downloaded = cur.fetchone()["cnt"]

        cur.execute("SELECT COUNT(*) AS cnt FROM captures WHERE fetch_status = 'pending'")
        pending = cur.fetchone()["cnt"]

        cur.execute("SELECT COUNT(*) AS cnt FROM captures WHERE fetch_status = 'failed'")
        failed = cur.fetchone()["cnt"]

        print("\n📦 Capture download status")
        print(f"  Total captures: {total_captures}")
        print(f"  Downloaded:     {downloaded}")
        print(f"  Pending:        {pending}")
        print(f"  Failed:         {failed}")

        try:
            cur.execute("SELECT COUNT(*) AS cnt FROM parsed_data")
            parsed_pages = cur.fetchone()["cnt"]

            cur.execute("SELECT COUNT(*) AS cnt FROM disclosure_items")
            disclosure_items = cur.fetchone()["cnt"]

            cur.execute("SELECT COUNT(*) AS cnt FROM linked_targets")
            linked_targets = cur.fetchone()["cnt"]

            print("\n🧠 Parsing / extraction")
            print(f"  Parsed pages:      {parsed_pages}")
            print(f"  Disclosure items:  {disclosure_items}")
            print(f"  Linked targets:    {linked_targets}")

            cur.execute("""
                SELECT status, COUNT(*) AS cnt
                FROM linked_targets
                GROUP BY status
                ORDER BY cnt DESC
            """)
            rows = cur.fetchall()
            print("\n🔗 Linked target status")
            for row in rows:
                print(f"  {row['status']:15s}: {row['cnt']}")
        except sqlite3.OperationalError:
            print("\n🧠 Parsing / extraction")
            print("  Parsing tables not created yet")

        print("\n📅 Captures by year")
        print("-" * 70)
        cur.execute("""
            SELECT
                snapshot_year,
                COUNT(*) AS total,
                SUM(CASE WHEN fetch_status = 'downloaded' THEN 1 ELSE 0 END) AS downloaded,
                SUM(CASE WHEN fetch_status = 'pending' THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN fetch_status = 'failed' THEN 1 ELSE 0 END) AS failed
            FROM captures
            GROUP BY snapshot_year
            ORDER BY snapshot_year
        """)
        for row in cur.fetchall():
            print(
                f"  {row['snapshot_year']}: total={row['total']:3d} "
                f"downloaded={row['downloaded'] or 0:3d} "
                f"pending={row['pending'] or 0:3d} "
                f"failed={row['failed'] or 0:3d}"
            )

        conn.close()
        print("\n" + "=" * 80)

    except sqlite3.OperationalError as exc:
        print(f"Database not found or not initialized yet: {exc}")


if __name__ == "__main__":
    show_progress()