"""
Monitor progress of the Apple 2010-2025 scraping job
"""
import sqlite3
from collections import defaultdict


def show_progress(db_path: str = "apple_investor_pages.db") -> None:
    """Show current progress of the scraping job"""
    
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        print("="*80)
        print("APPLE INVESTOR RESEARCH - PROGRESS REPORT")
        print("="*80)
        
        # Overall stats
        cur.execute("SELECT COUNT(*) as total FROM captures")
        total = cur.fetchone()['total']
        
        cur.execute("SELECT COUNT(*) as cnt FROM captures WHERE fetch_status = 'pending'")
        pending = cur.fetchone()['cnt']
        
        cur.execute("SELECT COUNT(*) as cnt FROM captures WHERE fetch_status = 'downloaded'")
        downloaded = cur.fetchone()['cnt']
        
        cur.execute("SELECT COUNT(*) as cnt FROM captures WHERE fetch_status = 'failed'")
        failed = cur.fetchone()['cnt']
        
        print(f"\n📊 Overall Status:")
        print(f"  Total captures discovered: {total}")
        print(f"  ✓ Downloaded:              {downloaded}")
        print(f"  ⏳ Pending:                 {pending}")
        print(f"  ✗ Failed:                  {failed}")
        
        if total > 0:
            pct = (downloaded / total) * 100
            print(f"  Progress:                  {pct:.1f}%")
        
        # By year
        print(f"\n📅 Progress by Year:")
        print("-" * 70)
        
        cur.execute("""
            SELECT 
                snapshot_year,
                COUNT(*) as total,
                SUM(CASE WHEN fetch_status = 'downloaded' THEN 1 ELSE 0 END) as downloaded,
                SUM(CASE WHEN fetch_status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN fetch_status = 'failed' THEN 1 ELSE 0 END) as failed
            FROM captures
            GROUP BY snapshot_year
            ORDER BY snapshot_year
        """)
        
        for row in cur.fetchall():
            year = row['snapshot_year']
            total = row['total']
            dl = row['downloaded']
            pend = row['pending']
            fail = row['failed']
            
            pct = (dl / total * 100) if total > 0 else 0
            bar_length = 30
            filled = int(bar_length * pct / 100)
            bar = '█' * filled + '░' * (bar_length - filled)
            
            print(f"  {year}: [{bar}] {pct:5.1f}% | {dl:3d}/{total:3d} (pending: {pend}, failed: {fail})")
        
        # Parsing status
        try:
            cur.execute("SELECT COUNT(*) as cnt FROM parsed_data")
            parsed = cur.fetchone()['cnt']
            print(f"\n📝 Parsing Status:")
            print(f"  Pages parsed: {parsed}")
            if downloaded > 0:
                parse_pct = (parsed / downloaded * 100)
                print(f"  Progress:     {parse_pct:.1f}% of downloaded pages")
        except sqlite3.OperationalError:
            print(f"\n📝 Parsing Status:")
            print(f"  Not yet started (run parse_investor_pages.py)")
        
        # Show some recent activity
        print(f"\n🕒 Recent Downloads:")
        print("-" * 70)
        cur.execute("""
            SELECT snapshot_year, url, downloaded_at
            FROM captures
            WHERE fetch_status = 'downloaded'
            ORDER BY downloaded_at DESC
            LIMIT 5
        """)
        
        recent = cur.fetchall()
        if recent:
            for row in recent:
                time = row['downloaded_at'] or 'Unknown'
                print(f"  [{row['snapshot_year']}] {time}")
                print(f"    {row['url'][:70]}")
        else:
            print("  No downloads yet")
        
        # Next up
        print(f"\n⏭️  Next in Queue:")
        print("-" * 70)
        cur.execute("""
            SELECT snapshot_year, url
            FROM captures
            WHERE fetch_status = 'pending'
            ORDER BY snapshot_year, id
            LIMIT 5
        """)
        
        next_items = cur.fetchall()
        if next_items:
            for row in next_items:
                print(f"  [{row['snapshot_year']}] {row['url'][:70]}")
        else:
            print("  No pending downloads!")
        
        print("\n" + "="*80)
        
        conn.close()
        
    except sqlite3.OperationalError as e:
        print(f"Database not found or not initialized yet: {e}")
        print("\nRun wayback_scraper_fixed.py to start the discovery process")


if __name__ == "__main__":
    import sys
    import time
    
    # If run with --watch, monitor continuously
    if '--watch' in sys.argv:
        print("Monitoring mode - press Ctrl+C to stop\n")
        try:
            while True:
                show_progress()
                print("\nRefreshing in 30 seconds...\n")
                time.sleep(30)
        except KeyboardInterrupt:
            print("\n\nStopped monitoring.")
    else:
        show_progress()
