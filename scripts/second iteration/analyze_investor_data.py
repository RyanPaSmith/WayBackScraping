"""
Analyze parsed investor pages and display key findings
"""
import sqlite3
import json
from collections import defaultdict


def analyze_investor_data(db_path: str) -> None:
    """Analyze the parsed investor data and display insights"""
    
    conn = sqlite3.connect(db_path, timeout=60)
    conn.row_factory = sqlite3.Row
    
    try:
        cur = conn.cursor()
        
        # Check if parsed data exists
        cur.execute("SELECT COUNT(*) as cnt FROM parsed_data")
        count = cur.fetchone()['cnt']
        
        if count == 0:
            print("No parsed data found. Run parse_investor_pages.py first!")
            return
        
        print("="*80)
        print(f"INVESTOR PAGE ANALYSIS ({count} pages parsed)")
        print("="*80)
        
        # 1. Show what types of pages were found
        print("\n📄 Page Types Found:")
        print("-" * 60)
        cur.execute("""
            SELECT page_type, COUNT(*) as cnt, page_title
            FROM parsed_data
            GROUP BY page_type
            ORDER BY cnt DESC
        """)
        
        for row in cur.fetchall():
            print(f"  {row['page_type']:25s}: {row['cnt']:2d} page(s)")
        
        # 2. Show pages with financial data
        print("\n💰 Pages with Financial Information:")
        print("-" * 60)
        cur.execute("""
            SELECT c.snapshot_year, c.url, p.page_title, p.page_type
            FROM parsed_data p
            JOIN captures c ON p.capture_id = c.id
            WHERE p.has_financial_data = 1
            ORDER BY c.snapshot_year
        """)
        
        for row in cur.fetchall():
            print(f"  [{row['snapshot_year']}] {row['page_type']}")
            print(f"    {row['page_title']}")
            print(f"    {row['url'][:80]}")
            print()
        
        # 3. Show SEC filing pages
        print("\n📊 Pages with SEC Filings:")
        print("-" * 60)
        cur.execute("""
            SELECT c.snapshot_year, c.url, p.page_title
            FROM parsed_data p
            JOIN captures c ON p.capture_id = c.id
            WHERE p.has_sec_filings = 1
            ORDER BY c.snapshot_year
        """)
        
        for row in cur.fetchall():
            print(f"  [{row['snapshot_year']}] {row['page_title']}")
            print(f"    {row['url'][:80]}")
            print()
        
        # 4. Analyze document links found
        print("\n📎 Document Links Found (PDFs, etc.):")
        print("-" * 60)
        
        cur.execute("""
            SELECT c.snapshot_year, p.document_links_json
            FROM parsed_data p
            JOIN captures c ON p.capture_id = c.id
            WHERE p.document_links_json IS NOT NULL
            AND p.document_links_json != '[]'
        """)
        
        total_docs = 0
        doc_types = defaultdict(int)
        
        for row in cur.fetchall():
            docs = json.loads(row['document_links_json'])
            total_docs += len(docs)
            
            for doc in docs:
                url = doc.get('url', '').lower()
                if '.pdf' in url:
                    doc_types['PDF'] += 1
                elif '.xls' in url:
                    doc_types['Excel'] += 1
                elif '.doc' in url:
                    doc_types['Word'] += 1
                else:
                    doc_types['Other'] += 1
        
        print(f"  Total documents found: {total_docs}")
        for dtype, count in sorted(doc_types.items(), key=lambda x: x[1], reverse=True):
            print(f"    {dtype}: {count}")
        
        # 5. Show some example document links
        print("\n📋 Sample Document Links:")
        print("-" * 60)
        
        cur.execute("""
            SELECT c.snapshot_year, c.url as source_url, p.document_links_json
            FROM parsed_data p
            JOIN captures c ON p.capture_id = c.id
            WHERE p.document_links_json IS NOT NULL
            AND p.document_links_json != '[]'
            LIMIT 3
        """)
        
        for row in cur.fetchall():
            docs = json.loads(row['document_links_json'])
            print(f"\n  From: {row['source_url'][:70]}")
            for doc in docs[:5]:  # Show first 5 docs
                print(f"    • {doc.get('text', 'Untitled')}")
                print(f"      {doc.get('url', '')[:70]}")
        
        # 6. Analyze links by category
        print("\n\n🔗 Links by Category:")
        print("-" * 60)
        
        cur.execute("SELECT links_json FROM parsed_data WHERE links_json IS NOT NULL")
        
        link_categories = defaultdict(int)
        
        for row in cur.fetchall():
            links = json.loads(row['links_json'])
            for link in links:
                category = link.get('category', 'unknown')
                link_categories[category] += 1
        
        for category, count in sorted(link_categories.items(), key=lambda x: x[1], reverse=True):
            print(f"  {category:20s}: {count:4d} links")
        
        # 7. Show text samples
        print("\n\n📝 Sample Text from Investor Pages:")
        print("-" * 60)
        
        cur.execute("""
            SELECT c.snapshot_year, p.page_title, p.main_text, p.page_type
            FROM parsed_data p
            JOIN captures c ON p.capture_id = c.id
            WHERE p.main_text IS NOT NULL
            LIMIT 2
        """)
        
        for row in cur.fetchall():
            print(f"\n  [{row['snapshot_year']}] {row['page_type']} - {row['page_title']}")
            print("  " + "-" * 70)
            # Show first 300 characters
            text = row['main_text'][:300].replace('\n', ' ')
            print(f"  {text}...")
        
        # 8. Summary statistics
        print("\n\n📈 Summary Statistics:")
        print("-" * 60)
        
        cur.execute("SELECT MIN(snapshot_year) as min_year, MAX(snapshot_year) as max_year FROM captures")
        years = cur.fetchone()
        print(f"  Years covered: {years['min_year']} - {years['max_year']}")
        
        cur.execute("SELECT COUNT(DISTINCT snapshot_year) as cnt FROM captures")
        print(f"  Unique years: {cur.fetchone()['cnt']}")
        
        cur.execute("SELECT COUNT(DISTINCT url) as cnt FROM captures")
        print(f"  Unique URLs: {cur.fetchone()['cnt']}")
        
        cur.execute("SELECT SUM(has_financial_data) as cnt FROM parsed_data")
        print(f"  Pages with financial data: {cur.fetchone()['cnt']}")
        
        cur.execute("SELECT SUM(has_sec_filings) as cnt FROM parsed_data")
        print(f"  Pages mentioning SEC filings: {cur.fetchone()['cnt']}")
        
        print("\n" + "="*80)
        
    finally:
        conn.close()


def show_specific_page(db_path: str, capture_id: int) -> None:
    """Show detailed information about a specific page"""
    
    conn = sqlite3.connect(db_path, timeout=60)
    conn.row_factory = sqlite3.Row
    
    try:
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                c.url, c.snapshot_year, c.wayback_timestamp,
                p.page_title, p.page_type, p.main_text, 
                p.links_json, p.document_links_json
            FROM parsed_data p
            JOIN captures c ON p.capture_id = c.id
            WHERE c.id = ?
        """, (capture_id,))
        
        row = cur.fetchone()
        
        if not row:
            print(f"No parsed data found for capture {capture_id}")
            return
        
        print("="*80)
        print(f"PAGE DETAILS - Capture #{capture_id}")
        print("="*80)
        
        print(f"\nTitle: {row['page_title']}")
        print(f"Type: {row['page_type']}")
        print(f"Year: {row['snapshot_year']}")
        print(f"Wayback Timestamp: {row['wayback_timestamp']}")
        print(f"URL: {row['url']}")
        
        print("\n" + "-"*80)
        print("MAIN TEXT (first 1000 chars):")
        print("-"*80)
        print(row['main_text'][:1000])
        
        if row['document_links_json']:
            docs = json.loads(row['document_links_json'])
            print("\n" + "-"*80)
            print(f"DOCUMENT LINKS ({len(docs)}):")
            print("-"*80)
            for doc in docs:
                print(f"  • {doc.get('text', 'Untitled')}")
                print(f"    {doc.get('url', '')}")
        
        if row['links_json']:
            all_links = json.loads(row['links_json'])
            print("\n" + "-"*80)
            print(f"ALL LINKS ({len(all_links)}):")
            print("-"*80)
            
            by_category = defaultdict(list)
            for link in all_links:
                by_category[link.get('category', 'other')].append(link)
            
            for category, links in sorted(by_category.items()):
                if links:
                    print(f"\n  {category.upper()} ({len(links)}):")
                    for link in links[:5]:  # Show first 5
                        print(f"    • {link.get('text', 'Untitled')[:60]}")
                    if len(links) > 5:
                        print(f"    ... and {len(links)-5} more")
        
    finally:
        conn.close()


if __name__ == "__main__":
    DB_PATH = "apple_investor_pages.db"
    
    # Run analysis
    analyze_investor_data(DB_PATH)
    
    # Optionally show detailed view of first page
    print("\n\n")
    print("To see details of a specific page, use:")
    print("  show_specific_page('apple_investor_pages.db', capture_id)")
