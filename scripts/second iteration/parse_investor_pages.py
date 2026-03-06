"""
Parse downloaded investor pages to extract useful information
"""
import os
import re
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
from datetime import datetime


def init_parsed_data_table(conn: sqlite3.Connection) -> None:
    """Create table to store parsed information"""
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS parsed_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            capture_id INTEGER NOT NULL,
            
            -- Extracted information
            page_title TEXT,
            page_type TEXT,  -- 'investor_home', 'sec_filing', 'earnings', 'presentation', etc.
            
            -- Links found
            links_json TEXT,  -- JSON array of {url, text, type}
            
            -- Documents mentioned
            document_links_json TEXT,  -- JSON array of PDF/doc links
            
            -- Text content
            main_text TEXT,
            
            -- Metadata
            has_financial_data BOOLEAN,
            has_sec_filings BOOLEAN,
            has_earnings_info BOOLEAN,
            has_presentations BOOLEAN,
            
            -- Timestamps
            parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            FOREIGN KEY (capture_id) REFERENCES captures(id),
            UNIQUE(capture_id)
        );
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_parsed_capture 
        ON parsed_data(capture_id);
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_page_type 
        ON parsed_data(page_type);
    """)
    
    conn.commit()


def classify_page_type(url: str, title: str, text: str) -> str:
    """Classify what type of investor page this is"""
    url_lower = url.lower()
    title_lower = title.lower() if title else ""
    text_lower = text.lower()[:2000]  # Check first 2000 chars
    
    # Check for specific page types
    if any(x in url_lower for x in ['sec-filing', '/sec/', '/filings']):
        return 'sec_filings'
    
    if any(x in url_lower for x in ['10-k', '10-q', '8-k']):
        return 'sec_filing_detail'
    
    if any(x in url_lower for x in ['earnings', 'quarterly-results', 'financial-results']):
        return 'earnings'
    
    if any(x in url_lower for x in ['presentation', 'presentations', 'events']):
        return 'presentations'
    
    if any(x in url_lower for x in ['annual-report', 'annualreport']):
        return 'annual_report'
    
    if any(x in url_lower for x in ['press-release', 'news', 'newsroom']):
        return 'press'
    
    if any(x in url_lower for x in ['shareholder', 'stockholder']):
        return 'shareholder_info'
    
    if any(x in url_lower for x in ['stock-price', 'stock-quote', 'quote']):
        return 'stock_info'
    
    # Check for IR home page indicators
    if 'investor' in url_lower and url_lower.endswith(('/', '/default.html', '/index.html')):
        return 'investor_home'
    
    return 'other'


def extract_links(soup: BeautifulSoup, base_url: str) -> Dict[str, List[Dict]]:
    """Extract and categorize all links from the page"""
    
    links = {
        'sec_filings': [],
        'presentations': [],
        'earnings': [],
        'annual_reports': [],
        'press_releases': [],
        'documents': [],  # PDFs, docs, etc.
        'other': []
    }
    
    for a in soup.find_all('a', href=True):
        href = a['href'].strip()
        text = a.get_text(strip=True)
        
        if not href or href.startswith(('#', 'javascript:', 'mailto:')):
            continue
        
        # Make absolute URLs (simple version)
        if href.startswith('http'):
            url = href
        elif href.startswith('/'):
            # Extract domain from base_url
            match = re.match(r'(https?://[^/]+)', base_url)
            domain = match.group(1) if match else ''
            url = domain + href
        else:
            url = href  # Relative URL
        
        link_data = {
            'url': url,
            'text': text,
            'href': href
        }
        
        # Categorize the link
        href_lower = href.lower()
        text_lower = text.lower()
        
        # Document links (PDFs, DOCs, etc.)
        if any(href_lower.endswith(ext) for ext in ['.pdf', '.doc', '.docx', '.xls', '.xlsx']):
            links['documents'].append(link_data)
            continue
        
        # SEC filings
        if any(x in href_lower or x in text_lower for x in ['10-k', '10-q', '8-k', 'sec', 'filing']):
            links['sec_filings'].append(link_data)
        
        # Presentations
        elif any(x in href_lower or x in text_lower for x in ['presentation', 'webcast', 'conference-call', 'event']):
            links['presentations'].append(link_data)
        
        # Earnings
        elif any(x in href_lower or x in text_lower for x in ['earnings', 'quarterly-result', 'financial-result']):
            links['earnings'].append(link_data)
        
        # Annual reports
        elif any(x in href_lower or x in text_lower for x in ['annual-report', 'proxy']):
            links['annual_reports'].append(link_data)
        
        # Press releases
        elif any(x in href_lower or x in text_lower for x in ['press', 'news-release', 'announcement']):
            links['press_releases'].append(link_data)
        
        else:
            links['other'].append(link_data)
    
    return links


def extract_main_text(soup: BeautifulSoup) -> str:
    """Extract the main text content from the page"""
    
    # Remove script and style elements
    for element in soup(['script', 'style', 'nav', 'header', 'footer']):
        element.decompose()
    
    # Get text
    text = soup.get_text(separator='\n', strip=True)
    
    # Clean up whitespace
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    text = '\n'.join(lines)
    
    return text


def check_content_flags(text: str, links: Dict) -> Dict[str, bool]:
    """Check for presence of different types of investor information"""
    text_lower = text.lower()
    
    return {
        'has_financial_data': any(x in text_lower for x in [
            'revenue', 'earnings', 'eps', 'net income', 'gross margin',
            'operating income', 'cash flow', 'balance sheet'
        ]),
        'has_sec_filings': len(links['sec_filings']) > 0 or any(x in text_lower for x in [
            '10-k', '10-q', '8-k', 'sec filing', 'edgar'
        ]),
        'has_earnings_info': len(links['earnings']) > 0 or any(x in text_lower for x in [
            'quarterly results', 'earnings report', 'earnings call'
        ]),
        'has_presentations': len(links['presentations']) > 0 or 'presentation' in text_lower
    }


def parse_html_file(file_path: str, url: str) -> Dict:
    """Parse a single HTML file and extract investor information"""
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            html_content = f.read()
    except Exception as e:
        return {'error': f"Failed to read file: {e}"}
    
    # Parse with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Extract page title
    title_tag = soup.find('title')
    page_title = title_tag.get_text(strip=True) if title_tag else ''
    
    # Extract main text
    main_text = extract_main_text(soup)
    
    # Extract links
    links = extract_links(soup, url)
    
    # Classify page type
    page_type = classify_page_type(url, page_title, main_text)
    
    # Check for content flags
    content_flags = check_content_flags(main_text, links)
    
    return {
        'page_title': page_title,
        'page_type': page_type,
        'main_text': main_text[:10000],  # Limit to 10k chars for storage
        'links': links,
        'content_flags': content_flags,
        'error': None
    }


def parse_all_downloaded_captures(db_path: str, downloads_dir: str) -> None:
    """Parse all downloaded HTML files and store results"""
    
    conn = sqlite3.connect(db_path, timeout=60)
    conn.row_factory = sqlite3.Row
    
    try:
        init_parsed_data_table(conn)
        cur = conn.cursor()
        
        # Get all downloaded captures
        cur.execute("""
            SELECT id, url, local_path, snapshot_year, mime_type
            FROM captures
            WHERE fetch_status = 'downloaded'
            AND local_path IS NOT NULL
        """)
        
        captures = cur.fetchall()
        
        if not captures:
            print("No downloaded captures to parse")
            return
        
        print(f"\nParsing {len(captures)} downloaded files...")
        
        for i, capture in enumerate(captures, 1):
            capture_id = capture['id']
            url = capture['url']
            local_path = capture['local_path']
            year = capture['snapshot_year']
            mime_type = capture['mime_type'] or ''
            
            print(f"\n[{i}/{len(captures)}] Parsing capture {capture_id}")
            print(f"  File: {os.path.basename(local_path)}")
            
            # Check if already parsed
            cur.execute("SELECT id FROM parsed_data WHERE capture_id = ?", (capture_id,))
            if cur.fetchone():
                print("  ⊘ Already parsed, skipping")
                continue
            
            # Skip PDFs for now (need different parsing)
            if 'pdf' in mime_type.lower() or local_path.endswith('.pdf'):
                print("  ⊘ PDF file - skipping for now")
                continue
            
            # Check file exists
            if not os.path.exists(local_path):
                print(f"  ✗ File not found: {local_path}")
                continue
            
            # Parse HTML
            result = parse_html_file(local_path, url)
            
            if result.get('error'):
                print(f"  ✗ Parse error: {result['error']}")
                continue
            
            # Store results
            import json
            
            # Combine all links into a single JSON structure
            all_links = []
            for category, link_list in result['links'].items():
                for link in link_list:
                    all_links.append({
                        'category': category,
                        **link
                    })
            
            # Extract just document links
            doc_links = result['links']['documents']
            
            cur.execute("""
                INSERT INTO parsed_data (
                    capture_id, page_title, page_type, links_json, 
                    document_links_json, main_text, has_financial_data,
                    has_sec_filings, has_earnings_info, has_presentations
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                capture_id,
                result['page_title'],
                result['page_type'],
                json.dumps(all_links, indent=2),
                json.dumps(doc_links, indent=2),
                result['main_text'],
                result['content_flags']['has_financial_data'],
                result['content_flags']['has_sec_filings'],
                result['content_flags']['has_earnings_info'],
                result['content_flags']['has_presentations']
            ))
            
            conn.commit()
            
            print(f"  ✓ Parsed successfully")
            print(f"    Title: {result['page_title'][:60]}")
            print(f"    Type: {result['page_type']}")
            print(f"    Links: {len(all_links)} total")
            print(f"    Documents: {len(doc_links)} PDFs/docs")
            print(f"    Flags: financial={result['content_flags']['has_financial_data']}, "
                  f"sec={result['content_flags']['has_sec_filings']}, "
                  f"earnings={result['content_flags']['has_earnings_info']}")
        
        print(f"\n✓ Parsing complete!")
        
    finally:
        conn.close()


def generate_summary_report(db_path: str, output_file: str = "parsing_summary.txt") -> None:
    """Generate a summary report of parsed data"""
    
    conn = sqlite3.connect(db_path, timeout=60)
    conn.row_factory = sqlite3.Row
    
    try:
        cur = conn.cursor()
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("INVESTOR PAGE PARSING SUMMARY\n")
            f.write("=" * 80 + "\n\n")
            
            # Overall stats
            cur.execute("SELECT COUNT(*) as cnt FROM parsed_data")
            total = cur.fetchone()['cnt']
            f.write(f"Total pages parsed: {total}\n\n")
            
            # By page type
            f.write("Pages by Type:\n")
            f.write("-" * 40 + "\n")
            cur.execute("""
                SELECT page_type, COUNT(*) as cnt 
                FROM parsed_data 
                GROUP BY page_type 
                ORDER BY cnt DESC
            """)
            for row in cur.fetchall():
                f.write(f"  {row['page_type']:20s}: {row['cnt']:3d}\n")
            
            # Content flags
            f.write("\nContent Analysis:\n")
            f.write("-" * 40 + "\n")
            cur.execute("SELECT COUNT(*) as cnt FROM parsed_data WHERE has_financial_data = 1")
            f.write(f"  Has financial data:  {cur.fetchone()['cnt']}\n")
            
            cur.execute("SELECT COUNT(*) as cnt FROM parsed_data WHERE has_sec_filings = 1")
            f.write(f"  Has SEC filings:     {cur.fetchone()['cnt']}\n")
            
            cur.execute("SELECT COUNT(*) as cnt FROM parsed_data WHERE has_earnings_info = 1")
            f.write(f"  Has earnings info:   {cur.fetchone()['cnt']}\n")
            
            cur.execute("SELECT COUNT(*) as cnt FROM parsed_data WHERE has_presentations = 1")
            f.write(f"  Has presentations:   {cur.fetchone()['cnt']}\n")
            
            # Detailed page list
            f.write("\n" + "=" * 80 + "\n")
            f.write("DETAILED PAGE LIST\n")
            f.write("=" * 80 + "\n\n")
            
            cur.execute("""
                SELECT 
                    c.snapshot_year,
                    c.url,
                    p.page_title,
                    p.page_type,
                    p.has_financial_data,
                    p.has_sec_filings,
                    p.has_earnings_info
                FROM parsed_data p
                JOIN captures c ON p.capture_id = c.id
                ORDER BY c.snapshot_year, p.page_type
            """)
            
            for row in cur.fetchall():
                f.write(f"{row['snapshot_year']} | {row['page_type']:20s}\n")
                f.write(f"  Title: {row['page_title']}\n")
                f.write(f"  URL:   {row['url']}\n")
                flags = []
                if row['has_financial_data']:
                    flags.append('financial')
                if row['has_sec_filings']:
                    flags.append('sec')
                if row['has_earnings_info']:
                    flags.append('earnings')
                if flags:
                    f.write(f"  Flags: {', '.join(flags)}\n")
                f.write("\n")
        
        print(f"\n✓ Summary report written to: {output_file}")
        
    finally:
        conn.close()


def export_links_to_csv(db_path: str, output_file: str = "extracted_links.csv") -> None:
    """Export all extracted links to a CSV file"""
    import csv
    import json
    
    conn = sqlite3.connect(db_path, timeout=60)
    conn.row_factory = sqlite3.Row
    
    try:
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                c.firm,
                c.snapshot_year,
                c.url as source_url,
                p.page_type,
                p.links_json
            FROM parsed_data p
            JOIN captures c ON p.capture_id = c.id
            WHERE p.links_json IS NOT NULL
        """)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Firm', 'Year', 'Source_URL', 'Page_Type', 
                           'Link_Category', 'Link_URL', 'Link_Text'])
            
            for row in cur.fetchall():
                links = json.loads(row['links_json'])
                
                for link in links:
                    writer.writerow([
                        row['firm'],
                        row['snapshot_year'],
                        row['source_url'],
                        row['page_type'],
                        link.get('category', ''),
                        link.get('url', ''),
                        link.get('text', '')[:100]  # Truncate long text
                    ])
        
        print(f"\n✓ Links exported to: {output_file}")
        
    finally:
        conn.close()


if __name__ == "__main__":
    DB_PATH = "apple_investor_pages.db"
    DOWNLOADS_DIR = "downloads"
    
    print("="*60)
    print("Parsing Downloaded Investor Pages")
    print("="*60)
    
    # Parse all downloaded HTML files
    parse_all_downloaded_captures(DB_PATH, DOWNLOADS_DIR)
    
    # Generate summary report
    generate_summary_report(DB_PATH, "parsing_summary.txt")
    
    # Export links to CSV
    export_links_to_csv(DB_PATH, "extracted_links.csv")
    
    print("\n" + "="*60)
    print("✓ All parsing complete!")
    print("="*60)
    print("\nGenerated files:")
    print("  - parsing_summary.txt   (overview of parsed content)")
    print("  - extracted_links.csv   (all links found on pages)")
    print("  - Database updated with parsed_data table")
