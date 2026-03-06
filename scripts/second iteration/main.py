"""
Master pipeline script - runs discovery, download, parsing, and analysis
"""
import sys
import subprocess


def run_step(script_name, description):
    """Run a Python script and handle errors"""
    print("\n" + "="*80)
    print(f"STEP: {description}")
    print("="*80)
    
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            check=True,
            capture_output=False
        )
        print(f"\n✓ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n✗ {description} failed with error code {e.returncode}")
        return False
    except FileNotFoundError:
        print(f"\n✗ Could not find {script_name}")
        return False


def main():
    print("="*80)
    print("INVESTOR RESEARCH PIPELINE")
    print("="*80)
    print("\nThis will run:")
    print("  1. Discovery & Download from Wayback Machine")
    print("  2. Parse downloaded HTML files")
    print("  3. Analyze and display results")
    print("\n")
    
    # Step 1: Scrape
    if not run_step("wayback_scraper_fixed.py", "Discovery & Download"):
        print("\n⚠ Pipeline stopped due to scraping error")
        return
    
    # Step 2: Parse
    if not run_step("parse_investor_pages.py", "HTML Parsing"):
        print("\n⚠ Pipeline stopped due to parsing error")
        return
    
    # Step 3: Analyze
    if not run_step("analyze_investor_data.py", "Analysis & Reporting"):
        print("\n⚠ Pipeline stopped due to analysis error")
        return
    
    print("\n" + "="*80)
    print("✓ COMPLETE PIPELINE FINISHED SUCCESSFULLY!")
    print("="*80)
    print("\nGenerated files:")
    print("  - apple_investor_pages.db  (SQLite database)")
    print("  - parsing_summary.txt      (summary report)")
    print("  - extracted_links.csv      (all links found)")
    print("  - downloads/AAPL/          (downloaded HTML/PDF files)")
    print("\n")


if __name__ == "__main__":
    main()