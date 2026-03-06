"""
Simple test script to verify Wayback Machine API access
"""
import requests
import time

USER_AGENT = "wayback-test/1.0"
CDX_ENDPOINT = "https://web.archive.org/cdx/search/cdx"

def test_cdx_simple():
    """Test basic CDX API access"""
    print("Testing CDX API with simple query...")
    
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    
    # Very simple test query
    params = {
        "url": "https://investor.apple.com/",
        "matchType": "prefix",
        "from": "2010",
        "to": "2010",
        "output": "json",
        "fl": "timestamp,original,statuscode",
        "filter": ["statuscode:200"],
        "limit": "5",
    }
    
    try:
        print(f"Querying: {CDX_ENDPOINT}")
        print(f"Parameters: {params}")
        
        resp = session.get(CDX_ENDPOINT, params=params, timeout=(10, 30))
        resp.raise_for_status()
        
        data = resp.json()
        
        print(f"\n✓ Success! Found {len(data)-1} results")
        print("\nFirst few results:")
        for row in data[:6]:
            print(f"  {row}")
            
        return True
        
    except requests.exceptions.Timeout:
        print("✗ Request timed out - Wayback might be slow")
        return False
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Request failed: {e}")
        return False


def test_wayback_download():
    """Test downloading a single page"""
    print("\n\nTesting Wayback page download...")
    
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    
    # Known archived page
    url = "https://web.archive.org/web/20100701id_/https://investor.apple.com/"
    
    try:
        print(f"Downloading: {url}")
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        
        content = resp.text
        print(f"\n✓ Downloaded {len(content)} bytes")
        print(f"First 200 chars:\n{content[:200]}")
        
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Download failed: {e}")
        return False


if __name__ == "__main__":
    print("="*60)
    print("Wayback Machine API Test")
    print("="*60)
    
    test1 = test_cdx_simple()
    time.sleep(2)
    test2 = test_wayback_download()
    
    print("\n" + "="*60)
    if test1 and test2:
        print("✓ All tests passed!")
    else:
        print("✗ Some tests failed - check network/Wayback availability")
    print("="*60)
