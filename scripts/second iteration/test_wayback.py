import requests

USER_AGENT = "wayback-test/2.0"
CDX_ENDPOINT = "https://web.archive.org/cdx/search/cdx"


def test_cdx_simple() -> bool:
    print("Testing CDX API...")

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    params = {
        "url": "https://investor.apple.com/",
        "matchType": "prefix",
        "from": "2011",
        "to": "2011",
        "output": "json",
        "fl": "timestamp,original,statuscode,mimetype",
        "filter": ["statuscode:200"],
        "limit": "5",
    }

    try:
        response = session.get(CDX_ENDPOINT, params=params, timeout=(10, 30))
        response.raise_for_status()
        data = response.json()

        print(f"✓ CDX success, rows returned: {max(len(data) - 1, 0)}")
        for row in data[:6]:
            print(f"  {row}")
        return True

    except requests.exceptions.RequestException as exc:
        print(f"✗ CDX request failed: {exc}")
        return False


def test_wayback_download() -> bool:
    print("\nTesting direct Wayback download...")

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    url = "https://web.archive.org/web/20110701id_/http://investor.apple.com/results.cfm"

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        print(f"✓ Download success, bytes: {len(response.text)}")
        print(response.text[:250])
        return True

    except requests.exceptions.RequestException as exc:
        print(f"✗ Download failed: {exc}")
        return False


if __name__ == "__main__":
    ok1 = test_cdx_simple()
    ok2 = test_wayback_download()

    print()
    if ok1 and ok2:
        print("✓ Wayback tests passed")
    else:
        print("✗ One or more Wayback tests failed")