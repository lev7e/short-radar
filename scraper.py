import requests
import time
import json
import os
import re
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from collections import defaultdict

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "en-US,en;q=0.9",
}

SOURCE_STATUS = {}

# -------------------------------------------------
# UTILS
# -------------------------------------------------

def save_json(name, payload):
    path = os.path.join(DATA_DIR, name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"Saved {name} ({len(payload) if isinstance(payload, list) else '1'} records)")
    return True


def safe_get(url, timeout=30):
    try:
        r = requests.get(url, headers=BROWSER_HEADERS, timeout=timeout)
        return r
    except Exception as e:
        print(f"Request failed: {url} → {e}")
        return None


def now_iso():
    return datetime.now(timezone.utc).isoformat()


# -------------------------------------------------
# CHARTEXCHANGE (OPTIONAL)
# -------------------------------------------------

def fetch_chartexchange():
    url = "https://chartexchange.com/short-interest/"
    r = safe_get(url)

    if not r or r.status_code != 200:
        print("Chartexchange blocked or unavailable")
        SOURCE_STATUS["chartexchange"] = "blocked_or_error"
        return {}

    soup = BeautifulSoup(r.text, "html.parser")
    rows = soup.find_all("tr")

    result = {}

    for row in rows:
        cols = [c.get_text(strip=True) for c in row.find_all("td")]
        if len(cols) < 5:
            continue

        ticker = cols[0].upper()
        try:
            c2b = float(cols[1].replace("%", "").replace(",", ""))
        except:
            continue

        result[ticker] = {
            "ticker": ticker,
            "c2b": c2b,
        }

    SOURCE_STATUS["chartexchange"] = f"ok:{len(result)}"
    return result


# -------------------------------------------------
# REGSHO
# -------------------------------------------------

def fetch_regsho():
    url = "https://www.nasdaqtrader.com/trader.aspx?id=regshothreshold"
    r = safe_get(url)

    if not r or r.status_code != 200:
        SOURCE_STATUS["regsho"] = "error"
        return set()

    soup = BeautifulSoup(r.text, "html.parser")
    rows = soup.find_all("tr")

    tickers = set()
    for row in rows:
        cols = row.find_all("td")
        if cols:
            t = cols[0].get_text(strip=True).upper()
            if t:
                tickers.add(t)

    SOURCE_STATUS["regsho"] = f"ok:{len(tickers)}"
    return tickers


# -------------------------------------------------
# SPLITS (ROBUST)
# -------------------------------------------------

def fetch_splits(url):
    r = safe_get(url)
    if not r or r.status_code != 200:
        return {}

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    if not table:
        return {}

    rows = table.find_all("tr")
    headers = [h.get_text(strip=True) for h in rows[0].find_all("th")]

    data = {}

    for row in rows[1:]:
        cols = [c.get_text(strip=True) for c in row.find_all("td")]
        if len(cols) != len(headers):
            continue

        row_dict = dict(zip(headers, cols))

        ticker = (
            row_dict.get("Ticker")
            or row_dict.get("Symbol")
            or row_dict.get("ticker")
            or row_dict.get("symbol")
        )

        if not ticker and "Company" in row_dict:
            m = re.search(r"\((.*?)\)", row_dict["Company"])
            if m:
                ticker = m.group(1)

        if ticker:
            data[ticker.upper()] = True

    return data


# -------------------------------------------------
# FINRA SHORT INTEREST (NO HEAD)
# -------------------------------------------------

def fetch_finra_si():
    base = "https://cdn.finra.org/equity/otcmarket/biweekly/shrtint"
    result = {}

    for i in range(1, 20):
        url = f"{base}{i}.csv"
        r = safe_get(url)
        if not r or r.status_code != 200:
            continue

        lines = r.text.splitlines()
        if len(lines) < 2:
            continue

        headers = lines[0].split(",")

        for line in lines[1:]:
            cols = line.split(",")
            if len(cols) != len(headers):
                continue

            row = dict(zip(headers, cols))
            ticker = row.get("Symbol", "").upper()
            si = row.get("Current Short Interest")

            if ticker and si:
                try:
                    result[ticker] = int(si)
                except:
                    continue

    SOURCE_STATUS["finra_si"] = f"ok:{len(result)}"
    return result


# -------------------------------------------------
# MAIN
# -------------------------------------------------

def main():
    start = time.time()

    ce = fetch_chartexchange()          # optional
    regsho = fetch_regsho()
    splits = fetch_splits("https://stockanalysis.com/actions/reverse-splits/")
    finra = fetch_finra_si()

    # Build ticker universe (CE optional)
    all_tickers = (
        set(regsho)
        | set(splits.keys())
        | set(finra.keys())
        | set(ce.keys())
    )

    summary = []

    for t in all_tickers:
        summary.append({
            "ticker": t,
            "regsho": t in regsho,
            "reverse_split": t in splits,
            "short_interest": finra.get(t),
            "c2b": ce.get(t, {}).get("c2b"),
        })

    save_json("summary.json", summary)
    save_json("meta.json", {
        "updated_at": now_iso(),
        "elapsed_sec": round(time.time() - start, 2),
        "scraper_ok": True,
        "counts": {
            "chartexchange": len(ce),
            "regsho": len(regsho),
            "splits_reverse": len(splits),
            "finra_si": len(finra),
            "summary": len(summary),
        },
        "source_status": SOURCE_STATUS,
    })


if __name__ == "__main__":
    main()
