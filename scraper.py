#!/usr/bin/env python3
"""
Short Tracker - Multi-Source Scraper
Runs via GitHub Actions, outputs data.json
"""

import requests
import json
import re
import time
import sys
from datetime import datetime, date, timedelta
from bs4 import BeautifulSoup

# ── Session ────────────────────────────────────────────────────────────────────
S = requests.Session()
S.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
})

def safe_get(url, **kwargs):
    try:
        r = S.get(url, timeout=20, **kwargs)
        r.raise_for_status()
        return r
    except Exception as e:
        print(f"  WARN GET {url}: {e}", file=sys.stderr)
        return None

def next_data(html):
    """Extract __NEXT_DATA__ JSON from a Next.js page."""
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            pass
    return None

# ── 1. NASDAQ RegSHO Threshold List ──────────────────────────────────────────
def scrape_regsho():
    print("→ RegSHO (NASDAQ)...")
    tickers = set()
    date_str = None

    # Exchanges: nasdaq, nyse, arca, bats
    exchange_files = {
        "NASDAQ": "nasdaqth",
        "NYSE":   "nyseth",
        "ARCA":   "arcath",
    }
    for delta in range(7):
        d = date.today() - timedelta(days=delta)
        if d.weekday() >= 5:
            continue
        date_str = d.strftime("%b %-d, %Y")
        ds = d.strftime("%Y%m%d")
        for exch, prefix in exchange_files.items():
            url = f"https://www.nasdaqtrader.com/dynamic/symdir/regsho/{prefix}{ds}.txt"
            r = safe_get(url)
            if r and r.status_code == 200:
                for line in r.text.strip().splitlines()[1:]:
                    parts = line.split("|")
                    if parts and parts[0].strip():
                        tickers.add(parts[0].strip())
        if tickers:
            break

    print(f"   {len(tickers)} tickers on RegSHO ({date_str})")
    return list(tickers), date_str

# ── 2. ChartExchange Screener ─────────────────────────────────────────────────
def scrape_chartexchange():
    print("→ ChartExchange Screener...")
    results = []
    page = 1

    base = (
        "https://chartexchange.com/screener/?page={page}"
        "&equity_type=ad,cs"
        "&exchange=BATS,NASDAQ,NYSE,NYSEAMERICAN"
        "&currency=USD"
        "&shares_float=%3C5000000"
        "&reg_price=%3C6,%3E0.8"
        "&borrow_fee_avail_ib=%3C100000"
        "&per_page=100"
        "&view_cols=display,borrow_fee_rate_ib,borrow_fee_avail_ib,shares_float,"
        "market_cap,reg_price,reg_change_pct,reg_volume,10_day_avg_vol,"
        "shortvol_all_short,shortint_db_pct,shortint_pct,"
        "shortint_position_change_pct,shortvol_all_short_pct,"
        "shortvol_all_short_pct_30d,pre_price,pre_change_pct"
        "&sort=borrow_fee_rate_ib,desc"
        "&section_saved=hide&section_select=hide&section_filter=hide&section_view=hide"
    )

    while True:
        url = base.format(page=page)
        r = safe_get(url)
        if not r:
            break

        soup = BeautifulSoup(r.text, "lxml")

        # Try JSON API first (check for JSON content-type)
        try:
            data = r.json()
            rows = data.get("data") or data.get("results") or []
            if rows:
                for row in rows:
                    results.append({
                        "ticker":        row.get("display") or row.get("ticker", ""),
                        "borrow_rate":   str(row.get("borrow_fee_rate_ib", "-")),
                        "avail_shares":  str(row.get("borrow_fee_avail_ib", "-")),
                        "float":         str(row.get("shares_float", "-")),
                        "market_cap":    str(row.get("market_cap", "-")),
                        "price":         str(row.get("reg_price", "-")),
                        "change_pct":    str(row.get("reg_change_pct", "-")),
                        "volume":        str(row.get("reg_volume", "-")),
                        "short_int_pct": str(row.get("shortint_pct", "-")),
                        "si_change_pct": str(row.get("shortint_position_change_pct", "-")),
                    })
                if len(rows) < 100:
                    break
                page += 1
                time.sleep(0.8)
                continue
        except Exception:
            pass

        # Fall back: parse HTML table
        table = soup.find("table")
        if not table:
            print("  ChartExchange: no table found on page", page, file=sys.stderr)
            break

        headers_row = table.find("tr")
        col_names = [th.get_text(strip=True).lower().replace(" ", "_")
                     for th in (headers_row.find_all("th") if headers_row else [])]

        data_rows = table.find_all("tr")[1:]
        if not data_rows:
            break

        for row in data_rows:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if not cells or not cells[0]:
                continue

            def col(i, fallback="-"):
                return cells[i] if len(cells) > i and cells[i] else fallback

            results.append({
                "ticker":        col(0),
                "borrow_rate":   col(1),
                "avail_shares":  col(2),
                "float":         col(3),
                "market_cap":    col(4),
                "price":         col(5),
                "change_pct":    col(6),
                "volume":        col(7),
                "short_int_pct": col(11) if len(cells) > 11 else "-",
                "si_change_pct": col(12) if len(cells) > 12 else "-",
            })

        if len(data_rows) < 100:
            break
        page += 1
        time.sleep(0.8)

    print(f"   {len(results)} screener rows across {page} page(s)")
    return results

# ── 3. StockAnalysis — Recent Reverse Splits ──────────────────────────────────
def scrape_splits_recent():
    print("→ StockAnalysis Recent Splits...")
    r = safe_get("https://stockanalysis.com/actions/splits/")
    if not r:
        return []

    # Try __NEXT_DATA__
    nd = next_data(r.text)
    if nd:
        try:
            rows = nd["props"]["pageProps"]["data"]
            results = []
            for row in rows:
                ratio = str(row.get("splitRatio") or row.get("ratio") or "")
                if ":" in ratio:
                    parts = ratio.split(":")
                    try:
                        if float(parts[0]) < float(parts[1]):
                            results.append({
                                "ticker":  row.get("symbol") or row.get("ticker", ""),
                                "company": row.get("name") or row.get("company", ""),
                                "ratio":   ratio,
                                "date":    str(row.get("date") or row.get("exDate") or ""),
                            })
                    except Exception:
                        pass
            print(f"   {len(results)} recent reverse splits")
            return results
        except Exception:
            pass

    # Fall back to HTML table
    soup = BeautifulSoup(r.text, "lxml")
    results = []
    table = soup.find("table")
    if not table:
        print("  StockAnalysis splits: no table", file=sys.stderr)
        return []
    for row in table.find_all("tr")[1:]:
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cells) < 4:
            continue
        ratio = cells[2]
        # Reverse split: "1-for-10" or "1:10" → new shares < old shares
        if re.search(r"1[:\-]for[:\-]\s*\d+", ratio, re.I) or \
           re.search(r"1:\d+", ratio):
            results.append({"ticker": cells[0], "company": cells[1],
                            "ratio": ratio, "date": cells[3]})
    print(f"   {len(results)} recent reverse splits")
    return results

# ── 4. TipRanks — Upcoming Splits ─────────────────────────────────────────────
def scrape_splits_upcoming():
    print("→ TipRanks Upcoming Splits (Playwright)...")
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            ctx = browser.new_context(user_agent=S.headers["User-Agent"])
            page = ctx.new_page()
            page.goto("https://www.tipranks.com/calendars/stock-splits/upcoming",
                      wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(4000)
            html = page.content()
            browser.close()

        soup = BeautifulSoup(html, "lxml")
        results = []
        table = soup.find("table")
        if table:
            for row in table.find_all("tr")[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cells) >= 3:
                    results.append({
                        "ticker":  cells[0],
                        "company": cells[1] if len(cells) > 1 else "",
                        "ratio":   cells[2] if len(cells) > 2 else "",
                        "date":    cells[3] if len(cells) > 3 else "",
                    })
        print(f"   {len(results)} upcoming splits")
        return results
    except ImportError:
        print("  Playwright not installed, skipping TipRanks", file=sys.stderr)
        return []
    except Exception as e:
        print(f"  TipRanks error: {e}", file=sys.stderr)
        return []

# ── 5. Ticker Changes — NASDAQ ────────────────────────────────────────────────
def scrape_changes_nasdaq():
    print("→ NASDAQ Ticker Changes...")
    results = []
    # NASDAQ serves this as an API too
    api_url = "https://api.nasdaq.com/api/quote/list-type/symbolchangehistory?offset=0&limit=100"
    r = safe_get(api_url, headers={**S.headers, "Accept": "application/json",
                                    "Origin": "https://www.nasdaq.com"})
    if r:
        try:
            data = r.json()
            rows = (data.get("data") or {}).get("data") or \
                   data.get("rows") or data.get("results") or []
            for row in rows:
                results.append({
                    "new_ticker": str(row.get("newSymbol") or row.get("symbol") or ""),
                    "old_ticker": str(row.get("oldSymbol") or row.get("previousSymbol") or ""),
                    "date":       str(row.get("dateOfChange") or row.get("date") or ""),
                    "source":     "NASDAQ",
                })
            if results:
                print(f"   {len(results)} NASDAQ changes (API)")
                return results
        except Exception:
            pass

    # Fall back: HTML
    r = safe_get("https://www.nasdaq.com/market-activity/stocks/symbol-change-history?page=1&rows_per_page=100")
    if not r:
        return results
    soup = BeautifulSoup(r.text, "lxml")
    table = soup.find("table")
    if table:
        for row in table.find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) >= 3:
                results.append({"new_ticker": cells[0], "old_ticker": cells[1],
                                 "date": cells[2], "source": "NASDAQ"})
    print(f"   {len(results)} NASDAQ ticker changes")
    return results

# ── 6. Ticker Changes — StockAnalysis ────────────────────────────────────────
def scrape_changes_stockanalysis():
    print("→ StockAnalysis Ticker Changes...")
    r = safe_get("https://stockanalysis.com/actions/changes/")
    if not r:
        return []

    nd = next_data(r.text)
    if nd:
        try:
            rows = nd["props"]["pageProps"]["data"]
            results = []
            for row in rows:
                results.append({
                    "new_ticker": str(row.get("newSymbol") or row.get("symbol") or ""),
                    "old_ticker": str(row.get("oldSymbol") or row.get("previousSymbol") or ""),
                    "date":       str(row.get("date") or ""),
                    "source":     "StockAnalysis",
                })
            print(f"   {len(results)} StockAnalysis changes")
            return results
        except Exception:
            pass

    soup = BeautifulSoup(r.text, "lxml")
    table = soup.find("table")
    if not table:
        return []
    results = []
    for row in table.find_all("tr")[1:]:
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cells) >= 3:
            results.append({"new_ticker": cells[0], "old_ticker": cells[1],
                             "date": cells[2], "source": "StockAnalysis"})
    print(f"   {len(results)} StockAnalysis changes")
    return results

# ── 7. DilutionTracker S1 Filings ─────────────────────────────────────────────
def scrape_s1():
    print("→ DilutionTracker S1 (Playwright)...")
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            ctx = browser.new_context(user_agent=S.headers["User-Agent"])
            page = ctx.new_page()
            # Intercept XHR/fetch responses to grab API data
            api_data = []

            def handle_response(response):
                if "s1" in response.url.lower() and "api" in response.url.lower():
                    try:
                        body = response.json()
                        api_data.append(body)
                    except Exception:
                        pass

            page.on("response", handle_response)
            page.goto("https://dilutiontracker.com/app/s1",
                      wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(5000)
            html = page.content()
            browser.close()

        # Try API data first
        if api_data:
            for d in api_data:
                rows = d.get("data") or d.get("filings") or d if isinstance(d, list) else []
                if rows:
                    results = []
                    for row in rows[:200]:
                        if isinstance(row, dict):
                            results.append({
                                "ticker":  str(row.get("ticker") or row.get("symbol") or ""),
                                "company": str(row.get("company") or row.get("name") or ""),
                                "date":    str(row.get("date") or row.get("filedDate") or ""),
                                "type":    str(row.get("type") or "S-1"),
                            })
                    if results:
                        print(f"   {len(results)} S1 filings (API)")
                        return results

        # Fall back: HTML
        soup = BeautifulSoup(html, "lxml")
        results = []
        table = soup.find("table")
        if table:
            for row in table.find_all("tr")[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cells) >= 2:
                    results.append({"ticker": cells[0], "company": cells[1] if len(cells) > 2 else "",
                                     "date": cells[2] if len(cells) > 2 else cells[1], "type": "S-1"})
        print(f"   {len(results)} S1 filings")
        return results
    except ImportError:
        print("  Playwright not installed, skipping DilutionTracker", file=sys.stderr)
        return []
    except Exception as e:
        print(f"  DilutionTracker error: {e}", file=sys.stderr)
        return []

# ── 8. Finviz Insider Buys ────────────────────────────────────────────────────
def scrape_insiders():
    print("→ Finviz Insider Buys...")
    r = safe_get("https://finviz.com/insidertrading.ashx?tc=1",
                 headers={**S.headers, "Referer": "https://finviz.com/"})
    if not r:
        # Try alternate URL
        r = safe_get("https://finviz.com/insidertrading?tc=1",
                     headers={**S.headers, "Referer": "https://finviz.com/"})
    if not r:
        return []

    soup = BeautifulSoup(r.text, "lxml")
    # Finviz uses a table with class containing 'body-table' or similar
    table = (soup.find("table", {"class": re.compile("body-table", re.I)}) or
             soup.find("table", id=re.compile("insider", re.I)) or
             soup.find("table"))

    if not table:
        print("  Finviz: no insider table found", file=sys.stderr)
        return []

    results = []
    rows = table.find_all("tr")
    for row in rows:
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
        # Finviz insider row: #, Ticker, Owner, Relationship, Date, Transaction, Cost, #Shares, Value, #Shares Total, SEC Form 4
        if len(cells) < 9:
            continue
        ticker = cells[1] if len(cells) > 1 else cells[0]
        if not ticker or ticker in ("#", "Ticker"):
            continue
        results.append({
            "ticker":       ticker,
            "owner":        cells[2] if len(cells) > 2 else "",
            "relationship": cells[3] if len(cells) > 3 else "",
            "date":         cells[4] if len(cells) > 4 else "",
            "transaction":  cells[5] if len(cells) > 5 else "",
            "cost":         cells[6] if len(cells) > 6 else "",
            "shares":       cells[7] if len(cells) > 7 else "",
            "value":        cells[8] if len(cells) > 8 else "",
        })

    print(f"   {len(results)} insider buys")
    return results

# ── 9. AskEdgar Float (per ticker batch) ──────────────────────────────────────
def scrape_float_askedgar(tickers):
    """Fetch float from app.askedgar.io for a list of tickers."""
    print(f"→ AskEdgar Float for {len(tickers)} tickers...")
    floats = {}
    for i, ticker in enumerate(tickers):
        try:
            r = safe_get(f"https://app.askedgar.io/{ticker}")
            if not r:
                continue
            ct = r.headers.get("content-type", "")
            if "json" in ct:
                data = r.json()
                f = (data.get("float") or data.get("sharesFloat") or
                     data.get("shares_float") or data.get("floatShares"))
                if f:
                    floats[ticker] = str(f)
            else:
                # Parse HTML/text
                soup = BeautifulSoup(r.text, "lxml")
                text = soup.get_text(" ")
                m = re.search(r"float[:\s]+([0-9][0-9,.]*\s*[MBK]?)", text, re.I)
                if m:
                    floats[ticker] = m.group(1).strip()
        except Exception as e:
            pass

        if (i + 1) % 10 == 0:
            print(f"   {i+1}/{len(tickers)} done...")
            time.sleep(0.5)

    print(f"   Got float for {len(floats)} tickers")
    return floats

# ── Merge & Enrich ─────────────────────────────────────────────────────────────
def build_dataset():
    result = {
        "updated":         datetime.now().strftime("%b %-d, %Y %H:%M UTC"),
        "regsho_tickers":  [],
        "screener":        [],
        "splits_recent":   [],
        "splits_upcoming": [],
        "ticker_changes":  [],
        "s1_filings":      [],
        "insiders":        [],
    }

    # RegSHO
    regsho_list, regsho_date = scrape_regsho()
    result["regsho_tickers"] = regsho_list
    result["regsho_date"] = regsho_date or ""

    # Screener (main list)
    screener = scrape_chartexchange()
    # Mark RegSHO status
    regsho_set = set(regsho_list)
    for row in screener:
        row["reg_sho"] = row["ticker"] in regsho_set

    # Float from AskEdgar for screener tickers not already having float from ChartExchange
    tickers_need_float = [r["ticker"] for r in screener
                          if not r.get("float") or r["float"] in ("-", "", "0")]
    if tickers_need_float:
        float_data = scrape_float_askedgar(tickers_need_float[:50])  # limit to 50
        for row in screener:
            if row["ticker"] in float_data:
                row["float"] = float_data[row["ticker"]]

    result["screener"] = screener

    # S1 filings
    s1 = scrape_s1()
    result["s1_filings"] = s1

    # Insider buys — build ticker→buyer map
    insiders = scrape_insiders()
    result["insiders"] = insiders
    insider_map = {}
    for ins in insiders:
        t = ins["ticker"]
        if t not in insider_map:
            insider_map[t] = ins["owner"]

    # Enrich screener with S1 date and buyer
    s1_map = {row["ticker"]: row["date"] for row in s1}
    for row in screener:
        row["s1_date"] = s1_map.get(row["ticker"], "-")
        row["buyer"]   = insider_map.get(row["ticker"], "-")

    # Splits
    result["splits_recent"]   = scrape_splits_recent()
    result["splits_upcoming"] = scrape_splits_upcoming()

    # Ticker changes — merge NASDAQ + StockAnalysis, deduplicate
    changes_n = scrape_changes_nasdaq()
    changes_s = scrape_changes_stockanalysis()
    seen = set()
    merged = []
    for row in changes_n + changes_s:
        key = (row["new_ticker"], row["old_ticker"])
        if key not in seen:
            seen.add(key)
            merged.append(row)
    result["ticker_changes"] = merged

    return result

# ── Main ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("Short Tracker Scraper")
    print("=" * 60)

    data = build_dataset()

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("\n✅ data.json written")
    print(f"   Screener rows:       {len(data['screener'])}")
    print(f"   RegSHO tickers:      {len(data['regsho_tickers'])}")
    print(f"   Recent splits:       {len(data['splits_recent'])}")
    print(f"   Upcoming splits:     {len(data['splits_upcoming'])}")
    print(f"   Ticker changes:      {len(data['ticker_changes'])}")
    print(f"   S1 filings:          {len(data['s1_filings'])}")
    print(f"   Insider buys:        {len(data['insiders'])}")
