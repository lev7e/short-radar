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

S = requests.Session()
S.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
})

def safe_get(url, **kwargs):
    try:
        r = S.get(url, timeout=25, **kwargs)
        r.raise_for_status()
        return r
    except Exception as e:
        print(f"  WARN {url}: {e}", file=sys.stderr)
        return None

TICKER_RE = re.compile(r'^[A-Z]{1,6}$')
def is_ticker(s):
    return bool(s and TICKER_RE.match(str(s).strip()))

# ── 1. RegSHO ─────────────────────────────────────────────────────────────────
def scrape_regsho():
    print("→ RegSHO (NASDAQ Trader TXT)...")
    tickers = set()
    date_str = None

    prefixes = ["nasdaqth", "nyseth", "arcath"]
    for delta in range(7):
        d = date.today() - timedelta(days=delta)
        if d.weekday() >= 5:
            continue
        ds = d.strftime("%Y%m%d")
        found_any = False
        for prefix in prefixes:
            url = f"https://www.nasdaqtrader.com/dynamic/symdir/regsho/{prefix}{ds}.txt"
            r = safe_get(url)
            if not r or r.status_code != 200:
                continue
            # File format: Symbol|SecurityName|Market|RegSHOThresholdFlag|Rule4320Flag
            for line in r.text.strip().splitlines():
                parts = line.split("|")
                if len(parts) >= 1:
                    t = parts[0].strip()
                    if is_ticker(t):
                        tickers.add(t)
                        found_any = True
        if found_any:
            date_str = d.strftime("%b %-d, %Y")
            break

    print(f"   {len(tickers)} valid tickers ({date_str})")
    return sorted(tickers), date_str

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

    # Expected column order from view_cols (ChartExchange prepends a "#" counter col)
    # So actual HTML cols: [#, display, borrow_fee_rate_ib, borrow_fee_avail_ib, shares_float,
    #                        market_cap, reg_price, reg_change_pct, reg_volume, ...]
    COL_MAP = {
        "display":                       "ticker",
        "symbol":                        "ticker",
        "borrowfee%ibkr":                "borrow_rate",
        "borrow_fee_rate_ib":            "borrow_rate",
        "borrowsharesavailableibkr":     "avail_shares",
        "borrow_fee_avail_ib":           "avail_shares",
        "sharesavailableibkr":           "avail_shares",
        "sharesfloat":                   "float",
        "shares_float":                  "float",
        "float":                         "float",
        "marketcap":                     "market_cap",
        "market_cap":                    "market_cap",
        "price":                         "price",
        "reg_price":                     "price",
        "change%":                       "change_pct",
        "chg%":                          "change_pct",
        "reg_change_pct":                "change_pct",
        "volume":                        "volume",
        "reg_volume":                    "volume",
        "shortint%":                     "short_int_pct",
        "shortint_pct":                  "short_int_pct",
        "shortintpositionchange%":       "si_change_pct",
        "shortint_position_change_pct":  "si_change_pct",
    }

    while True:
        url = base.format(page=page)
        r = safe_get(url)
        if not r:
            break

        soup = BeautifulSoup(r.text, "lxml")
        table = soup.find("table")
        if not table:
            print(f"  No table on page {page}", file=sys.stderr)
            break

        # Parse header row to build column index → field mapping
        header_row = table.find("tr")
        raw_headers = [th.get_text(strip=True) for th in header_row.find_all(["th","td"])]

        col_to_field = {}
        for i, h in enumerate(raw_headers):
            key = h.lower().replace(" ", "").replace("_", "").replace("%", "%").strip()
            # try exact then stripped
            field = COL_MAP.get(h.lower().strip()) or COL_MAP.get(key)
            if field:
                col_to_field[i] = field

        data_rows = table.find_all("tr")[1:]
        if not data_rows:
            break

        page_count = 0
        for row in data_rows:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if not cells:
                continue

            rec = {f: "-" for f in ["ticker","borrow_rate","avail_shares","float",
                                     "market_cap","price","change_pct","volume",
                                     "short_int_pct","si_change_pct"]}

            for i, field in col_to_field.items():
                if i < len(cells) and cells[i]:
                    rec[field] = cells[i]

            # Fallback: if no col_to_field mapping worked, guess by position
            if rec["ticker"] == "-":
                # Find first cell that looks like a ticker
                for i, c in enumerate(cells):
                    if is_ticker(c):
                        rec["ticker"] = c
                        break

            if not rec["ticker"] or rec["ticker"] == "-" or not is_ticker(rec["ticker"]):
                continue

            results.append(rec)
            page_count += 1

        print(f"   Page {page}: {page_count} rows (total {len(results)})")
        if page_count < 95:
            break
        page += 1
        time.sleep(1.0)

    print(f"   Total screener: {len(results)} rows")
    return results

# ── 3. Float from Finviz ──────────────────────────────────────────────────────
def scrape_float_askedgar(tickers):
    """
    Fetch float from app.askedgar.io/[TICKER].
    Tries JSON first, then __NEXT_DATA__, then HTML text scan.
    """
    print(f"→ AskEdgar Float for {len(tickers)} tickers...")
    floats = {}
    MAX = 100  # cap to avoid long runtimes

    # Regex patterns that match float values in text:
    # "Float: 1.23M", "Shares Float 4,567,890", "float":1234567", etc.
    FLOAT_PATTERNS = [
        re.compile(r'"(?:sharesFloat|shares_float|float(?:Shares)?|floatShares)"\s*:\s*"?([0-9][0-9,\.]+)"?', re.I),
        re.compile(r'(?:shares?\s+)?float\s*[:\-=]\s*([0-9][0-9,\.]+\s*[MBK]?)', re.I),
        re.compile(r'float\s*</[^>]+>\s*<[^>]+>\s*([0-9][0-9,\.]+\s*[MBK]?)', re.I),
    ]

    def parse_float_val(s):
        """Normalise e.g. '4.5M' → '4.5M', '4500000' → '4500000'"""
        s = s.strip().replace(",", "")
        return s if s else None

    for i, ticker in enumerate(tickers[:MAX]):
        url = f"https://app.askedgar.io/{ticker}"
        r = safe_get(url, headers={**S.headers, "Referer": "https://app.askedgar.io/"})
        if not r:
            continue

        val = None
        ct = r.headers.get("content-type", "")

        # ── JSON response ────────────────────────────────────────────────────
        if "json" in ct:
            try:
                d = r.json()
                # Common key names
                for k in ("sharesFloat", "shares_float", "float", "floatShares",
                          "shareFloat", "Float", "sharesOutstandingFloat"):
                    if k in d and d[k]:
                        val = parse_float_val(str(d[k]))
                        break
                # Nested under data/stats/fundamentals
                if not val:
                    for section in ("data", "stats", "fundamentals", "summary"):
                        if isinstance(d.get(section), dict):
                            for k in ("sharesFloat", "shares_float", "float", "floatShares"):
                                if d[section].get(k):
                                    val = parse_float_val(str(d[section][k]))
                                    break
                        if val:
                            break
            except Exception:
                pass

        # ── HTML / Next.js ───────────────────────────────────────────────────
        if not val:
            text = r.text

            # __NEXT_DATA__
            m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', text, re.S)
            if m:
                try:
                    nd = json.loads(m.group(1))
                    nd_str = json.dumps(nd)
                    for pat in FLOAT_PATTERNS:
                        fm = pat.search(nd_str)
                        if fm:
                            val = parse_float_val(fm.group(1))
                            break
                except Exception:
                    pass

            # Raw text scan
            if not val:
                for pat in FLOAT_PATTERNS:
                    fm = pat.search(text)
                    if fm:
                        candidate = parse_float_val(fm.group(1))
                        # Sanity check: float should be a number or end with M/B/K
                        if candidate and re.match(r'^[\d,\.]+[MBK]?$', candidate):
                            val = candidate
                            break

        if val:
            floats[ticker] = val

        if (i + 1) % 15 == 0:
            print(f"   {i+1}/{min(len(tickers), MAX)} ({len(floats)} found)")
            time.sleep(0.5)
        time.sleep(0.35)

    print(f"   AskEdgar: float for {len(floats)}/{min(len(tickers), MAX)} tickers")
    return floats

# ── 4. Reverse Splits — StockAnalysis ────────────────────────────────────────
def scrape_splits_recent():
    print("→ StockAnalysis Recent Splits...")
    r = safe_get("https://stockanalysis.com/actions/splits/")
    if not r:
        return []

    soup = BeautifulSoup(r.text, "lxml")
    results = []
    table = soup.find("table")
    if not table:
        # Try __NEXT_DATA__
        m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, re.S)
        if m:
            try:
                nd = json.loads(m.group(1))
                rows = nd["props"]["pageProps"].get("data") or []
                for row in rows:
                    ratio = str(row.get("splitRatio") or row.get("ratio") or "")
                    # Reverse split: new < old, e.g. "1:10" or "1-for-10"
                    if re.search(r'1\s*[:/]\s*\d+', ratio) or \
                       re.search(r'1.for.\d+', ratio, re.I):
                        results.append({
                            "ticker":  str(row.get("symbol") or ""),
                            "company": str(row.get("name") or ""),
                            "ratio":   ratio,
                            "date":    str(row.get("date") or ""),
                        })
                print(f"   {len(results)} recent reverse splits (NEXT_DATA)")
                return results
            except Exception as e:
                print(f"  NEXT_DATA error: {e}", file=sys.stderr)
        print("  No table found", file=sys.stderr)
        return []

    # Parse HTML table — headers: Date | Ticker | Company | Ratio | Type (varies)
    headers = [th.get_text(strip=True).lower() for th in table.find("tr").find_all(["th","td"])]

    def col_idx(names):
        for n in names:
            for i, h in enumerate(headers):
                if n in h:
                    return i
        return None

    i_ticker  = col_idx(["symbol","ticker"])
    i_company = col_idx(["company","name"])
    i_ratio   = col_idx(["ratio","split"])
    i_date    = col_idx(["date"])
    i_type    = col_idx(["type"])

    for row in table.find_all("tr")[1:]:
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
        if not cells:
            continue

        def gc(idx):
            return cells[idx] if idx is not None and idx < len(cells) else ""

        ratio = gc(i_ratio)
        rtype = gc(i_type).lower() if i_type is not None else ""

        # Keep only reverse splits
        is_reverse = (
            re.search(r'1\s*[:/]\s*\d+', ratio) or
            re.search(r'1.for.\d+', ratio, re.I) or
            "reverse" in rtype
        )
        if not is_reverse:
            continue

        results.append({
            "ticker":  gc(i_ticker),
            "company": gc(i_company),
            "ratio":   ratio or "reverse",
            "date":    gc(i_date),
        })

    print(f"   {len(results)} recent reverse splits")
    return results

# ── 5. Upcoming Splits — TipRanks ────────────────────────────────────────────
def scrape_splits_upcoming():
    print("→ TipRanks Upcoming Splits...")
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
            ctx = browser.new_context(user_agent=S.headers["User-Agent"])
            page = ctx.new_page()
            page.goto("https://www.tipranks.com/calendars/stock-splits/upcoming",
                      wait_until="domcontentloaded", timeout=35000)
            page.wait_for_timeout(5000)
            html = page.content()
            browser.close()

        soup = BeautifulSoup(html, "lxml")
        results = []
        table = soup.find("table")
        if not table:
            print("  TipRanks: no table found", file=sys.stderr)
            return []

        # Detect header columns
        header_row = table.find("tr")
        headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th","td"])]
        print(f"   TipRanks headers: {headers}")

        def col_idx(names):
            for n in names:
                for i, h in enumerate(headers):
                    if n in h:
                        return i
            return None

        i_ticker  = col_idx(["ticker","symbol"])
        i_company = col_idx(["company","name"])
        i_ratio   = col_idx(["ratio","split"])
        i_date    = col_idx(["date","ex-date","announcement"])
        i_type    = col_idx(["type"])

        for row in table.find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if not cells:
                continue

            def gc(idx):
                return cells[idx] if idx is not None and idx < len(cells) else ""

            rtype = gc(i_type).lower() if i_type is not None else ""
            ratio = gc(i_ratio)

            # Only reverse splits
            is_reverse = "reverse" in rtype or re.search(r'1\s*[:/]\s*\d+', ratio)
            if i_type is not None and not is_reverse:
                continue  # skip forward splits if we can detect type

            results.append({
                "ticker":  gc(i_ticker),
                "company": gc(i_company),
                "ratio":   ratio or rtype or "reverse",
                "date":    gc(i_date),
            })

        print(f"   {len(results)} upcoming splits")
        return results

    except ImportError:
        print("  Playwright not installed", file=sys.stderr)
        return []
    except Exception as e:
        print(f"  TipRanks error: {e}", file=sys.stderr)
        return []

# ── 6. Ticker Changes ─────────────────────────────────────────────────────────
def scrape_changes_nasdaq():
    print("→ NASDAQ Ticker Changes...")
    # Try JSON API first
    r = safe_get(
        "https://api.nasdaq.com/api/quote/list-type/symbolchangehistory?offset=0&limit=100",
        headers={**S.headers, "Accept": "application/json", "Origin": "https://www.nasdaq.com"}
    )
    if r:
        try:
            d = r.json()
            rows = ((d.get("data") or {}).get("data") or
                    d.get("rows") or d.get("results") or [])
            if rows:
                out = []
                for row in rows:
                    new_t = str(row.get("newSymbol") or row.get("symbol") or "")
                    old_t = str(row.get("oldSymbol") or row.get("previousSymbol") or "")
                    if new_t:
                        out.append({"new_ticker": new_t, "old_ticker": old_t,
                                    "date": str(row.get("dateOfChange") or row.get("date") or ""),
                                    "source": "NASDAQ"})
                if out:
                    print(f"   {len(out)} NASDAQ changes (API)")
                    return out
        except Exception:
            pass

    # HTML fallback
    r = safe_get("https://www.nasdaq.com/market-activity/stocks/symbol-change-history?page=1&rows_per_page=100")
    if not r:
        return []
    soup = BeautifulSoup(r.text, "lxml")
    table = soup.find("table")
    results = []
    if table:
        for row in table.find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) >= 2:
                results.append({"new_ticker": cells[0], "old_ticker": cells[1],
                                 "date": cells[2] if len(cells) > 2 else "",
                                 "source": "NASDAQ"})
    print(f"   {len(results)} NASDAQ changes")
    return results

def scrape_changes_stockanalysis():
    print("→ StockAnalysis Ticker Changes...")
    r = safe_get("https://stockanalysis.com/actions/changes/")
    if not r:
        return []

    # Try __NEXT_DATA__
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, re.S)
    if m:
        try:
            nd = json.loads(m.group(1))
            rows = nd["props"]["pageProps"].get("data") or []
            out = []
            for row in rows:
                out.append({
                    "new_ticker": str(row.get("newSymbol") or row.get("symbol") or ""),
                    "old_ticker": str(row.get("oldSymbol") or row.get("previousSymbol") or ""),
                    "date":       str(row.get("date") or ""),
                    "source":     "StockAnalysis",
                })
            print(f"   {len(out)} SA changes (NEXT_DATA)")
            return out
        except Exception:
            pass

    soup = BeautifulSoup(r.text, "lxml")
    table = soup.find("table")
    if not table:
        return []

    headers = [th.get_text(strip=True).lower() for th in table.find("tr").find_all(["th","td"])]
    def col_idx(names):
        for n in names:
            for i, h in enumerate(headers):
                if n in h: return i
        return None

    i_new  = col_idx(["new", "symbol"]) or 0
    i_old  = col_idx(["old", "previous"]) or 1
    i_date = col_idx(["date"]) or 2

    results = []
    for row in table.find_all("tr")[1:]:
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cells) >= 2:
            results.append({
                "new_ticker": cells[i_new] if i_new < len(cells) else "",
                "old_ticker": cells[i_old] if i_old < len(cells) else "",
                "date":       cells[i_date] if i_date < len(cells) else "",
                "source":     "StockAnalysis",
            })
    print(f"   {len(results)} SA changes")
    return results

# ── 7. DilutionTracker S1 ─────────────────────────────────────────────────────
def scrape_s1():
    print("→ DilutionTracker S1 (Playwright)...")
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
            ctx = browser.new_context(user_agent=S.headers["User-Agent"])
            page = ctx.new_page()

            api_data = []
            def on_response(response):
                if "api" in response.url and ("s1" in response.url or "filing" in response.url):
                    try:
                        api_data.append(response.json())
                    except Exception:
                        pass

            page.on("response", on_response)
            page.goto("https://dilutiontracker.com/app/s1",
                      wait_until="domcontentloaded", timeout=35000)
            page.wait_for_timeout(6000)
            html = page.content()
            browser.close()

        # Try API responses
        for d in api_data:
            rows = d.get("data") or d.get("filings") or (d if isinstance(d, list) else [])
            if rows:
                out = []
                for row in rows[:300]:
                    if isinstance(row, dict):
                        out.append({
                            "ticker":  str(row.get("ticker") or row.get("symbol") or ""),
                            "company": str(row.get("company") or row.get("name") or ""),
                            "date":    str(row.get("date") or row.get("filedDate") or ""),
                            "type":    str(row.get("type") or "S-1"),
                        })
                if out:
                    print(f"   {len(out)} S1 (API)")
                    return out

        # HTML fallback
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        results = []
        if table:
            headers = [th.get_text(strip=True).lower() for th in table.find("tr").find_all(["th","td"])]
            def ci(names):
                for n in names:
                    for i, h in enumerate(headers):
                        if n in h: return i
                return None
            i_ticker  = ci(["ticker","symbol"]) or 0
            i_company = ci(["company","name"]) or 1
            i_type    = ci(["type","form"])
            i_date    = ci(["date","filed"]) or 2

            for row in table.find_all("tr")[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cells) >= 2:
                    results.append({
                        "ticker":  cells[i_ticker] if i_ticker < len(cells) else "",
                        "company": cells[i_company] if i_company < len(cells) else "",
                        "type":    cells[i_type] if i_type and i_type < len(cells) else "S-1",
                        "date":    cells[i_date] if i_date < len(cells) else "",
                    })
        print(f"   {len(results)} S1 (HTML)")
        return results

    except ImportError:
        print("  Playwright not installed", file=sys.stderr)
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
        r = safe_get("https://finviz.com/insidertrading?tc=1",
                     headers={**S.headers, "Referer": "https://finviz.com/"})
    if not r:
        return []

    soup = BeautifulSoup(r.text, "lxml")
    # Find insider trading table
    table = None
    for t in soup.find_all("table"):
        text = t.get_text()
        if "Purchase" in text or "Sale" in text or "Buy" in text:
            table = t
            break

    if not table:
        print("  Finviz: no table", file=sys.stderr)
        return []

    results = []
    rows = table.find_all("tr")
    # Detect header
    header_idx = 0
    headers = []
    for i, row in enumerate(rows):
        cells = [td.get_text(strip=True) for td in row.find_all(["td","th"])]
        if "Ticker" in cells or "Owner" in cells:
            headers = [c.lower() for c in cells]
            header_idx = i
            break

    def ci(names):
        for n in names:
            for i, h in enumerate(headers):
                if n in h: return i
        return None

    i_ticker = ci(["ticker"]) 
    i_owner  = ci(["owner","insider"])
    i_rel    = ci(["relationship","title"])
    i_date   = ci(["date"])
    i_trans  = ci(["transaction","type"])
    i_cost   = ci(["cost","price"])
    i_shares = ci(["#shares","shares"])
    i_value  = ci(["value"])

    for row in rows[header_idx+1:]:
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cells) < 4:
            continue

        def gc(idx):
            return cells[idx] if idx is not None and idx < len(cells) else ""

        ticker = gc(i_ticker) if i_ticker is not None else (cells[1] if len(cells) > 1 else "")
        if not ticker or not is_ticker(ticker):
            continue

        results.append({
            "ticker":       ticker,
            "owner":        gc(i_owner),
            "relationship": gc(i_rel),
            "date":         gc(i_date),
            "transaction":  gc(i_trans),
            "cost":         gc(i_cost),
            "shares":       gc(i_shares),
            "value":        gc(i_value),
        })

    print(f"   {len(results)} insider rows")
    return results

# ── MAIN BUILD ────────────────────────────────────────────────────────────────
def build():
    result = {
        "updated":         datetime.now().strftime("%b %-d, %Y %H:%M UTC"),
        "regsho_tickers":  [],
        "regsho_date":     "",
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
    result["regsho_date"]    = regsho_date or ""
    regsho_set = set(regsho_list)

    # Screener
    screener = scrape_chartexchange()
    for row in screener:
        row["reg_sho"] = row["ticker"] in regsho_set

    # Float from AskEdgar — fetch for all screener tickers (most accurate source)
    all_tickers = [r["ticker"] for r in screener]
    fmap = scrape_float_askedgar(all_tickers)
    for row in screener:
        if row["ticker"] in fmap:
            row["float"] = fmap[row["ticker"]]  # AskEdgar overrides ChartExchange

    # S1 filings
    s1 = scrape_s1()
    result["s1_filings"] = s1
    s1_map = {r["ticker"]: r["date"] for r in s1}

    # Insiders
    insiders = scrape_insiders()
    result["insiders"] = insiders
    buyer_map = {}
    for ins in insiders:
        t = ins["ticker"]
        if t not in buyer_map:
            buyer_map[t] = ins["owner"]

    # Enrich screener
    for row in screener:
        row["s1_date"] = s1_map.get(row["ticker"], "-")
        row["buyer"]   = buyer_map.get(row["ticker"], "-")

    result["screener"] = screener

    # Splits
    result["splits_recent"]   = scrape_splits_recent()
    result["splits_upcoming"] = scrape_splits_upcoming()

    # Ticker changes
    changes_n = scrape_changes_nasdaq()
    changes_s = scrape_changes_stockanalysis()
    seen, merged = set(), []
    for row in changes_n + changes_s:
        key = (row["new_ticker"], row["old_ticker"])
        if key not in seen:
            seen.add(key)
            merged.append(row)
    result["ticker_changes"] = merged

    return result

if __name__ == "__main__":
    print("=" * 60)
    print("Short Tracker Scraper")
    print("=" * 60)
    data = build()
    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("\n✅ data.json written")
    print(f"   Screener:        {len(data['screener'])}")
    print(f"   RegSHO:          {len(data['regsho_tickers'])}")
    print(f"   Recent splits:   {len(data['splits_recent'])}")
    print(f"   Upcoming splits: {len(data['splits_upcoming'])}")
    print(f"   Ticker changes:  {len(data['ticker_changes'])}")
    print(f"   S1 filings:      {len(data['s1_filings'])}")
    print(f"   Insiders:        {len(data['insiders'])}")
