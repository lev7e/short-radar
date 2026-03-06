"""
SHORT RADAR — scraper.py
GitHub Actions, hafta içi 07:00 UTC

Kaynaklar:
  1. Chartexchange   — C2B, short volume  (HTML içindeki JSON parse)
  2. FINRA CDN       — RegSHO + Short Interest
  3. StockAnalysis   — Reverse splits (__NEXT_DATA__ parse)
  4. Finviz          — Insider işlemleri
  5. SEC EDGAR EFTS  — S-1 / S-1/A başvuruları
  6. SEC EDGAR XBRL  — Float + Warrant
"""

import json
import os
import re
import time
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup

OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
BROWSER_HEADERS = {
    "User-Agent":      BROWSER_UA,
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}
SEC_HEADERS = {
    "User-Agent": "ShortRadar research contact@example.com",
    "Accept":     "application/json",
}

SOURCE_STATUS = {}


# ══════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════
def load_existing(filename):
    path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save(filename, data, min_records=1):
    n = len(data) if isinstance(data, (list, dict)) else 1
    if isinstance(data, list) and n < min_records:
        old   = load_existing(filename)
        old_n = len(old) if isinstance(old, list) else 0
        print(f"  ⚠  {filename} — {n} kayıt < eşik {min_records}. Eski korundu ({old_n}).")
        return False
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"  ✓  {path}  ({n} kayıt)")
    return True


def save_meta(data):
    path = os.path.join(OUTPUT_DIR, "meta.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"  ✓  {path}")


def to_float(val, default=None):
    if val is None: return default
    s = str(val).strip()
    if s in ("-", "", "N/A", "n/a", "--"): return default
    s = s.replace("%","").replace(" ","")
    # Handle B/M/K suffixes (e.g. "1.23B", "456M", "5K")
    mul = 1
    if s and s[-1].upper() == "B": s, mul = s[:-1], 1_000_000_000
    elif s and s[-1].upper() == "M": s, mul = s[:-1], 1_000_000
    elif s and s[-1].upper() == "K": s, mul = s[:-1], 1_000
    # European comma vs dot
    if "," in s and "." in s:
        s = s.replace(",","")          # 1,234.56 → 1234.56
    elif "," in s and s.count(",") == 1 and len(s.split(",")[1]) <= 2:
        s = s.replace(",",".")          # 1,5 → 1.5 (European)
    else:
        s = s.replace(",","")          # 1,234 → 1234
    try:
        return float(s) * mul
    except Exception:
        return default


def parse_split_ratio(s):
    if not s:
        return None
    s = str(s).lower().replace("-", " ").replace("–", " ")
    m = re.search(r"([\d.]+)\s*(?:for|:)\s*([\d.]+)", s)
    if not m:
        return None
    new_, old_ = float(m.group(1)), float(m.group(2))
    if new_ == 0:
        return None
    return round(old_ / new_, 4) if old_ > new_ else None


def normalize_date(raw):
    if not raw:
        return ""
    raw = raw.strip()
    for fmt in ["%Y-%m-%d", "%b %d, %Y", "%B %d, %Y", "%m/%d/%Y", "%b %d,%Y"]:
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw


def workdays_back(n):
    """Son n iş gününü YYYYMMDD formatında döner."""
    days, d = [], datetime.now()
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.strftime("%Y%m%d"))
        d -= timedelta(days=1)
    return days


def _normalize_ce_row(row, hdrs):
    """
    Chartexchange HTML tablo satırını normalize eder.
    Log'dan tespit edilen gerçek kolon adları:
    '#', 'Symbol', 'BorrowFee%IBKR', 'BorrowSharesAvailableIBKR',
    'SharesFloat', 'MarketCap', 'Price', 'Change\xa0%', 'Volume',
    '10DayAvgVolume', 'ShortVolume%', 'ShortInterest%', 'SI%Change',
    'ShortVolume%30D', 'PreMarketPrice', 'PreMarketChange%'
    """
    def pick(*keys):
        for k in keys:
            v = row.get(k)
            if v not in ("", "-", "N/A", "n/a", None, "0", 0):
                cleaned = str(v).replace("\xa0","").replace(",","").strip()
                return cleaned if cleaned not in ("", "-") else None
        return None

    # Ticker: Symbol kolonu
    ticker = pick("Symbol", "symbol", "Ticker", "ticker", "Display", "display")
    if not ticker and hdrs and len(hdrs) > 1:
        # İkinci kolon genellikle Symbol (#, Symbol, ...)
        ticker = str(row.get(hdrs[1], "")).strip()
    if not ticker:
        return {"ticker": "", "symbol": ""}

    return {
        "symbol":                       ticker,
        "ticker":                       ticker,
        # Gerçek CE kolon adları (log'dan)
        "borrow_fee_rate_ib":           pick("BorrowFee%IBKR", "Borrow Rate", "C2B Rate",
                                             "borrow_fee_rate_ib", "BorrowFee%"),
        "borrow_fee_avail_ib":          pick("BorrowSharesAvailableIBKR", "Available",
                                             "borrow_fee_avail_ib", "SharesAvailable"),
        "shares_float":                 pick("SharesFloat", "Float", "shares_float",
                                             "Shares Float", "FloatShares"),
        "reg_price":                    pick("Price", "reg_price", "Close", "Last"),
        "reg_change_pct":               pick("Change\xa0%", "Change%", "Change %",
                                             "reg_change_pct", "Chg%"),
        "reg_volume":                   pick("Volume", "Vol", "reg_volume"),
        "10_day_avg_vol":               pick("10DayAvgVolume", "Avg Vol", "AvgVolume",
                                             "10_day_avg_vol", "10D Avg Vol"),
        "shortint_pct":                 pick("ShortInterest%", "SI%", "Short Interest %",
                                             "shortint_pct", "ShortInt%"),
        "shortint_position_change_pct": pick("SI%Change", "SI Chg %", "SIChange",
                                             "shortint_position_change_pct"),
        "shortvol_all_short_pct":       pick("ShortVolume%", "Short Vol %",
                                             "shortvol_all_short_pct"),
        "shortvol_all_short_pct_30d":   pick("ShortVolume%30D", "30D Short %",
                                             "shortvol_all_short_pct_30d"),
        "pre_price":                    pick("PreMarketPrice", "Pre Price", "pre_price"),
        "pre_change_pct":               pick("PreMarketChange%", "Pre Chg %", "pre_change_pct"),
        "market_cap":                   pick("MarketCap", "Market Cap", "market_cap"),
        "_source":                      "ce_html",
    }


def next_data(html):
    """Next.js sayfasındaki __NEXT_DATA__ JSON'unu parse eder."""
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None


# ══════════════════════════════════════════════════
# 1. CHARTEXCHANGE — C2B + Short Volume
#    HTTP 200 döner ama JSON değil HTML gelir.
#    HTML içinde gömülü JSON datayı parse ederiz.
# ══════════════════════════════════════════════════
def fetch_chartexchange():
    print("\n[1/6] Chartexchange C2B taranıyor...")

    session = requests.Session()
    session.headers.update({**BROWSER_HEADERS,
                             "Accept": "text/html,application/xhtml+xml,*/*;q=0.9"})

    # Ana sayfa → session cookie
    try:
        r0 = session.get("https://chartexchange.com/", timeout=20)
        print(f"    Ana sayfa: HTTP {r0.status_code}, cookie: {bool(session.cookies)}")
        time.sleep(1.5)
    except Exception as e:
        print(f"    Ana sayfa uyarı: {e}")

    COLS = (
        "display,borrow_fee_rate_ib,borrow_fee_avail_ib,"
        "shares_float,market_cap,reg_price,reg_change_pct,reg_volume,"
        "10_day_avg_vol,shortvol_all_short,shortvol_all_short_pct,"
        "shortint_db_pct,shortint_pct,shortint_position_change_pct,"
        "shortvol_all_short_pct_30d,pre_price,pre_change_pct"
    )

    all_rows, page = [], 1
    while True:
        url = (
            f"https://chartexchange.com/screener/?page={page}"
            "&equity_type=ad,cs&exchange=BATS,NASDAQ,NYSE,NYSEAMERICAN"
            "&currency=USD&shares_float=%3C5000000&reg_price=%3C6,%3E0.8"
            f"&borrow_fee_avail_ib=%3C100000&per_page=100&view_cols={COLS}"
            "&sort=borrow_fee_rate_ib,desc&format=json"
        )
        try:
            r = session.get(url, timeout=30, headers={
                "Accept":           "application/json, */*; q=0.01",
                "X-Requested-With": "XMLHttpRequest",
                "Referer":          "https://chartexchange.com/screener/",
            })
            print(f"    Sayfa {page}: HTTP {r.status_code}, {len(r.content)} byte, "
                  f"Content-Type: {r.headers.get('Content-Type','?')[:40]}")

            # Önce JSON dene
            rows = None
            ct = r.headers.get("Content-Type", "")
            if "json" in ct:
                try:
                    data = r.json()
                    rows = data.get("data", data) if isinstance(data, dict) else data
                except Exception as e:
                    print(f"    JSON parse hatası: {e}")

            # JSON gelmediyse HTML içindeki __NEXT_DATA__ veya gömülü JSON dene
            if rows is None and r.status_code == 200:
                html = r.text
                # Yöntem 1: __NEXT_DATA__
                nd = next_data(html)
                if nd:
                    # screener verisi props.pageProps.data veya benzeri
                    for path in [["props","pageProps","data"],
                                 ["props","pageProps","rows"],
                                 ["props","pageProps","screener"]]:
                        node = nd
                        try:
                            for key in path:
                                node = node[key]
                            if isinstance(node, list) and node:
                                rows = node
                                print(f"    __NEXT_DATA__ ({'.'.join(path)}): {len(rows)} satır")
                                break
                        except (KeyError, TypeError):
                            pass

                # Yöntem 2: window.__data__ veya benzer global değişken
                if rows is None:
                    m2 = re.search(r'window\.__(?:data|rows|screener)__\s*=\s*(\[.*?\]);', html, re.DOTALL)
                    if m2:
                        try:
                            rows = json.loads(m2.group(1))
                            print(f"    window global: {len(rows)} satır")
                        except Exception:
                            pass

                # Yöntem 3: HTML tablo
                if rows is None:
                    soup = BeautifulSoup(html, "html.parser")
                    table = soup.find("table")
                    if table:
                        soup2 = BeautifulSoup(html, "html.parser")
                        # CE tablo yapısını debug et
                        all_trs = table.find_all("tr")
                        all_ths = table.find_all("th")
                        print(f"    CE tablo: {len(all_trs)} tr, {len(all_ths)} th")
                        if all_trs:
                            first_tr = all_trs[0]
                            print(f"    İlk tr içeriği: {str(first_tr)[:200]}")

                        # Başlıkları bul: th → thead>td → ilk tr td → colgroup → data-* attr
                        hdrs = []
                        # 1. th
                        hdrs = [th.get_text(strip=True) for th in all_ths if th.get_text(strip=True)]
                        # 2. thead içindeki td
                        if not hdrs:
                            thead = table.find("thead")
                            if thead:
                                hdrs = [td.get_text(strip=True) for td in thead.find_all("td") if td.get_text(strip=True)]
                        # 3. İlk tr'nin td'leri
                        if not hdrs and all_trs:
                            hdrs = [td.get_text(strip=True) for td in all_trs[0].find_all("td") if td.get_text(strip=True)]
                            data_trs = all_trs[1:]
                        else:
                            data_trs = all_trs[1:] if all_trs else []

                        print(f"    Başlıklar: {hdrs[:8]}")
                        raw_rows = []
                        for tr in data_trs:
                            cells = [td.get_text(strip=True) for td in tr.find_all(["td","th"])]
                            if cells and len(cells) >= 3:
                                raw_rows.append(cells)
                        print(f"    Ham satır: {len(raw_rows)}, örnek: {str(raw_rows[0])[:150] if raw_rows else 'yok'}")

                        # hdrs boşsa ilk satır başlıktır
                        if not hdrs and raw_rows:
                            hdrs = raw_rows[0]
                            raw_rows = raw_rows[1:]
                            print(f"    İlk satırdan başlık: {hdrs[:8]}")

                        if raw_rows and hdrs:
                            dict_rows = [dict(zip(hdrs, cells)) for cells in raw_rows]
                            rows = [_normalize_ce_row(r, hdrs) for r in dict_rows]
                            rows = [r for r in rows if r.get("ticker","").strip()]
                            print(f"    Normalize: {len(rows)} ticker")
                        if not rows:
                            rows = None

                if rows is None:
                    print(f"    İçerik başı: {html[:300]}")

            if rows is None or not isinstance(rows, list):
                print(f"    HTTP {r.status_code} — veri alınamadı")
                SOURCE_STATUS["chartexchange"] = f"error:no_data_http{r.status_code}"
                break

            all_rows.extend(rows)
            print(f"    +{len(rows)} satır, toplam {len(all_rows)}")
            if len(rows) < 100:
                break
            page += 1
            time.sleep(2.5)  # Rate limit

        except Exception as e:
            print(f"    İstisna: {e}")
            SOURCE_STATUS["chartexchange"] = f"error:{e}"
            break

    if all_rows:
        SOURCE_STATUS["chartexchange"] = f"ok:{len(all_rows)}"
        # Debug: ilk satırın C2B değerini logla
        sample = all_rows[0]
        print(f"    Sample: ticker={sample.get('ticker')} c2b={sample.get('borrow_fee_rate_ib')} float={sample.get('shares_float')}")

    # CE yetersizse iborrowdesk.com'dan ek veri çek
    if len(all_rows) < 10:
        print("    CE yetersiz, iborrowdesk.com deneniyor...")
        ibd = fetch_iborrowdesk()
        if ibd:
            all_rows = ibd
            SOURCE_STATUS["chartexchange"] = f"ok_ibd:{len(all_rows)}"

    save("chartexchange.json", all_rows, min_records=10)
    return all_rows


def fetch_iborrowdesk():
    """
    iborrowdesk.com — borrow rate + availability
    Herkese açık, login gerektirmez.
    CSV export: https://iborrowdesk.com/api/ticker/AAPL
    Tüm liste: sayfalı HTML scrape
    """
    print("    [iborrowdesk] taranıyor...")
    rows = []
    try:
        # Ana liste sayfası
        r = requests.get(
            "https://iborrowdesk.com/",
            headers={**BROWSER_HEADERS, "Accept": "text/html,*/*"},
            timeout=20,
        )
        print(f"    iborrowdesk ana: HTTP {r.status_code}, {len(r.content)}b")
        if r.status_code != 200:
            return []

        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        if table:
            hdrs = [th.get_text(strip=True) for th in table.find_all("th")]
            print(f"    iborrowdesk başlıklar: {hdrs}")
            for tr in table.find_all("tr")[1:200]:
                cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                if cells and len(cells) >= 2:
                    row = dict(zip(hdrs, cells))
                    # Normalize
                    ticker = row.get("Symbol") or row.get("Ticker") or (cells[0] if cells else "")
                    rate   = row.get("Fee") or row.get("Rate") or row.get("Borrow Rate","")
                    avail  = row.get("Available") or row.get("Availability","")
                    rows.append({
                        "symbol":             ticker.strip(),
                        "ticker":             ticker.strip(),
                        "borrow_fee_rate_ib": rate.replace("%","").strip(),
                        "borrow_fee_avail_ib":avail.replace(",","").strip(),
                        "_source":            "iborrowdesk",
                    })
        print(f"    iborrowdesk: {len(rows)} ticker")
    except Exception as e:
        print(f"    iborrowdesk hata: {e}")
    return rows


# ══════════════════════════════════════════════════
# 2. RegSHO — FINRA CDN daily + NASDAQ Trader fallback
# ══════════════════════════════════════════════════
def _parse_pipe(text, source):
    rows, lines = [], text.strip().split("\n")
    if len(lines) < 2:
        return rows
    hdrs = [h.strip() for h in lines[0].split("|")]
    for line in lines[1:]:
        parts = [p.strip() for p in line.split("|")]
        if not parts or not parts[0] or parts[0].lower() in ("symbol",""):
            continue
        row = dict(zip(hdrs, parts))
        if "Symbol" not in row:
            row["Symbol"] = parts[0]
        row["_source"] = source
        rows.append(row)
    return rows


def fetch_regsho():
    print("\n[2/6] RegSHO threshold list...")
    rows = []

    # ── Primary: NASDAQ Trader HTML page ──────────────────────────────────
    try:
        r = requests.get(
            "https://nasdaqtrader.com/Trader.aspx?id=RegSHOThreshold",
            headers={**BROWSER_HEADERS, "Accept": "text/html,*/*"},
            timeout=30,
        )
        print(f"    NASDAQ HTML: HTTP {r.status_code}, {len(r.content)}b")
        if r.status_code == 200:
            from bs4 import BeautifulSoup as _BS
            soup = _BS(r.text, "html.parser")
            # Find the data table (has "Symbol" header)
            for table in soup.find_all("table"):
                hdrs = [th.get_text(strip=True) for th in table.find_all("th")]
                if not hdrs:
                    first_tr = table.find("tr")
                    if first_tr:
                        hdrs = [td.get_text(strip=True) for td in first_tr.find_all("td")]
                if any(h in ("Symbol","Security","Ticker") for h in hdrs):
                    print(f"    RegSHO HTML columns: {hdrs}")
                    # Find date column — any header containing "Date" or "List"
                    date_col = next((h for h in hdrs if "date" in h.lower() or "list" in h.lower()), None)
                    mkt_col  = next((h for h in hdrs if "market" in h.lower() or "exchange" in h.lower()), None)
                    sym_col  = next((h for h in hdrs if h in ("Symbol","Security","Ticker")), hdrs[0] if hdrs else "Symbol")
                    for tr in table.find_all("tr")[1:]:
                        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                        if len(cells) >= 2:
                            row = dict(zip(hdrs, cells))
                            sym = row.get(sym_col) or cells[0]
                            sym = sym.strip().upper()
                            if sym and sym not in ("SYMBOL","TICKER","SECURITY",""):
                                rows.append({
                                    "Symbol":               sym,
                                    "Market":               row.get(mkt_col,"") if mkt_col else "",
                                    "Threshold List Date":  row.get(date_col,"") if date_col else "",
                                })
                    if rows:
                        print(f"    ✓ NASDAQ HTML: {len(rows)} ticker")
                        break
    except Exception as e:
        print(f"    NASDAQ HTML error: {e}")

    # ── Fallback: NASDAQ Trader TXT files ─────────────────────────────────
    if not rows:
        for date_str in workdays_back(10):
            for url in [
                f"https://www.nasdaqtrader.com/dynamic/symdir/regsho/nasdaqth{date_str}.txt",
                f"https://nasdaqtrader.com/dynamic/symdir/regsho/nasdaqth{date_str}.txt",
            ]:
                try:
                    r = requests.get(url, headers=BROWSER_HEADERS, timeout=15)
                    if r.status_code != 200:
                        continue
                    rows = _parse_pipe(r.text, "NASDAQ")
                    if rows:
                        print(f"    ✓ NASDAQ TXT ({date_str}): {len(rows)} ticker")
                        break
                except Exception as e:
                    pass
            if rows:
                break

    if not rows:
        print("    RegSHO: no data found")

    SOURCE_STATUS["regsho"] = f"ok:{len(rows)}" if rows else "error:all_failed"
    save("regsho.json", rows, min_records=1)
    return rows


def fetch_splits():
    print("\n[3/6] Split listesi çekiliyor (StockAnalysis)...")
    rows = []
    # ── Upcoming #1: TipRanks (needs TIPRANKS_COOKIE secret in GitHub) ────────
    tipranks_cookie = os.environ.get("TIPRANKS_COOKIE","")
    if tipranks_cookie:
        try:
            tr = requests.get(
                "https://www.tipranks.com/calendars/stock-splits/upcoming",
                headers={
                    **BROWSER_HEADERS,
                    "Accept": "text/html,*/*",
                    "Cookie": tipranks_cookie,
                    "Referer": "https://www.tipranks.com/",
                },
                timeout=20,
            )
            print(f"    TipRanks: HTTP {tr.status_code}, {len(tr.content)}b")
            if tr.status_code == 200:
                import json as _json
                nd = next_data(tr.text)
                tr_rows = []
                if nd:
                    nd_str = _json.dumps(nd)
                    # pageProps → various paths
                    pp = nd.get("props",{}).get("pageProps",{})
                    for key in list(pp.keys()):
                        val = pp[key]
                        if isinstance(val, list) and val and isinstance(val[0], dict):
                            if any(k in str(val[0]) for k in ["ticker","symbol","ratio","split"]):
                                tr_rows = val
                                print(f"    TipRanks pageProps[{key}]: {len(tr_rows)} rows")
                                break
                if tr_rows:
                    for r2 in tr_rows:
                        sym       = (r2.get("ticker") or r2.get("symbol","")).upper()
                        ratio_str = str(r2.get("ratio") or r2.get("splitRatio",""))
                        raw_date  = (r2.get("exDate") or r2.get("date") or
                                     r2.get("splitDate") or r2.get("payableDate",""))
                        ratio     = parse_split_ratio(ratio_str)
                        norm_date = normalize_date(str(raw_date))
                        if not sym: continue
                        rows.append({
                            "Symbol": sym, "Ratio": ratio_str,
                            "Date": norm_date, "is_reverse": ratio is not None,
                            "split_ratio": ratio, "split_date": norm_date,
                            "list_type": "upcoming",
                        })
                    print(f"    TipRanks upcoming: {len([r2 for r2 in rows if r2['list_type']=='upcoming'])} splits")
        except Exception as e:
            print(f"    TipRanks error: {e}")
    else:
        print("    TipRanks: no TIPRANKS_COOKIE secret set, skipping")

    # ── Upcoming #2: Yahoo Finance calendar (confirmed working) ───────────────
    try:
        yr = requests.get(
            "https://finance.yahoo.com/calendar/splits",
            headers={**BROWSER_HEADERS, "Accept": "text/html,*/*"},
            timeout=30,
        )
        print(f"    Yahoo splits: HTTP {yr.status_code}, {len(yr.content)}b")
        if yr.status_code == 200:
            from bs4 import BeautifulSoup as _BS2
            ys = _BS2(yr.text, "html.parser")
            for yt in ys.find_all("table"):
                yh = [th.get_text(strip=True) for th in yt.find_all("th")]
                if "Symbol" in yh and "Ratio" in yh:
                    for tr in yt.find_all("tr")[1:]:
                        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                        if len(cells) < 3: continue
                        row  = dict(zip(yh, cells))
                        sym  = row.get("Symbol","").split(".")[0].strip().upper()
                        ratio_str = row.get("Ratio","")
                        raw_date  = row.get("Payable On","") or row.get("Date","")
                        ratio     = parse_split_ratio(ratio_str)
                        norm_date = normalize_date(raw_date)
                        if not sym: continue
                        try:
                            from datetime import date as _date
                            split_dt   = datetime.strptime(norm_date, "%Y-%m-%d").date() if norm_date else None
                            is_upcoming = split_dt and split_dt >= _date.today()
                        except Exception:
                            is_upcoming = True
                        rows.append({
                            "Symbol": sym, "Ratio": ratio_str,
                            "Date": norm_date, "Company": row.get("Company",""),
                            "is_reverse": ratio is not None, "split_ratio": ratio,
                            "split_date": norm_date,
                            "list_type": "upcoming" if is_upcoming else "recent",
                        })
                    print(f"    Yahoo: {len(rows)} splits parsed")
                    break
    except Exception as e:
        print(f"    Yahoo splits error: {e}")

    SPLIT_URLS = {
        "recent":   [
            "https://stockanalysis.com/actions/splits/",
            "https://stockanalysis.com/actions/splits/?p=quarterly",
        ],
    }
    for label, urls in SPLIT_URLS.items():
        url, r = urls[0], None
        for u in urls:
            try:
                r = requests.get(u, headers={**BROWSER_HEADERS,"Accept":"text/html,*/*"}, timeout=30)
                print(f"    {label} {u}: HTTP {r.status_code}, {len(r.content)}b")
                if r.status_code == 200:
                    url = u; break
            except Exception as e:
                print(f"    {label} {u}: {e}")
        if not r or r.status_code != 200:
            continue
        try:
            page_rows = []
            # __NEXT_DATA__
            nd = next_data(r.text)
            if nd:
                for path in [["props","pageProps","data"],["props","pageProps","splits"],
                             ["props","pageProps","tableData"]]:
                    node = nd
                    try:
                        for key in path:
                            node = node[key]
                        if isinstance(node, list) and node:
                            page_rows = node
                            print(f"    {label} __NEXT_DATA__: {len(page_rows)}")
                            break
                    except (KeyError, TypeError):
                        pass
            # HTML tablo fallback
            if not page_rows:
                soup = BeautifulSoup(r.text, "html.parser")
                table = soup.find("table")
                if table:
                    hdrs = [th.get_text(strip=True) for th in table.find_all("th")]
                    for tr in table.find_all("tr")[1:]:
                        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                        if cells:
                            page_rows.append(dict(zip(hdrs, cells)))
                    print(f"    {label} HTML: {len(page_rows)}")
            for row in page_rows:
                sym = (row.get("Symbol") or row.get("symbol") or row.get("ticker",""))
                ratio_str = (row.get("Ratio") or row.get("ratio") or
                             row.get("Split Ratio") or row.get("splitRatio",""))
                raw_date  = (row.get("Date") or row.get("date") or
                             row.get("Split Date") or row.get("splitDate",""))
                ratio     = parse_split_ratio(str(ratio_str))
                norm_date = normalize_date(str(raw_date))
                # Determine if upcoming: date in future OR label is "upcoming"
                try:
                    from datetime import date as _date
                    split_dt   = datetime.strptime(norm_date, "%Y-%m-%d").date() if norm_date else None
                    is_upcoming = label == "upcoming" or (split_dt and split_dt >= _date.today())
                except Exception:
                    is_upcoming = label == "upcoming"
                rows.append({
                    "Symbol":      sym, "Ratio": ratio_str,
                    "Date":        norm_date,
                    "is_reverse":  ratio is not None, "split_ratio": ratio,
                    "split_date":  norm_date,
                    "list_type":   "upcoming" if is_upcoming else "recent",
                })
            time.sleep(0.5)
        except Exception as e:
            print(f"    {label}: {e}")
    SOURCE_STATUS["splits"] = f"ok:{sum(1 for x in rows if x.get('is_reverse'))}"
    save("splits.json", rows, min_records=1)
    return rows


# ══════════════════════════════════════════════════
# 4. FINVIZ — Insider
# ══════════════════════════════════════════════════
def fetch_insider():
    print("\n[4/6] Finviz insider taranıyor...")
    rows = []
    try:
        for fin_url in [
            "https://finviz.com/insidertrading.ashx?or=-10&tv=100&tc=1&o=-transactionDate",
            "https://finviz.com/insidertrading?tc=1",
        ]:
            r = requests.get(fin_url, headers={**BROWSER_HEADERS,
                             "Accept": "text/html,*/*", "Referer": "https://finviz.com/"},
                             timeout=30)
            print(f"    HTTP {r.status_code}, {len(r.content)}b")
            if r.status_code == 200 and len(r.content) > 10000:
                break
        soup = BeautifulSoup(r.text, "html.parser")
        table = None
        for sel in [
            lambda s: s.find("table", {"id": "insider-trading-table"}),
            lambda s: s.find("table", class_=re.compile(r"insider|trading", re.I)),
            lambda s: next((t for t in s.find_all("table")
                           if any("Ticker" in str(th) for th in t.find_all("th"))), None),
        ]:
            table = sel(soup)
            if table:
                break
        if table:
            hdrs = [th.get_text(strip=True) for th in table.find_all("th")]
            for tr in table.find_all("tr")[1:100]:
                cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                if cells and len(cells) >= 4:
                    rows.append(dict(zip(hdrs, cells)))
        print(f"    {len(rows)} insider işlem")
        SOURCE_STATUS["insider"] = f"ok:{len(rows)}"
    except Exception as e:
        print(f"    Hata: {e}")
        SOURCE_STATUS["insider"] = f"error:{e}"
    save("insider.json", rows, min_records=5)
    return rows


# ══════════════════════════════════════════════════
# 5. SEC EDGAR — S-1 / S-1/A
# ══════════════════════════════════════════════════
def fetch_sec_s1():
    print("\n[5/6] SEC EDGAR S-1 başvuruları çekiliyor...")
    rows, seen = [], set()
    start = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    end   = datetime.now().strftime("%Y-%m-%d")
    for form_type in ["S-1", "S-1/A"]:
        try:
            r = requests.get(
                "https://efts.sec.gov/LATEST/search-index",
                params={"forms": form_type, "dateRange": "custom",
                        "startdt": start, "enddt": end},
                headers=SEC_HEADERS, timeout=30,
            )
            print(f"    {form_type}: HTTP {r.status_code}")
            if r.status_code != 200:
                continue
            data  = r.json()
            hits  = data.get("hits", {}).get("hits", [])
            if hits:
                s0  = hits[0].get("_source", {})
                dn0 = s0.get("display_names", "")
                dn_sample = str(dn0[0]) if isinstance(dn0, list) and dn0 else repr(dn0)[:80]
                print(f"    {form_type}: {len(hits)} hit, display_names[0]: {dn_sample}")
            for hit in hits:
                s = hit.get("_source", {})
                # display_names can be:
                # - list of dicts: [{"name": "Acme", "ticker": "ACM", "cik": "..."}]
                # - list of strings: ["CYBRIATECH INC  (CIK 0002072184)"]
                # - a plain string
                display = s.get("display_names") or []
                entity, ticker = "", ""

                if isinstance(display, list) and display:
                    first = display[0]
                    if isinstance(first, dict):
                        entity = first.get("name","").strip()
                        ticker = first.get("ticker","").strip()
                    else:
                        # String format: "COMPANY NAME (CIK 0002072184)"
                        raw = str(first)
                        entity = re.sub(r"\s*\(CIK[^)]*\)", "", raw).strip()
                elif isinstance(display, str) and display:
                    entity = re.sub(r"\s*\(CIK[^)]*\)", "", display).strip()

                # Fallback fields
                if not entity:
                    entity = (s.get("entity_name") or s.get("company_name") or "").strip()
                if not ticker:
                    t2 = s.get("ticker") or s.get("tickers") or ""
                    ticker = (t2[0] if isinstance(t2, list) and t2 else str(t2)).strip()
                filed = (s.get("file_date") or s.get("filed_at") or "")
                uid   = f"{entity}|{filed}"
                if uid in seen:
                    continue
                if not entity and not ticker:
                    continue
                seen.add(uid)
                # Try to extract ticker from various places
                if not ticker:
                    # "proposed: TICK" pattern in company name
                    m = re.search(r"\(proposed[:\s]+([A-Za-z]{1,5})\)", entity, re.I)
                    if m:
                        ticker = m.group(1).upper()
                if not ticker:
                    # Try to get ticker from ciks list via EDGAR submissions
                    # Only do this if we have a cik — batch lookup via _cik_map is too slow
                    # Instead use tickers field directly from the hit
                    tickers_field = s.get("tickers") or s.get("ticker") or []
                    if isinstance(tickers_field, list) and tickers_field:
                        ticker = str(tickers_field[0]).upper()
                    elif isinstance(tickers_field, str) and tickers_field:
                        ticker = tickers_field.upper()
                rows.append({"form": form_type, "ticker": ticker, "company": entity,
                             "filed_date": filed,
                             "edgar_url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company={requests.utils.quote(entity)}&type=S-1&dateb=&owner=include&count=10"})
            time.sleep(0.4)
        except Exception as e:
            print(f"    {form_type} hatası: {e}")
    print(f"    Toplam: {len(rows)} başvuru")
    SOURCE_STATUS["s1"] = f"ok:{len(rows)}"
    save("s1_edgar.json", rows, min_records=1)
    return rows


# ══════════════════════════════════════════════════
# (FINRA SI artık kullanılmıyor — askedgar kullanıyoruz)
# ══════════════════════════════════════════════════
def fetch_finra_short_interest():
    """
    FINRA SI artık kullanılmıyor — askedgar API kullanılıyor.
    Bu fonksiyon geriye dönük uyumluluk için bırakıldı.
    """
    return {}


def fetch_askedgar_si(tickers: list) -> dict:
    """
    askedgar.io API — per-ticker:
      /v1/stocks/short-interest?ticker=X  → SI, DTC
      /v1/sec/{ticker}/estimated-cash     → cash_per_share
      /v1/fmp/company/{ticker}/profile    → price, mktCap, float (sharesOutstanding)
    """
    print(f"\n[ASKEDGAR] Fetching {len(tickers)} tickers...")
    result  = {}
    HDR = {**BROWSER_HEADERS, "Accept": "application/json",
           "Referer": "https://app.askedgar.io/",
           "Origin":  "https://app.askedgar.io"}
    API = "https://api.askedgar.io"

    ok, err = 0, 0
    for ticker in tickers:
        rec = {}
        try:
            # 1. Short interest
            r = requests.get(f"{API}/v1/stocks/short-interest",
                             params={"ticker": ticker}, headers=HDR, timeout=10)
            if r.status_code == 200:
                d = r.json()
                rec["short_interest"]   = d.get("short_interest")
                rec["days_to_cover"]    = d.get("days_to_cover")
                rec["avg_daily_volume"] = d.get("avg_daily_volume")
                rec["si_date"]          = d.get("settlement_date","")
                ok += 1
            time.sleep(0.1)

            # 2. Estimated cash (cash per share)
            r2 = requests.get(f"{API}/v1/sec/{ticker}/estimated-cash",
                              headers=HDR, timeout=8)
            if r2.status_code == 200:
                d2 = r2.json()
                rec["estimated_cash"] = d2.get("estimated_cash")  # total cash $
                rec["cash_per_share"] = d2.get("cash_per_share")  # may exist
            time.sleep(0.1)

            # 3. Company profile (float = sharesOutstanding, price)
            r3 = requests.get(f"{API}/v1/fmp/company/{ticker}/profile",
                              headers=HDR, timeout=8)
            if r3.status_code == 200:
                d3 = r3.json()
                rec["profile_price"]      = d3.get("price")
                rec["profile_mktcap"]     = d3.get("mktCap")
                rec["shares_outstanding"] = d3.get("sharesOutstanding")
                # cash & cashPerShare may come directly from profile
                rec["cash_per_share"] = (
                    d3.get("cashPerShare") or
                    d3.get("cashPerShareTTM") or
                    d3.get("cash_per_share") or
                    rec.get("cash_per_share")
                )
                # Fallback: compute from estimated_cash / sharesOutstanding
                if not rec.get("cash_per_share"):
                    cash  = rec.get("estimated_cash")
                    shr   = d3.get("sharesOutstanding")
                    if cash and shr and shr > 0:
                        rec["cash_per_share"] = round(cash / shr, 4)
            time.sleep(0.1)

            if rec:
                result[ticker] = rec
        except Exception as e:
            err += 1

    print(f"    Askedgar: {ok} SI ok, {err} err, {len(result)} total")
    SOURCE_STATUS["askedgar_si"] = f"ok:{len(result)}" if result else "error:no_data"
    return result


def _cik_map(tickers):
    try:
        r = requests.get("https://www.sec.gov/files/company_tickers.json",
                         headers=SEC_HEADERS, timeout=30)
        mapping = {}
        for entry in r.json().values():
            t = entry.get("ticker","").upper()
            c = str(entry.get("cik_str","")).zfill(10)
            if t:
                mapping[t] = c
        result = {t: mapping[t.upper()] for t in tickers if t.upper() in mapping}
        print(f"    CIK: {len(result)}/{len(tickers)}")
        return result
    except Exception as e:
        print(f"    CIK error: {e}")
        return {}


def _latest_xbrl(facts, *concepts):
    priority = {"10-K":0,"10-Q":1,"S-1":2,"S-1/A":3,"8-K":4}
    for concept in concepts:
        for ns in ["us-gaap","dei"]:
            node = facts.get("facts",{}).get(ns,{}).get(concept,{})
            data = node.get("units",{}).get("shares",
                   node.get("units",{}).get("USD",[]))
            if not data:
                continue
            cands = [x for x in data if x.get("val") and x.get("end") and x.get("form")]
            if not cands:
                continue
            cands.sort(key=lambda x: (x.get("end",""),
                                      -priority.get(x.get("form",""),99)), reverse=True)
            b = cands[0]
            return b.get("val"), b.get("filed", b.get("end")), b.get("form")
    return None, None, None


def fetch_edgar_floats(tickers, split_map):
    print(f"\n[6/6] EDGAR XBRL — {len(tickers)} ticker...")
    if not tickers:
        SOURCE_STATUS["edgar_float"] = "skip:no_tickers"
        return {}
    cik_map = _cik_map(tickers)
    result, not_found = {}, []

    for ticker in tickers:
        cik = cik_map.get(ticker.upper())
        if not cik:
            not_found.append(ticker)
            continue
        try:
            r = requests.get(
                f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json",
                headers=SEC_HEADERS, timeout=25,
            )
            if r.status_code != 200:
                not_found.append(ticker); time.sleep(0.15); continue
            facts = r.json()
            float_val, float_date, float_form = _latest_xbrl(
                facts, "CommonStockSharesOutstanding",
                "EntityCommonStockSharesOutstanding",
                "FloatShares", "CommonStockSharesIssued")
            warrant_val, _, _ = _latest_xbrl(
                facts, "ClassOfWarrantOrRightOutstanding",
                "WarrantsAndRightsOutstanding")
            sp       = split_map.get(ticker, {})
            sp_ratio = sp.get("ratio")
            sp_date  = sp.get("date","")
            edgar_dt = (float_date or "")[:10]
            is_pre   = bool(sp_ratio and sp_date and edgar_dt and edgar_dt < sp_date)
            est_post = int(float_val / sp_ratio) if (is_pre and float_val and sp_ratio) else None
            result[ticker] = {
                "ticker": ticker, "cik": cik,
                "float_shares": float_val, "float_date": float_date,
                "float_form": float_form, "warrant_shares": warrant_val,
                "float_is_presplit": is_pre, "est_post_split_float": est_post,
                "split_ratio": sp_ratio, "split_date": sp_date,
            }
        except Exception:
            not_found.append(ticker)
        time.sleep(0.12)

    if not_found:
        s = ", ".join(not_found[:6]) + (f" +{len(not_found)-6}" if len(not_found)>6 else "")
        print(f"    Bulunamadı: {s}")
    print(f"    {len(result)} float alındı")
    SOURCE_STATUS["edgar_float"] = f"ok:{len(result)}"
    save("floats.json", list(result.values()), min_records=1)
    return result


# ══════════════════════════════════════════════════
# SQUEEZE SKORU
# ══════════════════════════════════════════════════
def squeeze_score(s):
    score, reasons = 0, []
    sf = to_float(s.get("short_float_pct"))
    if sf is not None:
        if   sf >= 50: score+=25; reasons.append("SI%≥50")
        elif sf >= 30: score+=18; reasons.append("SI%≥30")
        elif sf >= 15: score+=10; reasons.append("SI%≥15")
        elif sf >=  5: score+=4
    c2b = to_float(s.get("c2b"))
    if c2b is not None:
        if   c2b>=200: score+=25; reasons.append("C2B≥200%")
        elif c2b>=100: score+=18; reasons.append("C2B≥100%")
        elif c2b>= 50: score+=12; reasons.append("C2B≥50%")
        elif c2b>= 20: score+=6
        elif c2b>= 10: score+=3
    fl = to_float(s.get("diluted_float") or s.get("float"))
    if fl is not None:
        if   fl<  500_000: score+=20; reasons.append("Float<500K")
        elif fl<1_000_000: score+=15; reasons.append("Float<1M")
        elif fl<2_000_000: score+=10; reasons.append("Float<2M")
        elif fl<5_000_000: score+=5
    dtc = to_float(s.get("dtc"))
    if dtc is not None:
        if   dtc>=10: score+=15; reasons.append("DTC≥10")
        elif dtc>= 5: score+=10; reasons.append("DTC≥5")
        elif dtc>= 2: score+=5
    if s.get("reg_sho")=="✅":
        score+=10; reasons.append("RegSHO")
    si_chg = to_float(s.get("si_change"))
    if si_chg is not None:
        if   si_chg>=50: score+=5;  reasons.append("SI+%≥50")
        elif si_chg>=20: score+=3;  reasons.append("SI+%≥20")
        elif si_chg<-20: score-=3
    return max(0, min(100, score)), reasons


# ══════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════
if __name__ == "__main__":
    run_start = datetime.now(timezone.utc)
    print(f"\n{'='*60}")
    print(f"  SHORT RADAR — {run_start.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")

    ce       = fetch_chartexchange()
    rs       = fetch_regsho()
    sp       = fetch_splits()
    ins      = fetch_insider()
    s1       = fetch_sec_s1()
    finra_si = {}   # artık kullanılmıyor

    # ── Yardımcı setler ─────────────────────────
    regsho_tickers = {
        r.get("Symbol") or r.get("Ticker","")
        for r in rs if (r.get("Symbol") or r.get("Ticker"))
    }

    split_map = {}
    for row in sp:
        if not row.get("is_reverse"):
            continue
        t = row.get("Symbol") or row.get("symbol") or row.get("Ticker","")
        if t and t not in split_map and row.get("split_ratio"):
            split_map[t] = {"ratio": row["split_ratio"], "date": row.get("split_date","")}

    ce_map = {}
    for row in ce:
        t = row.get("symbol") or row.get("ticker") or row.get("Symbol","")
        if t:
            ce_map[t] = row

    avg_vol_map = {}
    for row in ce:
        t = row.get("symbol") or row.get("ticker") or row.get("Symbol","")
        v = to_float(row.get("10_day_avg_vol") or row.get("tenDayAvgVol"))
        if t and v:
            avg_vol_map[t] = v

    # ── EDGAR float ─────────────────────────────
    # EDGAR float: only CE screener tickers + split tickers (not all 645 RegSHO)
    # CE is already filtered (small float, high borrow) so it's the right universe
    float_tickers = list(set(ce_map.keys()) | set(split_map.keys()))
    if not float_tickers:
        float_tickers = list(regsho_tickers)[:100]  # fallback, max 100
    float_map     = fetch_edgar_floats(float_tickers, split_map)

    # askedgar SI — RegSHO + CE tickerları için
    # askedgar SI: CE tickers + RegSHO (capped at 200 to avoid timeout)
    si_tickers = list(set(ce_map.keys()) | regsho_tickers)[:200]
    askedgar_si = fetch_askedgar_si(si_tickers)

    # askedgar SI → float_map'e ekle
    for ticker, fd in float_map.items():
        fi       = askedgar_si.get(ticker, {})
        si_sh    = fi.get("short_interest")
        eff_fl   = fd.get("est_post_split_float") or fd.get("float_shares")
        warrant  = fd.get("warrant_shares") or 0
        diluted  = int(eff_fl + warrant) if eff_fl else None
        sf_pct   = round(si_sh / eff_fl * 100, 2) if (si_sh and eff_fl) else None
        avg_vol  = avg_vol_map.get(ticker)
        # DTC: askedgar'dan direkt geliyorsa onu kullan, yoksa hesapla
        dtc_api  = fi.get("days_to_cover")
        dtc_calc = round(si_sh / avg_vol, 2) if (si_sh and avg_vol and avg_vol>0) else None
        dtc      = dtc_api if dtc_api else dtc_calc
        fd.update({"finra_si": si_sh, "finra_si_date": fi.get("si_date",""),
                   "short_float_pct": sf_pct, "diluted_float": diluted,
                   "dtc": dtc, "effective_float": eff_fl,
                   "cash_per_share": fi.get("cash_per_share"),
                   "shares_outstanding": fi.get("shares_outstanding"),
                   "estimated_cash": fi.get("estimated_cash")})
    save("floats.json", list(float_map.values()), min_records=1)

    # ── S1 haritası ─────────────────────────────
    s1_map = {s["ticker"]: s for s in s1 if s.get("ticker")}

    # ── Birleşik summary ────────────────────────
    all_tickers = {t for t in (set(ce_map)|regsho_tickers|set(split_map)|set(float_map))
                   if t and re.match(r"^[A-Z]{1,5}$", t)}
    print(f"\n  Birleşik set: {len(all_tickers)} ticker (CE:{len(ce_map)} RS:{len(regsho_tickers)} SP:{len(split_map)} FI:{len(float_map)})")

    summary_map = {}
    for ticker in sorted(all_tickers):
        row = ce_map.get(ticker, {})
        fd  = float_map.get(ticker, {})
        s1r = s1_map.get(ticker, {})
        eff_float = fd.get("effective_float") or to_float(row.get("shares_float"))
        sf_pct    = fd.get("short_float_pct") or to_float(row.get("shortint_pct"))
        rec = {
            "ticker":               ticker,
            "c2b":                  to_float(row.get("borrow_fee_rate_ib")),
            "shares_avail":         to_float(row.get("borrow_fee_avail_ib")),
            "float":                (fd.get("est_post_split_float") or
                                     fd.get("float_shares") or
                                     to_float(row.get("shares_float"))),
            "diluted_float":        fd.get("diluted_float"),
            "warrant_shares":       fd.get("warrant_shares"),
            "float_is_presplit":    fd.get("float_is_presplit", False),
            "est_post_split_float": fd.get("est_post_split_float"),
            "float_date":           fd.get("float_date",""),
            "float_form":           fd.get("float_form",""),
            "short_float_pct":      sf_pct,
            "finra_si":             fd.get("finra_si"),
            "finra_si_date":        fd.get("finra_si_date",""),
            "si_change":            to_float(row.get("shortint_position_change_pct")),
            "short_vol_pct":        to_float(row.get("shortvol_all_short_pct")),
            "short_vol_pct_30d":    to_float(row.get("shortvol_all_short_pct_30d")),
            "short_vol":            to_float(row.get("10_day_avg_vol")),  # shortvol_all_short
            "shortint_db_pct":      to_float(row.get("shortint_db_pct")),
            "market_cap":           (to_float(row.get("market_cap")) or
                                     askedgar_si.get(ticker,{}).get("profile_mktcap")),
            "reg_volume":           to_float(row.get("reg_volume")),
            "dtc":                  (fd.get("dtc") or
                                     askedgar_si.get(ticker,{}).get("days_to_cover")),
            "cash_per_share":       (fd.get("cash_per_share") or
                                     askedgar_si.get(ticker,{}).get("cash_per_share")),
            "shares_outstanding":   fd.get("shares_outstanding"),
            "avg_vol_10d":          avg_vol_map.get(ticker),
            "price":                to_float(row.get("reg_price")),
            "change_pct":           to_float(row.get("reg_change_pct")),
            "pre_change":           to_float(row.get("pre_change_pct")),
            "reg_sho":              "✅" if ticker in regsho_tickers else "❌",
            "has_split":            "✅" if ticker in split_map else "-",
            "split_ratio":          split_map.get(ticker,{}).get("ratio"),
            "split_date":           (split_map.get(ticker,{}).get("date","") or
                                     fd.get("split_date","")),
            "s1_date":              s1r.get("filed_date",""),
            "offering_warning":     bool(s1r and s1r.get("form")=="S-1/A"),
        }
        sc, reasons = squeeze_score(rec)
        rec["squeeze_score"]   = sc
        rec["squeeze_reasons"] = ", ".join(reasons)
        summary_map[ticker]    = rec

    # S1 zenginleştir
    for s in s1:
        t  = s.get("ticker","")
        fd = float_map.get(t, {})
        s.update({"float":           fd.get("est_post_split_float") or fd.get("float_shares"),
                  "diluted_float":   fd.get("diluted_float"),
                  "float_date":      fd.get("float_date",""),
                  "short_float_pct": fd.get("short_float_pct"),
                  "reg_sho":         "✅" if t in regsho_tickers else "❌",
                  "in_summary":      t in summary_map})

    run_end = datetime.now(timezone.utc)
    elapsed = round((run_end - run_start).total_seconds())

    results = {
        "summary":  save("summary.json",         list(summary_map.values()), min_records=1),
        "regsho_t": save("regsho_tickers.json",  list(regsho_tickers),       min_records=1),
        "s1":       save("s1_edgar.json",         s1,                        min_records=1),
    }

    save_meta({
        "updated_at":      run_end.isoformat(),
        "elapsed_sec":     elapsed,
        "scraper_ok":      results["summary"],
        "protected_files": [k for k,v in results.items() if not v],
        "source_status":   SOURCE_STATUS,
        "counts": {
            "chartexchange":  len(ce),
            "regsho":         len(rs),
            "splits_reverse": sum(1 for x in sp if x.get("is_reverse")),
            "splits_upcoming":sum(1 for x in sp if x.get("is_reverse") and x.get("list_type")=="upcoming"),
            "insider":        len(ins),
            "s1_edgar":       len(s1),
            "floats":         len(float_map),
            "askedgar_si":    len(askedgar_si),
            "summary":        len(summary_map),
        },
    })

    print(f"\n{'='*60}")
    print(f"  {'✅' if results['summary'] else '⚠'} Tamamlandı ({elapsed}s)")
    for src, status in SOURCE_STATUS.items():
        icon = "✓" if status.startswith("ok") else "✗"
        print(f"  {icon}  {src}: {status}")
    print(f"{'='*60}\n")
