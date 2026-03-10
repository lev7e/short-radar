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
    """
    Returns ratio (old/new) ONLY for reverse splits (old > new).
    Reverse:  "1 for 10"  → ratio=10  (10 old shares → 1 new)
              "1:20"      → ratio=20
              "0.1"       → ratio=10  (decimal < 1 means reverse)
    Forward:  "2 for 1"   → None
    """
    if not s:
        return None
    raw = str(s).strip()
    # Plain decimal: "0.05" → 1/0.05=20 (reverse), "2.0" → forward
    try:
        v = float(raw)
        if v == 0:
            return None  # guard division by zero
        if 0 < v < 1:
            return round(1 / v, 4)  # e.g. 0.05 → 20:1 reverse
        return None  # >=1 → forward or par
    except ValueError:
        pass
    s = raw.lower().replace("–","-").replace("−","-")
    # Handle "1-for-10", "1 for 10", "1:10"
    m = re.search(r"([\d.]+)\s*(?:[-]?for[-]?|:)\s*([\d.]+)", s)
    if not m:
        return None
    new_, old_ = float(m.group(1)), float(m.group(2))
    if new_ == 0 or old_ == 0:
        return None
    # Reverse = new < old (fewer shares after)
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
        "shortvol_all_short_pct":       pick(
                                             "ShortVolume(SV) %",   # actual CE col
                                             "ShortVolume(SV) %",
                                             "ShortVolume%", "Short Vol %",
                                             "shortvol_all_short_pct"),
        "shortvol_all_short_pct_30d":   pick("ShortVolume%30D", "30D Short %",
                                             "shortvol_all_short_pct_30d"),
        "shortvol_all_short":           pick(
                                             "ShortVolume(SV)",         # actual CE col
                                             "ShortVolumeAll", "ShortVolume",
                                             "shortvol_all_short"),
        "pre_price":                    pick("PreMarketPrice", "Pre Price", "pre_price",
                                             "PreMarket Price"),
        "pre_change_pct":               pick("PreMarketChange%", "PreMarket Change%",
                                             "Pre Chg %", "pre_change_pct"),
        "market_cap":                   pick("MarketCap", "Market Cap", "market_cap"),
        "shortint_db_pct":              pick("ShortInterest%DB", "SI%DB", "shortint_db_pct",
                                             "ShortIntDB%", "ShortInterestDB%"),
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
                    if rows and isinstance(rows, list) and isinstance(rows[0], dict):
                        print(f"    CE JSON keys (row[0]): {list(rows[0].keys())}")
                        # Find SI-related keys
                        si_keys = [k for k in rows[0].keys() if any(x in k.lower() for x in ("short","si","interest","chg","change"))]
                        print(f"    CE SI-related keys: {si_keys}")
                        if si_keys:
                            print(f"    CE SI sample values: { {k: rows[0][k] for k in si_keys} }")
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
                            if rows:
                                raw0 = dict_rows[0] if dict_rows else {}
                                print(f"    CE raw row keys: {list(raw0.keys())[:12]}")
                                print(f"    CE raw row vals: {list(raw0.values())[:12]}")
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
        sample = all_rows[0]
        # Log all CE fields to diagnose missing values
        sv  = sample.get("shortvol_all_short_pct")
        sv3 = sample.get("shortvol_all_short_pct_30d")
        sic = sample.get("shortint_position_change_pct")
        pre = sample.get("pre_change_pct")
        print(f"    CE sample: ticker={sample.get('ticker')} c2b={sample.get('borrow_fee_rate_ib')} "
              f"float={sample.get('shares_float')} sv%={sv} sv30d%={sv3} si_chg%={sic} pre%={pre}")
        # Show raw row to see what CE actually sends
        raw_keys = [k for k in sample.keys() if not k.startswith("_")]
        print(f"    CE fields: {raw_keys}")

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
    """
    Reverse split verileri — 3 kaynak:
    1. TipRanks upcoming  (https://www.tipranks.com/calendars/stock-splits/upcoming)
    2. TipRanks historical (https://www.tipranks.com/calendars/stock-splits/historical)
    3. StockAnalysis       (https://stockanalysis.com/actions/splits/)
    Yahoo kaldırıldı — güncel veri sağlamıyor.
    """
    print("\n[3/6] Splits (TipRanks + StockAnalysis)...")
    rows   = []
    seen   = set()

    def add_row(sym, ratio_str, raw_date, is_upcoming, source):
        if not sym: return
        sym = re.sub(r"[^A-Z]", "", sym.upper())[:6]
        if not sym or sym in seen: return
        ratio    = parse_split_ratio(ratio_str)
        is_rev   = ratio is not None
        if not is_rev and ratio_str:
            rm = re.search(r"(\d+)\s*(?:[-:/]|for|to)\s*(\d+)", str(ratio_str).lower())
            if rm:
                n1, n2 = float(rm.group(1)), float(rm.group(2))
                if n2 > n1: is_rev = True; ratio = round(n2/n1, 4)
        if not is_rev:
            return  # skip forward splits
        norm_date = normalize_date(str(raw_date))
        seen.add(sym)
        rows.append({
            "Symbol": sym, "Ratio": ratio_str,
            "Date": norm_date, "split_date": norm_date,
            "is_reverse": True, "split_ratio": ratio,
            "list_type": "upcoming" if is_upcoming else "recent",
            "source": source,
        })

    # ────────────────────────────────────────────────────────────────
    # SOURCE 1 & 2: TipRanks
    # ────────────────────────────────────────────────────────────────
    TR_HDR = {
        **BROWSER_HEADERS,
        "Accept":          "text/html,application/xhtml+xml,*/*;q=0.9",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer":         "https://www.tipranks.com/",
        "sec-fetch-site":  "same-origin",
        "sec-fetch-mode":  "navigate",
    }
    for tr_url, is_up in [
        ("https://www.tipranks.com/calendars/stock-splits/upcoming",   True),
        ("https://www.tipranks.com/calendars/stock-splits/historical", False),
    ]:
        label = "upcoming" if is_up else "historical"
        try:
            r = requests.get(tr_url, headers=TR_HDR, timeout=25)
            print(f"    TipRanks {label}: HTTP {r.status_code}, {len(r.content)}b")
            if r.status_code != 200: continue

            added = 0
            # Strategy A: __NEXT_DATA__ JSON
            m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, re.S)
            if m:
                try:
                    nd = json.loads(m.group(1))
                    # Recursively find any list with ticker-like keys
                    def scan(obj, depth=0):
                        if depth > 10: return []
                        if isinstance(obj, list) and len(obj) > 0 and isinstance(obj[0], dict):
                            k0 = {kk.lower() for kk in obj[0].keys()}
                            if any(x in k0 for x in ("ticker","symbol","stockticker","stock")):
                                return [obj]
                        res = []
                        if isinstance(obj, dict):
                            for v in obj.values(): res.extend(scan(v, depth+1))
                        elif isinstance(obj, list):
                            for v in obj: res.extend(scan(v, depth+1))
                        return res
                    arrays = scan(nd)
                    for arr in arrays:
                        for item in arr:
                            sym = (item.get("ticker") or item.get("symbol") or
                                   item.get("stockTicker") or item.get("Ticker") or "")
                            # ratio may be stored as "1:10" or {"from":1,"to":10}
                            ratio_raw = (item.get("ratio") or item.get("splitRatio") or
                                         item.get("Ratio") or item.get("split_ratio") or "")
                            if isinstance(ratio_raw, dict):
                                fr = ratio_raw.get("from",1); to = ratio_raw.get("to",1)
                                ratio_raw = f"{fr}:{to}"
                            date_raw = (item.get("date") or item.get("splitDate") or
                                        item.get("exDate") or item.get("ex_date") or "")
                            add_row(sym, str(ratio_raw), str(date_raw), is_up, f"tipranks_{label}")
                            added += 1
                except json.JSONDecodeError:
                    pass

            # Strategy B: window.__INITIAL_STATE__ or similar embedded JSON
            if not added:
                for pat in [
                    r'window\.__(?:INITIAL_STATE|APP_STATE|DATA)__\s*=\s*({.*?});</script>',
                    r'window\.calendarData\s*=\s*(\[.*?\]);</script>',
                ]:
                    jm = re.search(pat, r.text, re.S)
                    if jm:
                        try:
                            jd = json.loads(jm.group(1))
                            if isinstance(jd, list):
                                for item in jd:
                                    sym = item.get("ticker") or item.get("symbol","")
                                    ratio_raw = item.get("ratio") or item.get("splitRatio","")
                                    date_raw  = item.get("date") or item.get("exDate","")
                                    add_row(sym, str(ratio_raw), str(date_raw), is_up, f"tipranks_{label}")
                                    added += 1
                        except Exception: pass

            # Strategy C: HTML table
            if not added:
                soup = BeautifulSoup(r.text, "html.parser")
                for tbl in soup.find_all("table"):
                    ths  = [th.get_text(strip=True) for th in tbl.find_all("th")]
                    if not ths:
                        fr = tbl.find("tr")
                        if fr: ths = [td.get_text(strip=True) for td in fr.find_all("td")]
                    ths_lower = [h.lower() for h in ths]
                    ticker_i = next((i for i,h in enumerate(ths_lower) if "ticker" in h or "symbol" in h), None)
                    ratio_i  = next((i for i,h in enumerate(ths_lower) if "ratio" in h), None)
                    date_i   = next((i for i,h in enumerate(ths_lower) if "date" in h or "ex" in h), None)
                    if ticker_i is None: continue
                    for tr in tbl.find_all("tr")[1:]:
                        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                        if len(cells) <= ticker_i: continue
                        sym       = cells[ticker_i]
                        ratio_raw = cells[ratio_i]  if ratio_i and len(cells) > ratio_i else ""
                        date_raw  = cells[date_i]   if date_i  and len(cells) > date_i  else ""
                        add_row(sym, ratio_raw, date_raw, is_up, f"tipranks_{label}")
                        added += 1

            # Strategy D: data attributes / JSON in divs
            if not added:
                soup = BeautifulSoup(r.text, "html.parser")
                for tag in soup.find_all(attrs={"data-testid": True}):
                    dj = tag.get("data-props") or tag.get("data-initial") or ""
                    if dj:
                        try:
                            obj = json.loads(dj)
                            if isinstance(obj, list):
                                for item in obj:
                                    sym = item.get("ticker","")
                                    add_row(sym, item.get("ratio",""), item.get("date",""), is_up, f"tipranks_{label}")
                                    added += 1
                        except Exception: pass

            print(f"    TipRanks {label}: {added} reverse split eklendi")
        except Exception as e:
            print(f"    TipRanks {label} hata: {e}")
        time.sleep(0.8)

    # ────────────────────────────────────────────────────────────────
    # SOURCE 3: StockAnalysis
    # ────────────────────────────────────────────────────────────────
    for sa_url, is_up in [
        ("https://stockanalysis.com/actions/reverse-splits/", False),
        ("https://stockanalysis.com/actions/splits/",         False),  # includes reverse
    ]:
        try:
            r = requests.get(sa_url, headers={**BROWSER_HEADERS, "Accept": "text/html,*/*"}, timeout=25)
            print(f"    SA {sa_url}: HTTP {r.status_code}, {len(r.content)}b")
            if r.status_code != 200: continue

            added = 0
            # Strategy A: __NEXT_DATA__
            m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, re.S)
            if m:
                try:
                    nd = json.loads(m.group(1))
                    # SA typically stores table data in props.pageProps.data or similar
                    def find_sa_data(obj, depth=0):
                        if depth > 8: return []
                        if isinstance(obj, list) and len(obj) > 0 and isinstance(obj[0], dict):
                            k0 = {kk.lower() for kk in obj[0].keys()}
                            if any(x in k0 for x in ("ticker","symbol","s")):
                                return [obj]
                        res = []
                        if isinstance(obj, dict):
                            for v in obj.values(): res.extend(find_sa_data(v, depth+1))
                        return res
                    for arr in find_sa_data(nd):
                        for item in arr:
                            sym = (item.get("s") or item.get("ticker") or
                                   item.get("symbol") or item.get("Symbol",""))
                            ratio_raw = (item.get("ratio") or item.get("splitRatio") or
                                         item.get("r") or item.get("Ratio",""))
                            date_raw  = (item.get("date") or item.get("exDate") or
                                         item.get("d") or item.get("Date",""))
                            add_row(sym, str(ratio_raw), str(date_raw), is_up, "stockanalysis")
                            added += 1
                except json.JSONDecodeError: pass

            # Strategy B: HTML table
            if not added:
                soup = BeautifulSoup(r.text, "html.parser")
                for tbl in soup.find_all("table"):
                    ths = [th.get_text(strip=True) for th in tbl.find_all("th")]
                    if not ths:
                        fr = tbl.find("tr")
                        if fr: ths = [td.get_text(strip=True) for td in fr.find_all("td")]
                    ths_l = [h.lower() for h in ths]
                    t_i = next((i for i,h in enumerate(ths_l) if h in ("symbol","ticker","stock")), None)
                    r_i = next((i for i,h in enumerate(ths_l) if "ratio" in h), None)
                    d_i = next((i for i,h in enumerate(ths_l) if "date" in h or "ex" in h), None)
                    if t_i is None: continue
                    for tr in tbl.find_all("tr")[1:]:
                        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                        if len(cells) <= t_i: continue
                        sym       = cells[t_i]
                        ratio_raw = cells[r_i] if r_i and len(cells) > r_i else ""
                        date_raw  = cells[d_i] if d_i and len(cells) > d_i else ""
                        add_row(sym, ratio_raw, date_raw, is_up, "stockanalysis")
                        added += 1
                    if added: break

            # Strategy C: SA sometimes embeds data as window.sa_data or JSON blob
            if not added:
                for pat in [
                    r'sa\.data\s*=\s*(\[.*?\]);',
                    r'"data":\s*(\[\{"[st]',
                ]:
                    jm = re.search(pat, r.text, re.S)
                    if jm:
                        try:
                            arr = json.loads(jm.group(1) + ("]" if not jm.group(1).endswith("]") else ""))
                            for item in arr:
                                sym = item.get("ticker") or item.get("symbol","")
                                add_row(sym, item.get("ratio",""), item.get("date",""), is_up, "stockanalysis")
                                added += 1
                        except Exception: pass

            print(f"    SA: {added} reverse split eklendi (toplam seen: {len(seen)})")
            if added > 5: break  # reverse-splits page sufficient, skip /splits/
        except Exception as e:
            print(f"    SA hata: {e}")
        time.sleep(0.5)

    rev_count  = sum(1 for r in rows if r["is_reverse"])
    up_count   = sum(1 for r in rows if r.get("list_type") == "upcoming")
    print(f"    Splits toplam: {len(rows)} ({rev_count} reverse, {up_count} upcoming)")

    SOURCE_STATUS["splits"] = f"ok:{rev_count}" if rows else "error:no_data"
    save("splits.json", rows, min_records=1)
    return rows


def fetch_insider():
    """
    SEC EDGAR Form 4 — insider alım/satım bildirimleri.
    Finviz yerine direkt EDGAR EFTS kullanıyoruz.
    Son 30 gün, sadece BUY işlemleri (P = Purchase).
    """
    print("\n[4/7] SEC EDGAR Form 4 insider işlemleri...")
    rows, seen = [], set()
    start = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    end   = datetime.now().strftime("%Y-%m-%d")

    try:
        r = requests.get(
            "https://efts.sec.gov/LATEST/search-index",
            params={"forms": "4", "dateRange": "custom",
                    "startdt": start, "enddt": end},
            headers=SEC_HEADERS, timeout=30,
        )
        print(f"    Form 4: HTTP {r.status_code}")
        if r.status_code != 200:
            SOURCE_STATUS["insider"] = f"error:http{r.status_code}"
            save("insider.json", [], min_records=1)
            return []

        data = r.json()
        hits = data.get("hits", {}).get("hits", [])
        print(f"    Form 4: {len(hits)} hit")

        for hit in hits:
            s       = hit.get("_source", {})
            display = s.get("display_names") or []
            entity, ticker = "", ""

            if isinstance(display, list) and display:
                first = display[0]
                if isinstance(first, dict):
                    entity = first.get("name", "").strip()
                    ticker = first.get("ticker", "").strip()
                else:
                    raw    = str(first)
                    entity = re.sub(r"\s*\(CIK[^)]*\)", "", raw).strip()
            elif isinstance(display, str) and display:
                entity = re.sub(r"\s*\(CIK[^)]*\)", "", display).strip()

            if not ticker and entity:
                tm = re.search(r"\(([A-Z]{1,6})\)\s*$", entity)
                if tm:
                    ticker = tm.group(1)
                    entity = entity[:tm.start()].strip()

            if not ticker:
                t2 = s.get("tickers") or s.get("ticker") or ""
                ticker = (t2[0] if isinstance(t2, list) and t2 else str(t2)).strip()

            if not ticker:
                continue  # Form 4 without ticker not useful

            # Filer = the insider (person filing, display_names[1] if present)
            person = ""
            if isinstance(display, list) and len(display) > 1:
                f1 = display[1]
                person = f1.get("name","").strip() if isinstance(f1,dict) else re.sub(r"\s*\(CIK[^)]*\)","",str(f1)).strip()
                person = re.sub(r"\s*\([A-Z]{1,6}\)\s*$","",person).strip()

            filed = s.get("file_date") or s.get("filed_at") or ""
            uid   = f"{ticker}|{person}|{filed[:10]}"
            if uid in seen:
                continue
            if not entity and not ticker:
                continue
            seen.add(uid)

            rows.append({
                "ticker":     ticker,
                "company":    entity,
                "person":     person,
                "filed_date": filed,
                "form":       "4",
                "transaction": "Buy",  # EFTS only returns buy-signal filings typically
                "edgar_url":  f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={s.get('entity_id','')}&type=4&dateb=&owner=include&count=10",
            })

        print(f"    Form 4 toplam: {len(rows)} insider")
    except Exception as e:
        print(f"    Form 4 hatası: {e}")

    SOURCE_STATUS["insider"] = f"ok:{len(rows)}"
    save("insider.json", rows, min_records=1)
    return rows


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
                        # OR:            "ARVANA INC (AVNI)"  ← ticker in parens
                        raw = str(first)
                        entity = re.sub(r"\s*\(CIK[^)]*\)", "", raw).strip()
                elif isinstance(display, str) and display:
                    entity = re.sub(r"\s*\(CIK[^)]*\)", "", display).strip()

                # Extract ticker from "(TICK)" pattern in entity name
                # CIK already stripped, so "(AVAX)" at end → ticker
                # e.g. "ARVANA INC (AVNI)" or "Grayscale Trust (AVAX)"
                if not ticker and entity:
                    # Try last parens group that looks like a ticker
                    tm = re.search(r"\(([A-Z]{1,6})\)\s*$", entity)
                    if tm:
                        ticker = tm.group(1)
                        entity = entity[:tm.start()].strip()
                    # Also try: "NAME (TICK)  " with trailing spaces
                    elif re.search(r"\([A-Z]{1,6}\)", entity):
                        tm2 = re.findall(r"\(([A-Z]{1,6})\)", entity)
                        if tm2:
                            ticker = tm2[-1]  # take last ticker-like paren
                            entity = re.sub(r"\s*\([A-Z]{1,6}\)\s*$", "", entity).strip()

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



def fetch_sec_13g():
    """
    SC 13G / SC 13G/A / SC 13D / SC 13D/A — institutional >5% ownership filings.
    Piyasaya etkisi büyük: >5% hisse alanlar bildirmek zorunda.
    13G: pasif yatırımcı, 13D: aktif (yönetim değişikliği niyeti).
    Son 60 günü çekeriz — güncel ve tarihsel hareket ikisi de görünsün.
    """
    print("\n[7/7] SEC EDGAR 13G/13D başvuruları çekiliyor...")
    rows, seen = [], set()
    start = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
    end   = datetime.now().strftime("%Y-%m-%d")

    for form_type in ["SC 13G", "SC 13G/A", "SC 13D", "SC 13D/A"]:
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
            data = r.json()
            hits = data.get("hits", {}).get("hits", [])
            print(f"    {form_type}: {len(hits)} hit")
            for hit in hits:
                s       = hit.get("_source", {})
                display = s.get("display_names") or []
                entity, ticker = "", ""

                if isinstance(display, list) and display:
                    first = display[0]
                    if isinstance(first, dict):
                        entity = first.get("name", "").strip()
                        ticker = first.get("ticker", "").strip()
                    else:
                        raw    = str(first)
                        entity = re.sub(r"\s*\(CIK[^)]*\)", "", raw).strip()
                elif isinstance(display, str) and display:
                    entity = re.sub(r"\s*\(CIK[^)]*\)", "", display).strip()

                # Extract ticker from "(TICK)" in entity name
                if not ticker and entity:
                    tm = re.search(r"\(([A-Z]{1,6})\)\s*$", entity)
                    if tm:
                        ticker = tm.group(1)
                        entity = entity[:tm.start()].strip()
                    elif re.search(r"\([A-Z]{1,6}\)", entity):
                        tm2 = re.findall(r"\(([A-Z]{1,6})\)", entity)
                        if tm2:
                            ticker = tm2[-1]
                            entity = re.sub(r"\s*\([A-Z]{1,6}\)\s*$", "", entity).strip()

                if not entity:
                    entity = (s.get("entity_name") or s.get("company_name") or "").strip()
                if not ticker:
                    t2 = s.get("tickers") or s.get("ticker") or ""
                    ticker = (t2[0] if isinstance(t2, list) and t2 else str(t2)).strip()

                # 13G/D have TWO parties: the filer (institution) + the issuer (company)
                # display_names[0] is the issuer (target company)
                # display_names[1] might be the filer/institution
                filer = ""
                if isinstance(display, list) and len(display) > 1:
                    f1 = display[1]
                    if isinstance(f1, dict):
                        filer = f1.get("name", "").strip()
                    else:
                        filer = re.sub(r"\s*\(CIK[^)]*\)", "", str(f1)).strip()
                        filer = re.sub(r"\s*\([A-Z]{1,6}\)\s*$", "", filer).strip()

                # Pct owned may be in the filing text — not in EFTS, but note form type
                # 13D = activist (>10% likely), 13G = passive (5-10%)
                activist = "13D" in form_type

                filed = s.get("file_date") or s.get("filed_at") or ""
                uid   = f"{ticker or entity}|{filer}|{filed[:10]}"
                if uid in seen:
                    continue
                if not entity and not ticker:
                    continue
                seen.add(uid)

                edgar_url = (
                    f"https://www.sec.gov/cgi-bin/browse-edgar"
                    f"?action=getcompany&company={requests.utils.quote(entity or ticker)}"
                    f"&type={requests.utils.quote(form_type)}&dateb=&owner=include&count=10"
                )
                rows.append({
                    "form":      form_type,
                    "ticker":    ticker,
                    "company":   entity,
                    "filer":     filer,      # institution that filed
                    "filed_date": filed,
                    "activist":  activist,
                    "edgar_url": edgar_url,
                })
            time.sleep(0.3)
        except Exception as e:
            print(f"    {form_type} hatası: {e}")

    print(f"    Toplam: {len(rows)} 13G/13D başvuru")
    SOURCE_STATUS["13g"] = f"ok:{len(rows)}"
    save("13g_edgar.json", rows, min_records=1)
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

            # 2. Estimated cash from SEC
            r2 = requests.get(f"{API}/v1/sec/{ticker}/estimated-cash",
                              headers=HDR, timeout=8)
            if r2.status_code == 200:
                d2 = r2.json()
                rec["estimated_cash"] = d2.get("estimated_cash")
                rec["cash_per_share"] = d2.get("cash_per_share")
            time.sleep(0.1)

            # 3. Company profile (float = sharesOutstanding, price)
            r3 = requests.get(f"{API}/v1/fmp/company/{ticker}/profile",
                              headers=HDR, timeout=8)
            if r3.status_code == 200:
                d3 = r3.json()
                # Log ALL profile keys on first ticker to discover available fields
                if ok <= 1:
                    print(f"    Profile ALL keys ({ticker}): {list(d3.keys())}")
                    print(f"    Profile ALL vals ({ticker}): {dict(list(d3.items())[:20])}")

                rec["profile_price"]      = d3.get("price")
                rec["profile_mktcap"]     = d3.get("mktCap")
                rec["shares_outstanding"] = d3.get("sharesOutstanding")
                rec["vol_avg"]            = d3.get("volAvg")
                rec["country"]            = d3.get("country")
                rec["inst_own_pct"]       = (d3.get("institutionalOwnershipPercentage") or
                                              d3.get("institutionalHoldersPercentage") or
                                              d3.get("instOwn") or
                                              d3.get("institutionalOwnership"))
                rec["enterprise_value"]   = d3.get("enterpriseValue") or d3.get("enterpriseValueTTM")
                # FMP profile may have explicit float (distinct from OS)
                rec["profile_float"] = (
                    d3.get("floatShares") or
                    d3.get("float") or
                    d3.get("sharesFloat") or
                    None
                )
                # fallback: derive float from mktCap/price if no explicit field
                if not rec["profile_float"]:
                    mkt2 = d3.get("mktCap"); prc2 = d3.get("price")
                    if mkt2 and prc2 and float(prc2) > 0:
                        rec["profile_float"] = round(float(mkt2) / float(prc2))
                # cash & cashPerShare may come directly from profile
                # FMP profile has these cash fields
                cps = (d3.get("cashPerShare") or d3.get("cashPerShareTTM") or
                       d3.get("cash_per_share"))
                # Compute from total cash / shares if direct field missing
                if not cps:
                    total_cash = (d3.get("totalCash") or d3.get("cash") or
                                  d3.get("cashAndCashEquivalents") or
                                  rec.get("estimated_cash"))
                    shr = d3.get("sharesOutstanding") or d3.get("shares")
                    if total_cash and shr and float(shr) > 0:
                        cps = round(float(total_cash) / float(shr), 4)
                if not cps and rec.get("estimated_cash"):
                    shr = d3.get("sharesOutstanding")
                    if shr and float(shr) > 0:
                        cps = round(rec["estimated_cash"] / float(shr), 4)
                # Derive shares from mktCap/price if sharesOutstanding missing
                if not cps and rec.get("estimated_cash"):
                    mkt  = d3.get("mktCap")
                    prc  = d3.get("price")
                    if mkt and prc and float(prc) > 0:
                        implied_shares = float(mkt) / float(prc)
                        if implied_shares > 0:
                            cps = round(rec["estimated_cash"] / implied_shares, 4)
                if cps:
                    rec["cash_per_share"] = cps
                # Debug first ticker
                if ok <= 1:
                    mkt = d3.get("mktCap"); prc = d3.get("price")
                    impl_shr = round(float(mkt)/float(prc),0) if mkt and prc and float(prc)>0 else None
                    print(f"    Askedgar {ticker}: cash={rec.get('estimated_cash')} cps={cps} mktCap={mkt} price={prc} implied_shares={impl_shr}")
            # 4. Try institutional ownership endpoint
            try:
                r4 = requests.get(f"{API}/v1/fmp/company/{ticker}/institutional-ownership",
                                  headers=HDR, timeout=8)
                if r4.status_code == 200:
                    d4 = r4.json()
                    if ok <= 1:
                        print(f"    InstOwn ({ticker}) keys: {list(d4.keys()) if isinstance(d4,dict) else type(d4).__name__}")
                    if isinstance(d4, dict):
                        rec["inst_own_pct"] = rec.get("inst_own_pct") or (
                            d4.get("institutionalOwnershipPercentage") or
                            d4.get("percentage") or d4.get("pct")
                        )
                    elif isinstance(d4, list) and d4:
                        first = d4[0]
                        if ok <= 1:
                            print(f"    InstOwn list[0] keys: {list(first.keys())[:8]}")
                        rec["inst_own_pct"] = rec.get("inst_own_pct") or first.get("ownershipPercentage")
            except Exception:
                pass
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
def fetch_ftd():
    """
    SEC Fail-to-Deliver verisi — RegSHO'dan 5-10 gün önce sinyal verir.
    URL: https://www.sec.gov/data/foiadocuments/docs/fails.YYYYMMDD.zip
    İki haftayı dene (son iş günü zip mevcut olmayabilir).
    Her ticker için toplam FTD adet ve son tarih döner.
    """
    print("\n[FTD] SEC Fail-to-Deliver çekiliyor...")
    from zipfile import ZipFile
    from io import BytesIO
    from datetime import date as _date

    result = {}  # ticker → {ftd_shares, ftd_date, ftd_value}
    today  = _date.today()

    # Son 10 iş günü dene
    tried = 0
    for offset in range(1, 15):
        d = today - timedelta(days=offset)
        if d.weekday() >= 5:  # Sat/Sun
            continue
        ds = d.strftime("%Y%m%d")
        url = f"https://www.sec.gov/data/foiadocuments/docs/fails.{ds}.zip"
        try:
            r = requests.get(url, headers=SEC_HEADERS, timeout=20)
            print(f"    FTD {ds}: HTTP {r.status_code}, {len(r.content)}b")
            if r.status_code != 200:
                tried += 1
                if tried >= 3:
                    break
                continue

            # Parse pipe-delimited txt inside zip
            with ZipFile(BytesIO(r.content)) as zf:
                name = zf.namelist()[0]
                with zf.open(name) as f_in:
                    lines = f_in.read().decode("utf-8", errors="replace").splitlines()

            # Header: SETTLEMENT DATE|CUSIP|SYMBOL|QUANTITY (FAILS)|DESCRIPTION|PRICE
            hdrs = [h.strip().upper() for h in lines[0].split("|")]
            sym_i = next((i for i,h in enumerate(hdrs) if "SYMBOL" in h), 2)
            qty_i = next((i for i,h in enumerate(hdrs) if "QUANTITY" in h or "FAIL" in h), 3)
            prc_i = next((i for i,h in enumerate(hdrs) if "PRICE" in h), 5)
            date_i= next((i for i,h in enumerate(hdrs) if "DATE" in h or "SETTLEMENT" in h), 0)

            count = 0
            for line in lines[1:]:
                parts = line.split("|")
                if len(parts) <= max(sym_i, qty_i):
                    continue
                sym = parts[sym_i].strip().upper()
                if not sym or not re.match(r"^[A-Z]{1,5}$", sym):
                    continue
                qty = to_float(parts[qty_i])
                prc = to_float(parts[prc_i]) if len(parts) > prc_i else None
                dt  = parts[date_i].strip() if len(parts) > date_i else ""
                if qty and qty > 0:
                    existing = result.get(sym, {})
                    # Accumulate FTDs across days (up to 10 business days)
                    existing["ftd_shares"] = (existing.get("ftd_shares") or 0) + qty
                    existing["ftd_date"]   = dt or existing.get("ftd_date","")
                    if prc and qty:
                        existing["ftd_value"] = (existing.get("ftd_value") or 0) + (prc * qty)
                    result[sym] = existing
                    count += 1

            print(f"    FTD {ds}: {count} ticker FTD kaydı")
            # Get 2 most recent dates then stop
            if len(result) > 0 and tried < 2:
                tried += 1
                if tried >= 2:
                    break
        except Exception as e:
            print(f"    FTD {ds} hata: {e}")
            tried += 1
            if tried >= 3:
                break

    print(f"    FTD toplam: {len(result)} ticker")
    SOURCE_STATUS["ftd"] = f"ok:{len(result)}" if result else "error:no_data"
    # Save as list for JSON
    ftd_list = [{"ticker": t, **v} for t, v in result.items()]
    save("ftd.json", ftd_list, min_records=1)
    return result  # return dict for fast lookup


def update_regsho_history(current_tickers: set) -> dict:
    """
    regsho_history.json: {ticker: {first_date, last_date, days_count}}
    Her run'da güncellenir — RegSHO'da kaç gündür olduğunu takip eder.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    hist  = load_existing("regsho_history.json") or {}

    # Update existing entries
    for t in list(hist.keys()):
        if t in current_tickers:
            # Still on list — increment
            hist[t]["last_date"]  = today
            hist[t]["days_count"] = hist[t].get("days_count", 1) + 1
        else:
            # Dropped off — keep record but mark inactive
            hist[t]["active"] = False

    # Add new entries
    for t in current_tickers:
        if t not in hist:
            hist[t] = {"first_date": today, "last_date": today,
                       "days_count": 1, "active": True}
        else:
            hist[t]["active"] = True

    path = os.path.join(OUTPUT_DIR, "regsho_history.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(hist, f, indent=2)
    print(f"    RegSHO history: {len([v for v in hist.values() if v.get('active')])} aktif, {len(hist)} toplam")
    return hist


def _best_float(fd, ce_float):
    """Pick best float:
    1. askedgar live profile (FMP, most current)
    2. post-split estimate
    3. fresh EDGAR XBRL (<=180 days)
    4. CE screener float (current day)
    5. stale EDGAR (last resort)
    """
    from datetime import date as _date
    # 1. Askedgar profile float (live FMP data)
    if fd.get("askedgar_float"):
        return fd["askedgar_float"]
    # 2. Post-split estimate
    if fd.get("est_post_split_float"):
        return fd["est_post_split_float"]
    # 3. Fresh EDGAR XBRL
    edgar_val  = fd.get("float_shares")
    edgar_date = (fd.get("float_date") or "")[:10]
    if edgar_val and edgar_date:
        try:
            age = (_date.today() - _date.fromisoformat(edgar_date)).days
            if age <= 180:
                return edgar_val
        except Exception:
            pass
    # 4. CE screener float (always current market day)
    if ce_float:
        return ce_float
    # 5. Stale EDGAR
    return edgar_val


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
    # RegSHO gün sayısı — uzun süre listede = ciddi baskı
    rs_days = s.get("regsho_days") or 0
    if rs_days >= 10: score+=12; reasons.append(f"RS{rs_days}g")
    elif rs_days >= 5: score+=7; reasons.append(f"RS{rs_days}g")
    elif rs_days >= 3: score+=3
    # FTD — borsa teslimat başarısızlığı
    ftd = to_float(s.get("ftd_shares"))
    if ftd is not None:
        fl2 = to_float(s.get("diluted_float") or s.get("float")) or 0
        if   ftd > 500_000: score+=12; reasons.append("FTD>500K")
        elif ftd > 100_000: score+=8;  reasons.append("FTD>100K")
        elif ftd >  10_000: score+=4;  reasons.append("FTD>10K")
        # FTD/float oranı daha anlamlı
        if fl2 > 0 and ftd/fl2 > 0.05:
            score+=5; reasons.append(f"FTD/Float>{int(ftd/fl2*100)}%")
    # C2B günlük delta — ani artış erken sinyal
    c2b_d = to_float(s.get("c2b_delta"))
    if c2b_d is not None:
        if   c2b_d >= 100: score+=12; reasons.append("C2B▲100%")
        elif c2b_d >=  50: score+=8;  reasons.append("C2B▲50%")
        elif c2b_d >=  20: score+=4;  reasons.append("C2B▲20%")
    # Avail delta — borçlanılabilir hisse kuruyor
    av_d = s.get("avail_delta")
    if av_d is not None:
        avail = to_float(s.get("shares_avail")) or 0
        if av_d < -50_000: score+=8; reasons.append("Avail↓↓")
        elif av_d < -5_000: score+=4; reasons.append("Avail↓")
        if avail < 1000 and av_d < 0: score+=5; reasons.append("Avail≈0")
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
    g13      = fetch_sec_13g()
    ftd_map  = fetch_ftd()
    finra_si = {}   # artık kullanılmıyor

    # ── Yardımcı setler ─────────────────────────
    regsho_tickers = {
        r.get("Symbol") or r.get("Ticker","")
        for r in rs if (r.get("Symbol") or r.get("Ticker"))
    }
    regsho_hist = update_regsho_history(regsho_tickers)

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

    # ── Yesterday snapshot for delta calculation ────────
    prev_ce = {}
    prev_raw = load_existing("chartexchange_prev.json") or []
    for row in prev_raw:
        t = row.get("symbol") or row.get("ticker") or row.get("Symbol","")
        if t:
            prev_ce[t] = row
    # Save today's CE as tomorrow's "prev"
    import shutil
    ce_src = os.path.join(OUTPUT_DIR, "chartexchange.json")
    ce_dst = os.path.join(OUTPUT_DIR, "chartexchange_prev.json")
    if os.path.exists(ce_src):
        shutil.copy2(ce_src, ce_dst)

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
        # askedgar profile_float is often more current than EDGAR XBRL
        askedgar_float = fi.get("profile_float")
        fd.update({"finra_si": si_sh, "finra_si_date": fi.get("si_date",""),
                   "short_float_pct": sf_pct, "diluted_float": diluted,
                   "dtc": dtc, "effective_float": eff_fl,
                   "cash_per_share": fi.get("cash_per_share"),
                   "shares_outstanding": fi.get("shares_outstanding"),
                   "estimated_cash": fi.get("estimated_cash"),
                   "askedgar_float": askedgar_float,
                   "askedgar_float_date": "live",
                   "vol_avg": fi.get("vol_avg"),
                   "country": fi.get("country"),
                   "inst_own_pct": fi.get("inst_own_pct"),
                   "enterprise_value": fi.get("enterprise_value"),
                   "borrow_rate": fi.get("borrow_rate")})
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
        prev_row   = prev_ce.get(ticker, {})
        prev_c2b   = to_float(prev_row.get("borrow_fee_rate_ib"))
        prev_avail = to_float(prev_row.get("borrow_fee_avail_ib"))
        cur_c2b    = to_float(row.get("borrow_fee_rate_ib"))
        cur_avail  = to_float(row.get("borrow_fee_avail_ib"))
        c2b_delta  = round(cur_c2b - prev_c2b, 2) if (cur_c2b and prev_c2b) else None
        avail_delta= int(cur_avail - prev_avail) if (cur_avail is not None and prev_avail is not None) else None

        ftd_info   = ftd_map.get(ticker, {})
        rs_hist    = regsho_hist.get(ticker, {})
        rs_days    = rs_hist.get("days_count", 0) if rs_hist.get("active") else 0

        rec = {
            "ticker":               ticker,
            "c2b":                  cur_c2b,
            "c2b_delta":            c2b_delta,      # vs yesterday
            "shares_avail":         cur_avail,
            "avail_delta":          avail_delta,    # vs yesterday
            # Float priority: est_post_split > fresh EDGAR (<=180d) > CE > stale EDGAR
            "ftd_shares":           ftd_info.get("ftd_shares"),
            "ftd_date":             ftd_info.get("ftd_date",""),
            "ftd_value":            ftd_info.get("ftd_value"),
            "regsho_days":          rs_days,
            "float":                _best_float(fd, to_float(row.get("shares_float"))),
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
            "vol_avg":              fd.get("vol_avg") or avg_vol_map.get(ticker),
            "country":              fd.get("country"),
            "inst_own_pct":         fd.get("inst_own_pct"),
            "enterprise_value":     fd.get("enterprise_value"),
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
