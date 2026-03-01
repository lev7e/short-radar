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
    if val is None or val in ("-", ""):
        return default
    try:
        return float(str(val).replace("%", "").replace(",", ".").strip())
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
    Chartexchange HTML tablo satırını API alanlarına normalize eder.
    HTML başlıkları scraper versiyonuna göre değişebilir — hepsini dene.
    """
    # Ticker: ilk kolon genellikle "Display", "Symbol", "Ticker" veya "display"
    ticker = ""
    for k in ["Display", "Symbol", "Ticker", "display", "symbol", "ticker"]:
        if k in row and row[k]:
            ticker = str(row[k]).strip().split()[0]  # "AAPL (Apple)" → "AAPL"
            break
    if not ticker and hdrs:
        ticker = str(row.get(hdrs[0],"")).strip().split()[0]

    def pick(*keys):
        for k in keys:
            if k in row and row[k] not in ("", "-", "N/A", None):
                return row[k]
        return None

    return {
        "symbol":                       ticker,
        "ticker":                       ticker,
        "borrow_fee_rate_ib":           pick("Borrow Rate", "Borrow Fee", "C2B Rate", "C2B%",
                                             "borrow_fee_rate_ib", "borrowFeeRateIb",
                                             "Borrow Fee Rate", "Fee Rate"),
        "borrow_fee_avail_ib":          pick("Available", "Avail", "Shares Avail",
                                             "borrow_fee_avail_ib"),
        "shares_float":                 pick("Float", "Float Shares", "shares_float",
                                             "Shares Float"),
        "reg_price":                    pick("Price", "Last", "reg_price", "Close"),
        "reg_change_pct":               pick("Change %", "Chg %", "Change", "reg_change_pct"),
        "reg_volume":                   pick("Volume", "Vol", "reg_volume"),
        "10_day_avg_vol":               pick("Avg Vol", "10D Avg Vol", "10_day_avg_vol",
                                             "Avg Volume"),
        "shortint_pct":                 pick("SI %", "Short Int %", "Short Interest %",
                                             "shortint_pct", "SI%"),
        "shortint_position_change_pct": pick("SI Chg %", "SI Change", "shortint_position_change_pct"),
        "shortvol_all_short_pct":       pick("Short Vol %", "Short Volume %",
                                             "shortvol_all_short_pct"),
        "shortvol_all_short_pct_30d":   pick("30D Short %", "shortvol_all_short_pct_30d"),
        "pre_price":                    pick("Pre Price", "pre_price"),
        "pre_change_pct":               pick("Pre Chg %", "pre_change_pct"),
        "_source":                      "html_table",
        "_raw":                         row,   # debug için orijinal satır
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
                        # th yoksa thead>td veya ilk tr>td'den başlık al
                        hdrs = [th.get_text(strip=True) for th in table.find_all("th")]
                        all_trs = table.find_all("tr")
                        if not hdrs and all_trs:
                            # İlk tr'yi başlık kabul et
                            hdrs = [td.get_text(strip=True) for td in all_trs[0].find_all("td")]
                            data_trs = all_trs[1:]
                        else:
                            data_trs = all_trs[1:]
                        print(f"    CE tablo başlıkları: {hdrs[:8]}")
                        raw_rows = []
                        for tr in data_trs:
                            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                            if cells and len(cells) >= 3:
                                raw_rows.append(dict(zip(hdrs, cells)))
                        if raw_rows:
                            rows = [_normalize_ce_row(r, hdrs) for r in raw_rows]
                            # Geçerli ticker olan satırları filtrele
                            rows = [r for r in rows if r.get("ticker","").strip()]
                            print(f"    HTML tablo: {len(rows)} geçerli satır")
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
            time.sleep(0.7)

        except Exception as e:
            print(f"    İstisna: {e}")
            SOURCE_STATUS["chartexchange"] = f"error:{e}"
            break

    if all_rows:
        SOURCE_STATUS["chartexchange"] = f"ok:{len(all_rows)}"
    save("chartexchange.json", all_rows, min_records=10)
    return all_rows


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
    print("\n[2/6] RegSHO threshold listesi çekiliyor...")
    rows = []

    # ── Kaynak 1: FINRA CDN daily ──
    # Log'a göre 403 veriyordu — farklı URL formatları dene
    finra_urls = [
        "https://cdn.finra.org/equity/regsho/daily/threshold{d}.txt",
        "https://cdn.finra.org/equity/regsho/daily/FINRAthreshold{d}.txt",
        # Bazı tarihler için alternatif format
        "https://cdn.finra.org/equity/regsho/daily/FINRA_threshold_{d}.txt",
    ]
    for date_str in workdays_back(5):
        if rows:
            break
        for pat in finra_urls:
            url = pat.format(d=date_str)
            try:
                r = requests.get(url, headers=BROWSER_HEADERS, timeout=15)
                print(f"    FINRA {date_str}: HTTP {r.status_code} ({len(r.content)}b)")
                if r.status_code == 200 and "|" in r.text and len(r.text) > 100:
                    parsed = _parse_pipe(r.text, "FINRA")
                    if parsed:
                        rows = parsed
                        print(f"    ✓ FINRA CDN ({date_str}): {len(rows)} ticker")
                        break
            except Exception as e:
                print(f"    FINRA {date_str}: {e}")
        if not rows:
            time.sleep(0.3)

    # ── Kaynak 2: NASDAQ Trader dynamic TXT (log'da çalışıyordu) ──
    if not rows:
        for date_str in workdays_back(5):
            if rows:
                break
            for pat in [
                "https://www.nasdaqtrader.com/dynamic/symdir/regsho/nasdaqth{d}.txt",
                "https://nasdaqtrader.com/dynamic/symdir/regsho/nasdaqth{d}.txt",
            ]:
                url = pat.format(d=date_str)
                try:
                    r = requests.get(url, headers=BROWSER_HEADERS, timeout=15)
                    print(f"    NASDAQ {date_str}: HTTP {r.status_code}")
                    if r.status_code == 200 and "|" in r.text and len(r.text) > 100:
                        parsed = _parse_pipe(r.text, "NASDAQ")
                        if parsed:
                            rows = parsed
                            print(f"    ✓ NASDAQ Trader ({date_str}): {len(rows)} ticker")
                            break
                except Exception as e:
                    print(f"    NASDAQ {date_str}: {e}")
            time.sleep(0.3)

    SOURCE_STATUS["regsho"] = f"ok:{len(rows)}" if rows else "error:all_failed"
    save("regsho.json", rows, min_records=1)
    return rows


# ══════════════════════════════════════════════════
# 3. FINRA — Short Interest biweekly
#    Log'da HEAD başarısız oluyordu. Direkt GET kullan.
# ══════════════════════════════════════════════════
def fetch_finra_short_interest():
    print("\n[FINRA SI] Short interest çekiliyor...")
    result = {}
    exchanges = [("FNSQ","NASDAQ"), ("FNYS","NYSE"), ("FNOQ","OTC")]

    for prefix, exch in exchanges:
        found = False
        for date_str in workdays_back(90):   # biweekly → 90 gün (uzun lookback)
            url = f"https://cdn.finra.org/equity/regsho/biweekly/{prefix}{date_str}.txt"
            try:
                r = requests.get(url, headers=BROWSER_HEADERS, timeout=20)
                if r.status_code != 200:
                    continue
                text = r.text
                if "|" not in text or len(text) < 100:
                    continue
                count = 0
                for line in text.strip().split("\n")[1:]:
                    parts = line.strip().split("|")
                    if len(parts) < 3:
                        continue
                    ticker = parts[0].strip().upper()
                    if not ticker or ticker == "SYMBOL":
                        continue
                    try:
                        si = int(str(parts[2]).replace(",", ""))
                    except Exception:
                        continue
                    if ticker not in result or date_str > result[ticker]["si_date"]:
                        result[ticker] = {"short_interest": si,
                                          "si_date": date_str,
                                          "exchange": exch}
                    count += 1
                print(f"    {exch} ({date_str}): {count} ticker")
                found = True
                break
            except StopIteration:
                pass
            except Exception as e:
                print(f"    {exch} {date_str}: {e}")
            time.sleep(0.05)
        if not found:
            print(f"    {exch}: dosya bulunamadı (son 50 gün)")

    print(f"    Toplam FINRA SI: {len(result)} ticker")
    SOURCE_STATUS["finra_si"] = f"ok:{len(result)}" if result else "error:not_found"
    return result


# ══════════════════════════════════════════════════
# 4. STOCK ANALYSIS — Reverse Splits
#    Tablo bulunamıyor çünkü Next.js → __NEXT_DATA__ parse
# ══════════════════════════════════════════════════
def fetch_splits():
    print("\n[3/6] Split listesi çekiliyor (StockAnalysis)...")
    rows = []

    # StockAnalysis API endpoint — Next.js sayfasından daha güvenilir
    API_URLS = {
        "recent":   "https://stockanalysis.com/actions/splits/?p=quarterly",
        "upcoming": "https://stockanalysis.com/actions/splits/upcoming/?p=quarterly",
    }
    for label, url in API_URLS.items():
        try:
            r = requests.get(url, headers={**BROWSER_HEADERS,
                             "Accept": "text/html,*/*"}, timeout=30)
            print(f"    {label}: HTTP {r.status_code}, {len(r.content)}b")

            page_rows = []

            # Yöntem 1: __NEXT_DATA__ (Next.js)
            nd = next_data(r.text)
            if nd:
                # Olası veri yolları
                for path in [
                    ["props","pageProps","data"],
                    ["props","pageProps","splits"],
                    ["props","pageProps","tableData"],
                    ["props","pageProps","initialData"],
                ]:
                    node = nd
                    try:
                        for key in path:
                            node = node[key]
                        if isinstance(node, list) and node:
                            page_rows = node
                            print(f"    {label} __NEXT_DATA__[{'.'.join(path)}]: {len(page_rows)}")
                            break
                    except (KeyError, TypeError):
                        pass

                # Eğer bulunamadıysa tüm NEXT_DATA'yı tara
                if not page_rows:
                    nd_str = json.dumps(nd)
                    # split ratio pattern ara
                    matches = re.findall(
                        r'\{"symbol":"([A-Z]+)"[^}]*"ratio":"([^"]+)"[^}]*"date":"([^"]+)"',
                        nd_str
                    )
                    if matches:
                        for sym, ratio, date in matches:
                            page_rows.append({"Symbol": sym, "Ratio": ratio, "Date": date})
                        print(f"    {label} regex: {len(page_rows)}")

            # Yöntem 2: HTML tablo (fallback)
            if not page_rows:
                soup = BeautifulSoup(r.text, "html.parser")
                table = soup.find("table")
                if table:
                    hdrs = [th.get_text(strip=True) for th in table.find_all("th")]
                    for tr in table.find_all("tr")[1:]:
                        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                        if cells:
                            page_rows.append(dict(zip(hdrs, cells)))
                    print(f"    {label} HTML tablo: {len(page_rows)}")

            # Normalize
            for row in page_rows:
                # Field adları değişken olabilir
                sym   = (row.get("Symbol") or row.get("symbol") or
                         row.get("ticker") or row.get("Ticker",""))
                ratio_str = (row.get("Ratio") or row.get("ratio") or
                             row.get("Split Ratio") or row.get("splitRatio",""))
                raw_date  = (row.get("Date") or row.get("date") or
                             row.get("Split Date") or row.get("splitDate",""))
                ratio = parse_split_ratio(str(ratio_str))
                rows.append({
                    "Symbol":      sym,
                    "Ratio":       ratio_str,
                    "Date":        normalize_date(str(raw_date)),
                    "is_reverse":  ratio is not None,
                    "split_ratio": ratio,
                    "split_date":  normalize_date(str(raw_date)),
                    "list_type":   label,
                })

            rev = sum(1 for x in rows if x.get("is_reverse") and x.get("list_type")==label)
            print(f"    {label}: {rev} reverse split")
            time.sleep(0.5)

        except Exception as e:
            print(f"    {label}: {e}")

    SOURCE_STATUS["splits"] = f"ok:{sum(1 for x in rows if x.get('is_reverse'))}"
    save("splits.json", rows, min_records=1)
    return rows


# ══════════════════════════════════════════════════
# 5. FINVIZ — Insider (tablo yapısı değişmiş)
# ══════════════════════════════════════════════════
def fetch_insider():
    print("\n[4/6] Finviz insider taranıyor...")
    rows = []
    try:
        # Ana insidertrading sayfası (login gerektirmez)
        for fin_url in [
            "https://finviz.com/insidertrading.ashx?or=-10&tv=100&tc=1&o=-transactionDate",
            "https://finviz.com/insidertrading?tc=1",
        ]:
            r = requests.get(
                fin_url,
                headers={**BROWSER_HEADERS,
                         "Accept":  "text/html,*/*",
                         "Referer": "https://finviz.com/"},
                timeout=30,
            )
            print(f"    {fin_url.split('?')[0]}: HTTP {r.status_code}, {len(r.content)}b")
            if r.status_code == 200 and len(r.content) > 10000:
                break
        print(f"    HTTP {r.status_code}, {len(r.content)}b")
        soup = BeautifulSoup(r.text, "html.parser")

        # Birden fazla tablo seçici dene
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
            print(f"    Tablo başlıkları: {hdrs[:8]}")
            for tr in table.find_all("tr")[1:100]:
                cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                if cells and len(cells) >= 4:
                    rows.append(dict(zip(hdrs, cells)))
        else:
            # Son çare: tüm tabloları logla
            tables = soup.find_all("table")
            print(f"    Tablolar: {len(tables)}")
            for i, t in enumerate(tables[:3]):
                ths = [th.get_text(strip=True) for th in t.find_all("th")]
                print(f"      Tablo {i}: {ths[:5]}")

        print(f"    {len(rows)} insider işlem")
        SOURCE_STATUS["insider"] = f"ok:{len(rows)}"
    except Exception as e:
        print(f"    Hata: {e}")
        SOURCE_STATUS["insider"] = f"error:{e}"

    save("insider.json", rows, min_records=5)
    return rows


# ══════════════════════════════════════════════════
# 6. SEC EDGAR — S-1 / S-1/A
#    Log'da 200 hit'ten 14 kayıt → field adları farklı
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

            data = r.json()
            hits = data.get("hits", {}).get("hits", [])
            if hits:
                s0 = hits[0].get("_source",{})
                dn0 = s0.get("display_names","")
                print(f"    {form_type}: {len(hits)} hit")
                print(f"      Alanlar: {list(s0.keys())[:8]}")
                dn_sample = str(dn0[0]) if isinstance(dn0,list) and dn0 else repr(dn0)[:80]
                print(f"      display_names[0]: {dn_sample}")

            for hit in hits:
                s = hit.get("_source", {})

                # display_names: [{"name": "Company Inc.", "ticker": "ABC", "cik": "..."}]
                display = s.get("display_names") or []
                if isinstance(display, list) and display:
                    first   = display[0] if isinstance(display[0], dict) else {}
                    entity  = first.get("name","").strip()
                    ticker  = first.get("ticker","").strip()
                else:
                    entity = ""
                    ticker = ""

                # Fallback: eski alan adları
                if not entity:
                    entity = (s.get("entity_name") or s.get("company_name") or
                              s.get("entityName") or s.get("filer_name") or "").strip()
                if not ticker:
                    t2 = s.get("ticker") or s.get("tickers") or ""
                    ticker = (t2[0] if isinstance(t2, list) and t2 else str(t2)).strip()

                filed = (s.get("file_date") or s.get("filed_at") or
                         s.get("fileDate") or "")

                # Dedup
                uid = f"{entity}|{filed}"
                if uid in seen:
                    continue
                if not entity and not ticker:
                    continue   # hiçbir tanımlayıcı yok
                seen.add(uid)

                # Ticker yoksa şirket adında ara
                if not ticker:
                    m = re.search(r"\(proposed[:\s]+([A-Za-z]{1,5})\)", entity, re.I)
                    if m:
                        ticker = m.group(1).upper()

                rows.append({
                    "form":       form_type,
                    "ticker":     ticker,
                    "company":    entity,
                    "filed_date": filed,
                    "edgar_url": (
                        "https://www.sec.gov/cgi-bin/browse-edgar"
                        f"?action=getcompany&company={requests.utils.quote(entity)}"
                        "&type=S-1&dateb=&owner=include&count=10"
                    ),
                })
            time.sleep(0.4)
        except Exception as e:
            print(f"    {form_type} hatası: {e}")

    print(f"    Toplam: {len(rows)} başvuru")
    SOURCE_STATUS["s1"] = f"ok:{len(rows)}"
    save("s1_edgar.json", rows, min_records=1)
    return rows


# ══════════════════════════════════════════════════
# 7. EDGAR XBRL — Float + Warrant
# ══════════════════════════════════════════════════
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
        print(f"    CIK hatası: {e}")
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
    finra_si = fetch_finra_short_interest()

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
    float_tickers = list(regsho_tickers | set(split_map.keys()) | set(ce_map.keys()))
    float_map     = fetch_edgar_floats(float_tickers, split_map)

    # FINRA SI → float_map'e ekle
    for ticker, fd in float_map.items():
        fi       = finra_si.get(ticker, {})
        si_sh    = fi.get("short_interest")
        eff_fl   = fd.get("est_post_split_float") or fd.get("float_shares")
        warrant  = fd.get("warrant_shares") or 0
        diluted  = int(eff_fl + warrant) if eff_fl else None
        sf_pct   = round(si_sh / eff_fl * 100, 2) if (si_sh and eff_fl) else None
        avg_vol  = avg_vol_map.get(ticker)
        dtc      = round(si_sh / avg_vol, 2) if (si_sh and avg_vol and avg_vol>0) else None
        fd.update({"finra_si": si_sh, "finra_si_date": fi.get("si_date",""),
                   "short_float_pct": sf_pct, "diluted_float": diluted,
                   "dtc": dtc, "effective_float": eff_fl})
    save("floats.json", list(float_map.values()), min_records=1)

    # ── S1 haritası ─────────────────────────────
    s1_map = {s["ticker"]: s for s in s1 if s.get("ticker")}

    # ── Birleşik summary ────────────────────────
    all_tickers = {t for t in (set(ce_map)|regsho_tickers|set(split_map)|set(float_map))
                   if t and (t in ce_map or t in regsho_tickers or
                             t in split_map or finra_si.get(t))}
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
            "dtc":                  fd.get("dtc"),
            "avg_vol_10d":          avg_vol_map.get(ticker),
            "price":                to_float(row.get("reg_price")),
            "change_pct":           to_float(row.get("reg_change_pct")),
            "pre_change":           to_float(row.get("pre_change_pct")),
            "reg_sho":              "✅" if ticker in regsho_tickers else "❌",
            "has_split":            "✅" if ticker in split_map else "-",
            "split_ratio":          split_map.get(ticker,{}).get("ratio"),
            "split_date":           split_map.get(ticker,{}).get("date",""),
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
            "finra_si":       len(finra_si),
            "summary":        len(summary_map),
        },
    })

    print(f"\n{'='*60}")
    print(f"  {'✅' if results['summary'] else '⚠'} Tamamlandı ({elapsed}s)")
    for src, status in SOURCE_STATUS.items():
        icon = "✓" if status.startswith("ok") else "✗"
        print(f"  {icon}  {src}: {status}")
    print(f"{'='*60}\n")
