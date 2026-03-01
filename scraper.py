"""
SHORT RADAR — scraper.py
Çalışma: GitHub Actions, hafta içi 07:00 UTC

Veri kaynakları:
  1. Chartexchange   — C2B, short volume  (session + cookie simülasyonu)
  2. FINRA           — RegSHO threshold + short interest (CDN, doğrudan TXT)
  3. StockAnalysis   — Reverse split listesi
  4. Finviz          — Insider işlemleri
  5. SEC EDGAR EFTS  — S-1 / S-1/A başvuruları
  6. SEC EDGAR XBRL  — Float, warrant verisi

Her kaynak için başarı/başarısızlık meta.json'a kaydedilir.
Kaynak başarısız olursa eski dosya korunur (veri kaybı olmaz).
"""

import csv
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup

# ── Dizinler ────────────────────────────────────
OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Headers ─────────────────────────────────────
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
}

# SEC kendi politikası gereği e-posta içeren UA zorunlu
SEC_HEADERS = {
    "User-Agent": "ShortRadar research contact@example.com",
    "Accept":     "application/json",
}

# ── Kaynak durumu (meta.json için) ───────────────
SOURCE_STATUS = {}   # {"chartexchange": "ok"/"error:...", ...}


# ════════════════════════════════════════════════
# YARDIMCI FONKSİYONLAR
# ════════════════════════════════════════════════

def load_existing(filename: str):
    path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save(filename: str, data, min_records: int = 1) -> bool:
    """
    Güvenli kayıt: yeterli veri yoksa eski dosyayı korur, False döner.
    """
    n = len(data) if isinstance(data, (list, dict)) else 1
    if isinstance(data, list) and n < min_records:
        old   = load_existing(filename)
        old_n = len(old) if isinstance(old, list) else 0
        print(f"  ⚠  {filename} — {n} kayıt < eşik {min_records}. "
              f"Eski dosya korundu ({old_n} kayıt).")
        return False
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"  ✓  {path}  ({n} kayıt)")
    return True


def save_meta(data: dict) -> None:
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


def parse_split_ratio(ratio_str: str):
    """
    "1 for 20" / "1-for-20.5" / "1:25" → bölen float.
    Reverse split ise bölen döner, forward split ise None.
    """
    if not ratio_str:
        return None
    s = str(ratio_str).lower().replace("-", " ").replace("–", " ")
    m = re.search(r"([\d.]+)\s*(?:for|:)\s*([\d.]+)", s)
    if not m:
        return None
    new_, old_ = float(m.group(1)), float(m.group(2))
    if new_ == 0:
        return None
    return round(old_ / new_, 4) if old_ > new_ else None


def normalize_date(raw: str) -> str:
    if not raw:
        return ""
    raw = raw.strip()
    for fmt in ["%Y-%m-%d", "%b %d, %Y", "%B %d, %Y",
                "%m/%d/%Y", "%d/%m/%Y", "%b %d,%Y"]:
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw


def workdays_back(n: int):
    """Son n iş gününün tarihlerini YYYYMMDD formatında üretir."""
    today = datetime.now()
    days  = []
    d     = today
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.strftime("%Y%m%d"))
        d -= timedelta(days=1)
    return days


# ════════════════════════════════════════════════
# 1. CHARTEXCHANGE — C2B + Short Volume
# ════════════════════════════════════════════════
def fetch_chartexchange() -> list:
    """
    Chartexchange JSON screener.
    Session cookie alınarak bot filtresi aşılır.
    403 veya boş yanıt → eski dosya korunur.
    """
    print("\n[1/6] Chartexchange C2B taranıyor...")

    session = requests.Session()
    session.headers.update({
        **BROWSER_HEADERS,
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
        "Upgrade-Insecure-Requests": "1",
    })

    # Ana sayfayı ziyaret et → session cookie + Cloudflare/bot bypass
    try:
        resp = session.get("https://chartexchange.com/", timeout=20)
        print(f"    Ana sayfa: HTTP {resp.status_code}, "
              f"cookie: {bool(session.cookies)}")
        time.sleep(2)
    except Exception as e:
        print(f"    Ana sayfa uyarısı (devam): {e}")

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
            "&equity_type=ad,cs"
            "&exchange=BATS,NASDAQ,NYSE,NYSEAMERICAN"
            "&currency=USD"
            "&shares_float=%3C5000000"
            "&reg_price=%3C6,%3E0.8"
            "&borrow_fee_avail_ib=%3C100000"
            "&per_page=100"
            f"&view_cols={COLS}"
            "&sort=borrow_fee_rate_ib,desc"
            "&format=json"
        )
        try:
            r = session.get(url, timeout=30, headers={
                "Accept":           "application/json, */*; q=0.01",
                "X-Requested-With": "XMLHttpRequest",
                "Referer":          "https://chartexchange.com/screener/",
            })
            print(f"    Sayfa {page}: HTTP {r.status_code}, "
                  f"{len(r.text)} byte")

            if r.status_code == 403:
                print("    403 Forbidden — IP bloklu, eski veri korunacak")
                SOURCE_STATUS["chartexchange"] = "error:403_blocked"
                break
            if r.status_code != 200:
                print(f"    HTTP {r.status_code} — içerik: {r.text[:200]}")
                SOURCE_STATUS["chartexchange"] = f"error:http_{r.status_code}"
                break

            data = r.json()
            rows = data.get("data", data) if isinstance(data, dict) else data
            if not isinstance(rows, list) or not rows:
                print(f"    Boş yanıt: {str(data)[:300]}")
                SOURCE_STATUS["chartexchange"] = "error:empty_response"
                break

            all_rows.extend(rows)
            print(f"    +{len(rows)} satır, toplam {len(all_rows)}")
            if len(rows) < 100:
                break
            page += 1
            time.sleep(0.7)
        except Exception as e:
            print(f"    İstisna (sayfa {page}): {e}")
            SOURCE_STATUS["chartexchange"] = f"error:{e}"
            break

    if all_rows:
        SOURCE_STATUS["chartexchange"] = f"ok:{len(all_rows)}"
    save("chartexchange.json", all_rows, min_records=10)
    return all_rows


# ════════════════════════════════════════════════
# 2. FINRA — RegSHO Threshold + Short Interest
#    (aynı CDN, farklı dosya türleri)
# ════════════════════════════════════════════════
def _parse_pipe_txt(text: str, source_label: str) -> list:
    """Pipe-delimited TXT dosyasını dict listesine çevirir."""
    rows  = []
    lines = [l for l in text.strip().split("\n") if l.strip()]
    if len(lines) < 2:
        return rows
    hdrs = [h.strip() for h in lines[0].split("|")]
    for line in lines[1:]:
        parts = [p.strip() for p in line.split("|")]
        if not parts or not parts[0] or parts[0].lower() in ("symbol",""):
            continue
        row = dict(zip(hdrs, parts))
        # Symbol normalize
        if "Symbol" not in row:
            row["Symbol"] = parts[0]
        row["_source"] = source_label
        rows.append(row)
    return rows


def fetch_regsho() -> list:
    """
    RegSHO threshold listesi.
    Birden fazla kaynak ve tarih dener.

    URL'ler (öncelik sırasına göre):
      1. FINRA CDN daily threshold: cdn.finra.org/equity/regsho/daily/...
      2. NASDAQ Trader dynamic TXT: nasdaqtrader.com/dynamic/symdir/regsho/...
    """
    print("\n[2/6] RegSHO threshold listesi çekiliyor...")
    rows = []

    # ── Kaynak 1: FINRA CDN (en güvenilir, doğrudan S3-benzeri CDN) ──
    # Format: threshold{YYYYMMDD}.txt  veya  FINRAthreshold{YYYYMMDD}.txt
    finra_patterns = [
        "https://cdn.finra.org/equity/regsho/daily/threshold{d}.txt",
        "https://cdn.finra.org/equity/regsho/daily/FINRAthreshold{d}.txt",
    ]
    for date_str in workdays_back(5):
        if rows:
            break
        for pattern in finra_patterns:
            url = pattern.format(d=date_str)
            try:
                r = requests.get(url, headers=BROWSER_HEADERS, timeout=15)
                print(f"    FINRA threshold {date_str}: HTTP {r.status_code} ({len(r.text)} byte)")
                if r.status_code == 200 and "|" in r.text and len(r.text) > 50:
                    parsed = _parse_pipe_txt(r.text, "FINRA")
                    if parsed:
                        rows = parsed
                        print(f"    ✓ FINRA CDN ({date_str}): {len(rows)} ticker")
                        break
            except Exception as e:
                print(f"    FINRA {url}: {e}")
        time.sleep(0.3)

    # ── Kaynak 2: NASDAQ Trader dynamic ──
    if not rows:
        nasdaq_patterns = [
            "https://www.nasdaqtrader.com/dynamic/symdir/regsho/nasdaqth{d}.txt",
            "https://nasdaqtrader.com/dynamic/symdir/regsho/nasdaqth{d}.txt",
        ]
        for date_str in workdays_back(5):
            if rows:
                break
            for pattern in nasdaq_patterns:
                url = pattern.format(d=date_str)
                try:
                    r = requests.get(url, headers=BROWSER_HEADERS, timeout=15)
                    print(f"    NASDAQ trader {date_str}: HTTP {r.status_code}")
                    if r.status_code == 200 and "|" in r.text and len(r.text) > 50:
                        parsed = _parse_pipe_txt(r.text, "NASDAQ")
                        if parsed:
                            rows = parsed
                            print(f"    ✓ NASDAQ Trader ({date_str}): {len(rows)} ticker")
                            break
                except Exception as e:
                    print(f"    NASDAQ {url}: {e}")
            time.sleep(0.3)

    if rows:
        SOURCE_STATUS["regsho"] = f"ok:{len(rows)}"
    else:
        print("    ⚠ RegSHO: hiçbir kaynaktan veri alınamadı")
        SOURCE_STATUS["regsho"] = "error:all_sources_failed"

    save("regsho.json", rows, min_records=1)
    return rows


def fetch_finra_short_interest() -> dict:
    """
    FINRA biweekly short interest dosyaları.
    FNSQ=NASDAQ, FNYS=NYSE, FNOQ=OTC
    """
    print("\n[FINRA SI] Short interest çekiliyor...")
    result   = {}
    prefixes = [
        ("FNSQ", "NASDAQ"),
        ("FNYS", "NYSE"),
        ("FNOQ", "OTC"),
    ]
    for prefix, exchange in prefixes:
        found = False
        for date_str in workdays_back(45):   # biweekly → 45 gün yeterli
            url = f"https://cdn.finra.org/equity/regsho/biweekly/{prefix}{date_str}.txt"
            try:
                rh = requests.head(url, headers=BROWSER_HEADERS, timeout=8)
                if rh.status_code != 200:
                    continue
                r = requests.get(url, headers=BROWSER_HEADERS, timeout=30)
                if r.status_code != 200 or len(r.text) < 100:
                    continue
                count = 0
                for line in r.text.strip().split("\n")[1:]:
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
                        result[ticker] = {
                            "short_interest": si,
                            "si_date":        date_str,
                            "exchange":       exchange,
                        }
                    count += 1
                print(f"    {exchange} ({date_str}): {count} ticker")
                found = True
                break
            except Exception:
                pass
            time.sleep(0.05)
        if not found:
            print(f"    {exchange}: güncel dosya bulunamadı")

    print(f"    Toplam FINRA SI: {len(result)} ticker")
    SOURCE_STATUS["finra_si"] = f"ok:{len(result)}" if result else "error:no_data"
    return result


# ════════════════════════════════════════════════
# 3. STOCK ANALYSIS — Reverse Splits
# ════════════════════════════════════════════════
def fetch_splits() -> list:
    print("\n[3/6] Split listesi çekiliyor...")
    rows = []
    pages = {
        "recent":   "https://stockanalysis.com/actions/splits/",
        "upcoming": "https://stockanalysis.com/actions/splits/upcoming/",
    }
    for label, url in pages.items():
        try:
            r = requests.get(url, headers=BROWSER_HEADERS, timeout=30)
            print(f"    {label}: HTTP {r.status_code}")
            soup  = BeautifulSoup(r.text, "html.parser")
            table = soup.find("table")
            if not table:
                print(f"    {label}: tablo bulunamadı")
                continue
            hdrs = [th.text.strip() for th in table.find_all("th")]
            for tr in table.find_all("tr")[1:]:
                cells = [td.text.strip() for td in tr.find_all("td")]
                if not cells:
                    continue
                row      = dict(zip(hdrs, cells))
                ratio    = parse_split_ratio(row.get("Ratio") or row.get("Split Ratio",""))
                raw_date = row.get("Date") or row.get("Split Date","")
                row["is_reverse"]  = ratio is not None
                row["split_ratio"] = ratio
                row["split_date"]  = normalize_date(raw_date)
                row["list_type"]   = label
                rows.append(row)
            rev = sum(1 for x in rows if x.get("is_reverse") and x.get("list_type")==label)
            print(f"    {label}: {rev} reverse split")
            time.sleep(0.4)
        except Exception as e:
            print(f"    {label}: {e}")

    SOURCE_STATUS["splits"] = f"ok:{sum(1 for x in rows if x.get('is_reverse'))}"
    save("splits.json", rows, min_records=1)
    return rows


# ════════════════════════════════════════════════
# 4. FINVIZ — Insider İşlemleri
# ════════════════════════════════════════════════
def fetch_insider() -> list:
    print("\n[4/6] Finviz insider taranıyor...")
    try:
        r = requests.get(
            "https://finviz.com/insidertrading?tc=1",
            headers={**BROWSER_HEADERS, "Referer": "https://finviz.com/"},
            timeout=30,
        )
        print(f"    HTTP {r.status_code}, {len(r.text)} byte")
        soup = BeautifulSoup(r.text, "html.parser")
        rows = []
        for t in soup.find_all("table"):
            ths = t.find_all("th")
            if ths and any("Ticker" in th.text for th in ths):
                hdrs = [th.text.strip() for th in ths]
                for tr in t.find_all("tr")[1:60]:
                    cells = [td.text.strip() for td in tr.find_all("td")]
                    if cells and len(cells) >= 4:
                        rows.append(dict(zip(hdrs, cells)))
                break
        print(f"    {len(rows)} insider işlem")
        SOURCE_STATUS["insider"] = f"ok:{len(rows)}"
        save("insider.json", rows, min_records=5)
        return rows
    except Exception as e:
        print(f"    Hata: {e}")
        SOURCE_STATUS["insider"] = f"error:{e}"
        save("insider.json", [], min_records=5)
        return []


# ════════════════════════════════════════════════
# 5. SEC EDGAR — S-1 / S-1/A Başvuruları
# ════════════════════════════════════════════════
def fetch_sec_s1() -> list:
    """
    SEC EDGAR EFTS (full-text search) üzerinden S-1 ve S-1/A başvuruları.

    Önemli: S-1 dosyalayan şirketlerin büyük çoğunluğu henüz borsada işlem
    görmediğinden ticker alanı BOŞ gelir. Bu beklenen bir durumdur.
    Şirket adı (company) her zaman dolu olmalıdır.
    """
    print("\n[5/6] SEC EDGAR S-1 başvuruları çekiliyor...")
    rows, seen = [], set()
    start = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    end   = datetime.now().strftime("%Y-%m-%d")

    for form_type in ["S-1", "S-1/A"]:
        try:
            r = requests.get(
                "https://efts.sec.gov/LATEST/search-index",
                params={
                    "forms":     form_type,
                    "dateRange": "custom",
                    "startdt":   start,
                    "enddt":     end,
                },
                headers=SEC_HEADERS,
                timeout=30,
            )
            print(f"    {form_type}: HTTP {r.status_code}")
            if r.status_code != 200:
                continue

            hits = r.json().get("hits", {}).get("hits", [])
            print(f"    {form_type}: {len(hits)} hit")

            for hit in hits:
                s   = hit.get("_source", {})
                uid = s.get("entity_name","") + s.get("file_date","")
                if uid in seen:
                    continue
                seen.add(uid)

                entity = s.get("entity_name","").strip()

                # Çoğu S-1'de ticker yoktur; bazılarında şirket adında olabilir
                ticker = ""
                m = re.search(r"\(proposed[:\s]+([A-Za-z]{1,5})\)", entity, re.IGNORECASE)
                if m:
                    ticker = m.group(1).upper()

                edgar_url = (
                    "https://www.sec.gov/cgi-bin/browse-edgar"
                    "?action=getcompany"
                    f"&company={requests.utils.quote(entity)}"
                    "&type=S-1&dateb=&owner=include&count=10"
                )

                rows.append({
                    "form":       form_type,
                    "ticker":     ticker,
                    "company":    entity,
                    "filed_date": s.get("file_date",""),
                    "edgar_url":  edgar_url,
                })
            time.sleep(0.4)
        except Exception as e:
            print(f"    {form_type} hatası: {e}")
            SOURCE_STATUS[f"s1_{form_type}"] = f"error:{e}"

    print(f"    Toplam: {len(rows)} başvuru (son 30 gün)")
    SOURCE_STATUS["s1"] = f"ok:{len(rows)}"
    save("s1_edgar.json", rows, min_records=1)
    return rows


# ════════════════════════════════════════════════
# 6. SEC EDGAR XBRL — Float + Warrant
# ════════════════════════════════════════════════
def _cik_map(tickers: list) -> dict:
    try:
        r = requests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers=SEC_HEADERS, timeout=30,
        )
        mapping = {}
        for entry in r.json().values():
            t = entry.get("ticker","").upper()
            c = str(entry.get("cik_str","")).zfill(10)
            if t:
                mapping[t] = c
        result = {t: mapping[t.upper()] for t in tickers if t.upper() in mapping}
        print(f"    CIK eşleme: {len(result)}/{len(tickers)}")
        return result
    except Exception as e:
        print(f"    CIK map hatası: {e}")
        return {}


def _latest_xbrl(facts: dict, *concepts) -> tuple:
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
            cands.sort(
                key=lambda x: (x.get("end",""), -priority.get(x.get("form",""), 99)),
                reverse=True,
            )
            b = cands[0]
            return b.get("val"), b.get("filed", b.get("end")), b.get("form")
    return None, None, None


def fetch_edgar_floats(tickers: list, split_map: dict) -> dict:
    print(f"\n[6/6] EDGAR XBRL float çekiliyor — {len(tickers)} ticker...")
    if not tickers:
        print("    Ticker listesi boş, atlanıyor.")
        SOURCE_STATUS["edgar_float"] = "skip:no_tickers"
        return {}

    cik_map = _cik_map(tickers)
    result  = {}
    not_found = []

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
                not_found.append(ticker)
                time.sleep(0.15)
                continue
            facts = r.json()

            float_val, float_date, float_form = _latest_xbrl(
                facts,
                "CommonStockSharesOutstanding",
                "EntityCommonStockSharesOutstanding",
                "FloatShares",
                "CommonStockSharesIssued",
            )
            warrant_val, _, _ = _latest_xbrl(
                facts,
                "ClassOfWarrantOrRightOutstanding",
                "WarrantsAndRightsOutstanding",
            )

            sp          = split_map.get(ticker, {})
            sp_ratio    = sp.get("ratio")
            sp_date     = sp.get("date","")
            edgar_dt    = (float_date or "")[:10]
            is_presplit = bool(sp_ratio and sp_date and edgar_dt and edgar_dt < sp_date)
            est_post    = (int(float_val / sp_ratio)
                          if (is_presplit and float_val and sp_ratio) else None)

            result[ticker] = {
                "ticker":               ticker,
                "cik":                  cik,
                "float_shares":         float_val,
                "float_date":           float_date,
                "float_form":           float_form,
                "warrant_shares":       warrant_val,
                "float_is_presplit":    is_presplit,
                "est_post_split_float": est_post,
                "split_ratio":          sp_ratio,
                "split_date":           sp_date,
            }
        except Exception as e:
            not_found.append(ticker)
        time.sleep(0.12)

    if not_found:
        sample = ", ".join(not_found[:8])
        extra  = f" +{len(not_found)-8}" if len(not_found) > 8 else ""
        print(f"    Bulunamadı: {sample}{extra}")

    print(f"    {len(result)} ticker için float alındı")
    SOURCE_STATUS["edgar_float"] = f"ok:{len(result)}"
    save("floats.json", list(result.values()), min_records=1)
    return result


# ════════════════════════════════════════════════
# SQUEEZE SKORU
# ════════════════════════════════════════════════
def squeeze_score(s: dict) -> tuple:
    score, reasons = 0, []

    sf = to_float(s.get("short_float_pct"))
    if sf is not None:
        if   sf >= 50: score += 25; reasons.append("SI%≥50")
        elif sf >= 30: score += 18; reasons.append("SI%≥30")
        elif sf >= 15: score += 10; reasons.append("SI%≥15")
        elif sf >=  5: score +=  4

    c2b = to_float(s.get("c2b"))
    if c2b is not None:
        if   c2b >= 200: score += 25; reasons.append("C2B≥200%")
        elif c2b >= 100: score += 18; reasons.append("C2B≥100%")
        elif c2b >=  50: score += 12; reasons.append("C2B≥50%")
        elif c2b >=  20: score +=  6
        elif c2b >=  10: score +=  3

    fl = to_float(s.get("diluted_float") or s.get("float"))
    if fl is not None:
        if   fl <   500_000: score += 20; reasons.append("Float<500K")
        elif fl < 1_000_000: score += 15; reasons.append("Float<1M")
        elif fl < 2_000_000: score += 10; reasons.append("Float<2M")
        elif fl < 5_000_000: score +=  5

    dtc = to_float(s.get("dtc"))
    if dtc is not None:
        if   dtc >= 10: score += 15; reasons.append("DTC≥10")
        elif dtc >=  5: score += 10; reasons.append("DTC≥5")
        elif dtc >=  2: score +=  5

    if s.get("reg_sho") == "✅":
        score += 10; reasons.append("RegSHO")

    si_chg = to_float(s.get("si_change"))
    if si_chg is not None:
        if   si_chg >= 50: score +=  5; reasons.append("SI+%≥50")
        elif si_chg >= 20: score +=  3; reasons.append("SI+%≥20")
        elif si_chg < -20: score -=  3

    return max(0, min(100, score)), reasons


# ════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════
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
        for r in rs
        if r.get("Symbol") or r.get("Ticker")
    }
    print(f"\n  RegSHO tickers: {len(regsho_tickers)}")

    split_map = {}
    for row in sp:
        if not row.get("is_reverse"):
            continue
        t = (row.get("Symbol") or row.get("Ticker") or
             row.get("symbol") or row.get("ticker",""))
        if t and t not in split_map and row.get("split_ratio"):
            split_map[t] = {
                "ratio": row["split_ratio"],
                "date":  row.get("split_date",""),
            }

    # C2B'den ticker listesi
    top_c2b_tickers = set()
    for row in ce[:50]:
        t = row.get("symbol") or row.get("ticker") or row.get("Symbol","")
        if t:
            top_c2b_tickers.add(t)

    # Ortalama hacim haritası
    avg_vol_map = {}
    for row in ce:
        t = row.get("symbol") or row.get("ticker") or row.get("Symbol","")
        if not t:
            continue
        v = to_float(row.get("10_day_avg_vol") or row.get("tenDayAvgVol"))
        if v:
            avg_vol_map[t] = v

    # ── EDGAR float ─────────────────────────────
    float_tickers = list(regsho_tickers | set(split_map.keys()) | top_c2b_tickers)
    float_map     = fetch_edgar_floats(float_tickers, split_map)

    # FINRA SI + DTC → float_map'e yaz
    for ticker, fd in float_map.items():
        fi        = finra_si.get(ticker, {})
        si_shares = fi.get("short_interest")
        eff_float = fd.get("est_post_split_float") or fd.get("float_shares")
        warrant   = fd.get("warrant_shares") or 0
        diluted   = int(eff_float + warrant) if eff_float else None
        sf_pct    = (round(si_shares / eff_float * 100, 2)
                     if (si_shares and eff_float) else None)
        avg_vol   = avg_vol_map.get(ticker)
        dtc       = (round(si_shares / avg_vol, 2)
                     if (si_shares and avg_vol and avg_vol > 0) else None)
        fd.update({
            "finra_si":        si_shares,
            "finra_si_date":   fi.get("si_date",""),
            "short_float_pct": sf_pct,
            "diluted_float":   diluted,
            "dtc":             dtc,
            "effective_float": eff_float,
        })
    save("floats.json", list(float_map.values()), min_records=1)

    # ── CE haritası ─────────────────────────────
    ce_map = {}
    for row in ce:
        t = row.get("symbol") or row.get("ticker") or row.get("Symbol","")
        if t:
            ce_map[t] = row

    # ── S1 ticker haritası ───────────────────────
    s1_map = {}
    for s in s1:
        t = s.get("ticker","")
        if t:
            s1_map[t] = s

    # ── Tüm kaynaklardan birleşik ticker seti ────
    # CE gelmese bile RegSHO + Split + FINRA SI tickerları görünür.
    all_tickers = (
        set(ce_map.keys())          # Chartexchange (C2B)
        | regsho_tickers            # RegSHO listesi
        | set(split_map.keys())     # Reverse split yapanlar
        | set(float_map.keys())     # EDGAR float bilgisi olanlar
    )
    # Filtre: en az bir anlamlı veri kaynağından gelen tickerlar
    # (sadece EDGAR'da ismi geçen ama hiç işlem görmeyen şirketleri atla)
    def has_any_data(t):
        return (t in ce_map or t in regsho_tickers or
                t in split_map or finra_si.get(t))
    all_tickers = {t for t in all_tickers if t and has_any_data(t)}
    print(f"  Birleşik ticker seti: {len(all_tickers)} (CE:{len(ce_map)} RS:{len(regsho_tickers)} SP:{len(split_map)} FI:{len(float_map)})")

    # ── Özet tablosu ─────────────────────────────
    summary_map = {}
    for ticker in sorted(all_tickers):
        row = ce_map.get(ticker, {})   # CE verisi yoksa boş dict
        fd  = float_map.get(ticker, {})
        s1r = s1_map.get(ticker, {})

        eff_float = (fd.get("effective_float") or
                     to_float(row.get("shares_float")))
        sf_pct    = (fd.get("short_float_pct") or
                     to_float(row.get("shortint_pct")))

        rec = {
            "ticker":               ticker,
            # C2B — sadece CE gelirse dolu olur
            "c2b":                  to_float(row.get("borrow_fee_rate_ib") or
                                             row.get("borrowFeeRateIb")),
            "shares_avail":         to_float(row.get("borrow_fee_avail_ib")),
            # Float
            "float":                (fd.get("est_post_split_float") or
                                     fd.get("float_shares") or
                                     to_float(row.get("shares_float"))),
            "diluted_float":        fd.get("diluted_float"),
            "warrant_shares":       fd.get("warrant_shares"),
            "float_is_presplit":    fd.get("float_is_presplit", False),
            "est_post_split_float": fd.get("est_post_split_float"),
            "float_date":           fd.get("float_date",""),
            "float_form":           fd.get("float_form",""),
            # Short interest
            "short_float_pct":      sf_pct,
            "finra_si":             fd.get("finra_si"),
            "finra_si_date":        fd.get("finra_si_date",""),
            "si_change":            to_float(row.get("shortint_position_change_pct")),
            "short_vol_pct":        to_float(row.get("shortvol_all_short_pct")),
            "short_vol_30d_pct":    to_float(row.get("shortvol_all_short_pct_30d")),
            # DTC
            "dtc":                  fd.get("dtc"),
            "avg_vol_10d":          avg_vol_map.get(ticker),
            # Fiyat
            "price":                to_float(row.get("reg_price")),
            "change_pct":           to_float(row.get("reg_change_pct")),
            "pre_price":            to_float(row.get("pre_price")),
            "pre_change":           to_float(row.get("pre_change_pct")),
            # Flags
            "reg_sho":              "✅" if ticker in regsho_tickers else "❌",
            "has_split":            "✅" if ticker in split_map      else "-",
            "split_ratio":          split_map.get(ticker,{}).get("ratio"),
            "split_date":           split_map.get(ticker,{}).get("date",""),
            "s1_date":              s1r.get("filed_date",""),
            "s1_form":              s1r.get("form",""),
            "offering_warning":     bool(s1r and s1r.get("form") == "S-1/A"),
        }
        sc, reasons = squeeze_score(rec)
        rec["squeeze_score"]   = sc
        rec["squeeze_reasons"] = ", ".join(reasons)
        summary_map[ticker]    = rec

    # S1 kayıtlarını zenginleştir
    for s in s1:
        t  = s.get("ticker","")
        fd = float_map.get(t, {})
        s.update({
            "float":           fd.get("est_post_split_float") or fd.get("float_shares"),
            "diluted_float":   fd.get("diluted_float"),
            "float_date":      fd.get("float_date",""),
            "float_form":      fd.get("float_form",""),
            "short_float_pct": fd.get("short_float_pct"),
            "reg_sho":         "✅" if t in regsho_tickers else "❌",
            "in_summary":      t in summary_map,
        })

    run_end = datetime.now(timezone.utc)
    elapsed = round((run_end - run_start).total_seconds())

    # ── Dosyaları kaydet ────────────────────────
    results = {
        "summary":  save("summary.json",        list(summary_map.values()), min_records=1),
        "regsho_t": save("regsho_tickers.json", list(regsho_tickers),       min_records=1),
        "s1":       save("s1_edgar.json",        s1,                        min_records=1),
    }
    critical_ok = results["summary"]

    save_meta({
        "updated_at":      run_end.isoformat(),
        "elapsed_sec":     elapsed,
        "scraper_ok":      critical_ok,
        "protected_files": [k for k, v in results.items() if not v],
        "source_status":   SOURCE_STATUS,
        "counts": {
            "chartexchange":  len(ce),
            "regsho":         len(rs),
            "splits_reverse": sum(1 for x in sp if x.get("is_reverse")),
            "splits_upcoming":sum(1 for x in sp if x.get("is_reverse") and
                                  x.get("list_type")=="upcoming"),
            "insider":        len(ins),
            "s1_edgar":       len(s1),
            "floats":         len(float_map),
            "finra_si":       len(finra_si),
            "summary":        len(summary_map),
        },
    })

    print(f"\n{'='*60}")
    print(f"  ✅  Tamamlandı ({elapsed}s)")
    for src, status in SOURCE_STATUS.items():
        icon = "✓" if status.startswith("ok") else "✗"
        print(f"  {icon}  {src}: {status}")
    print(f"{'='*60}\n")
