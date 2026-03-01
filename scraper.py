"""
SHORT RADAR — scraper.py
Çalışma zamanı: GitHub Actions, hafta içi 07:00 UTC (02:00 ET, premarket'ten 2 saat önce)

Veri kaynakları:
  1. Chartexchange   — C2B, short volume, SI, float tarayıcısı
  2. Nasdaq          — RegSHO threshold listesi
  3. StockAnalysis   — Reverse split (recent + upcoming)
  4. Finviz          — Insider alım/satım
  5. SEC EDGAR       — S-1/S-1-A başvuruları (son 30 gün)
  6. FINRA           — Short interest (ayda 2 kez)
  7. SEC EDGAR XBRL  — Float, shares outstanding, warrant bilgisi
"""

import re
import csv
import json
import math
import os
import time
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup

# ── Dizin ──────────────────────────────────────
OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Headers ────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
# SEC: kendi politikası gereği e-posta içeren UA zorunlu
SEC_HEADERS = {
    "User-Agent": "ShortRadar research contact@example.com",
    "Accept": "application/json",
}

# ── Yardımcı ───────────────────────────────────
def load_existing(filename: str):
    """Mevcut JSON dosyasını okur; yoksa ya da bozuksa None döner."""
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
    Veriyi kaydeder. Güvenlik kuralları:
      - data boş liste/dict ise eski dosyayı KORUR (fetch başarısız olmuştur).
      - min_records'tan az kayıt varsa eski dosyayı KORUR.
      - Başarılı yazımda True, korunan durumda False döner.
    Bu sayede kısmi scraper hatası tüm veriyi silmez.
    """
    path    = os.path.join(OUTPUT_DIR, filename)
    is_list = isinstance(data, list)
    n       = len(data) if isinstance(data, (list, dict)) else 1

    # Güvenlik: yeterli kayıt yoksa eski dosyayı koru
    if is_list and n < min_records:
        old = load_existing(filename)
        old_n = len(old) if isinstance(old, list) else 0
        print(f"  ⚠  {filename} — {n} kayıt ({min_records} eşiğinin altında). "
              f"Eski dosya korundu ({old_n} kayıt).")
        return False

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"  ✓ {path}  ({n} kayıt)")
    return True


def save_meta(data: dict) -> None:
    """meta.json her zaman yazılır (scraper durumu burada izlenir)."""
    path = os.path.join(OUTPUT_DIR, "meta.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"  ✓ {path}")


def to_float(val, default=None):
    """Güvenli float dönüşümü: None, "-", "%" karakterleri temizler."""
    if val is None or val == "-" or val == "":
        return default
    try:
        return float(str(val).replace("%", "").replace(",", ".").strip())
    except Exception:
        return default


def parse_split_ratio(ratio_str: str):
    """
    "1 for 20", "1-for-20.5", "1:25", "0.05 for 1" → bölen (float)
    Reverse split için bölen = eski / yeni  (örn. 20.0)
    Forward split için None döner (zaten bizi ilgilendirmiyor).
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
    if old_ > new_:          # reverse split
        return round(old_ / new_, 4)
    return None              # forward split → ilgilenmiyoruz


def normalize_date(raw: str) -> str:
    """Çeşitli tarih formatlarını YYYY-MM-DD'ye çevirir."""
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


# ══════════════════════════════════════════════
# 1. CHARTEXCHANGE — C2B + Short Volume + SI
# ══════════════════════════════════════════════
def fetch_chartexchange() -> list:
    print("\n[1/7] Chartexchange taranıyor...")
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
            "&view_cols=display,borrow_fee_rate_ib,borrow_fee_avail_ib,"
            "shares_float,market_cap,reg_price,reg_change_pct,reg_volume,"
            "10_day_avg_vol,shortvol_all_short,shortvol_all_short_pct,"
            "shortint_db_pct,shortint_pct,shortint_position_change_pct,"
            "shortvol_all_short_pct_30d,pre_price,pre_change_pct"
            "&sort=borrow_fee_rate_ib,desc"
            "&format=json"
        )
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            data = r.json()
            rows = data.get("data", data) if isinstance(data, dict) else data
            if not rows:
                break
            all_rows.extend(rows)
            if len(rows) < 100:
                break
            page += 1
            time.sleep(0.3)
        except Exception as e:
            print(f"    Hata (sayfa {page}): {e}")
            break
    save("chartexchange.json", all_rows, min_records=10)
    return all_rows


# ══════════════════════════════════════════════
# 2. NASDAQ — RegSHO Threshold Listesi
# ══════════════════════════════════════════════
def fetch_regsho() -> list:
    print("\n[2/7] Nasdaq RegSHO çekiliyor...")
    url = "https://nasdaqtrader.com/Trader.aspx?id=RegSHOThreshold"
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        soup = BeautifulSoup(r.text, "html.parser")
        csv_url = None
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "RegSHOThreshold" in href and ".txt" in href:
                csv_url = ("https://nasdaqtrader.com" + href
                           if href.startswith("/") else href)
                break
        rows = []
        if csv_url:
            r2 = requests.get(csv_url, headers=HEADERS, timeout=30)
            reader = csv.DictReader(r2.text.strip().split("\n"), delimiter="|")
            for row in reader:
                if row.get("Symbol") and row["Symbol"] != "Symbol":
                    rows.append(dict(row))
        else:
            table = soup.find("table", {"class": "nasdaqTable"}) or soup.find("table")
            if table:
                hdrs = [th.text.strip() for th in table.find_all("th")]
                for tr in table.find_all("tr")[1:]:
                    cells = [td.text.strip() for td in tr.find_all("td")]
                    if cells and len(cells) == len(hdrs):
                        rows.append(dict(zip(hdrs, cells)))
        save("regsho.json", rows, min_records=1)
        return rows
    except Exception as e:
        print(f"    Hata: {e}")
        save("regsho.json", [], min_records=1)
        return []


# ══════════════════════════════════════════════
# 3. STOCK ANALYSIS — Reverse Splits
# ══════════════════════════════════════════════
def fetch_splits() -> list:
    print("\n[3/7] Split listesi çekiliyor (recent + upcoming)...")
    rows = []
    for label, url in {
        "recent":   "https://stockanalysis.com/actions/splits/",
        "upcoming": "https://stockanalysis.com/actions/splits/upcoming/",
    }.items():
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            soup = BeautifulSoup(r.text, "html.parser")
            table = soup.find("table")
            if not table:
                continue
            hdrs = [th.text.strip() for th in table.find_all("th")]
            for tr in table.find_all("tr")[1:]:
                cells = [td.text.strip() for td in tr.find_all("td")]
                if not cells:
                    continue
                row = dict(zip(hdrs, cells))
                ratio_str = row.get("Ratio") or row.get("Split Ratio", "")
                ratio = parse_split_ratio(ratio_str)
                raw_date = row.get("Date") or row.get("Split Date", "")
                row["is_reverse"]  = ratio is not None
                row["split_ratio"] = ratio
                row["split_date"]  = normalize_date(raw_date)
                row["list_type"]   = label
                rows.append(row)
            time.sleep(0.3)
        except Exception as e:
            print(f"    Hata ({label}): {e}")
    save("splits.json", rows, min_records=1)
    return rows


# ══════════════════════════════════════════════
# 4. FINVIZ — Insider Alım/Satım
# ══════════════════════════════════════════════
def fetch_insider() -> list:
    print("\n[4/7] Finviz insider işlemleri çekiliyor...")
    url = "https://finviz.com/insidertrading?tc=1"
    try:
        r = requests.get(url, headers={**HEADERS, "Referer": "https://finviz.com/"}, timeout=30)
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
        save("insider.json", rows, min_records=5)
        return rows
    except Exception as e:
        print(f"    Hata: {e}")
        save("insider.json", [], min_records=5)
        return []


# ══════════════════════════════════════════════
# 5. SEC EDGAR — S-1 / S-1/A başvuruları (son 30 gün)
# ══════════════════════════════════════════════
def fetch_sec_s1() -> list:
    print("\n[5/7] SEC EDGAR S1 başvuruları çekiliyor...")
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
            for hit in r.json().get("hits", {}).get("hits", []):
                src  = hit.get("_source", {})
                uid  = src.get("entity_name","") + src.get("file_date","")
                if uid in seen:
                    continue
                seen.add(uid)
                ticker = src.get("ticker","")
                rows.append({
                    "form":       form_type,
                    "ticker":     ticker,
                    "company":    src.get("entity_name",""),
                    "filed_date": src.get("file_date",""),
                    "edgar_url":  (
                        "https://www.sec.gov/cgi-bin/browse-edgar"
                        "?action=getcompany&company="
                        + requests.utils.quote(src.get("entity_name",""))
                        + "&type=S-1&dateb=&owner=include&count=10"
                    ),
                })
            time.sleep(0.3)
        except Exception as e:
            print(f"    EDGAR {form_type} hatası: {e}")

    print(f"    {len(rows)} başvuru bulundu (son 30 gün)")
    save("s1_edgar.json", rows, min_records=1)
    return rows


# ══════════════════════════════════════════════
# 6. FINRA — Short Interest (ayda 2 kez)
# ══════════════════════════════════════════════
def fetch_finra_short_interest() -> dict:
    print("\n[6/7] FINRA short interest çekiliyor...")
    result    = {}
    prefixes  = ["FNSQ", "FNYS", "FNOQ"]   # NASDAQ, NYSE, OTC
    today     = datetime.now()

    for prefix in prefixes:
        found = False
        for days_back in range(0, 50):
            d_str = (today - timedelta(days=days_back)).strftime("%Y%m%d")
            url   = f"https://cdn.finra.org/equity/regsho/biweekly/{prefix}{d_str}.txt"
            try:
                rh = requests.head(url, headers=HEADERS, timeout=6)
                if rh.status_code != 200:
                    continue
                # Dosya bulundu, indir
                exchange = {"FNSQ":"NASDAQ","FNYS":"NYSE","FNOQ":"OTC"}.get(prefix, prefix)
                print(f"    {exchange}: {d_str}")
                r = requests.get(url, headers=HEADERS, timeout=30)
                for line in r.text.strip().split("\n")[1:]:
                    parts = line.strip().split("|")
                    if len(parts) < 3:
                        continue
                    ticker = parts[0].strip().upper()
                    if not ticker or ticker == "SYMBOL":
                        continue
                    try:
                        si = int(str(parts[2]).replace(",",""))
                    except:
                        continue
                    if ticker not in result or d_str > result[ticker]["si_date"]:
                        result[ticker] = {
                            "short_interest": si,
                            "si_date":        d_str,
                            "exchange":       exchange,
                        }
                found = True
                break
            except:
                continue
            time.sleep(0.05)
        if not found:
            print(f"    {prefix} — güncel dosya bulunamadı")

    print(f"    {len(result)} ticker için FINRA SI alındı")
    return result


# ══════════════════════════════════════════════
# 7. SEC EDGAR XBRL — Float + Warrant + Offering
# ══════════════════════════════════════════════
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
        return {t: mapping[t.upper()] for t in tickers if t.upper() in mapping}
    except Exception as e:
        print(f"    CIK map hatası: {e}")
        return {}


def _latest_xbrl(facts: dict, *concepts) -> tuple:
    """En güncel XBRL değerini döner: (value, filed_date, form_type)"""
    priority = {"10-K":0,"10-Q":1,"S-1":2,"S-1/A":3,"8-K":4,"10-K/A":5,"10-Q/A":6}
    for concept in concepts:
        for ns_key in ["us-gaap","dei"]:
            node = facts.get("facts",{}).get(ns_key,{}).get(concept,{})
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


def _warrant_shares(facts: dict):
    """
    Warrant hisse sayısını bulmaya çalışır.
    Şirketler farklı etiket kullanır, en yaygın 3'ünü dener.
    """
    val, _, _ = _latest_xbrl(
        facts,
        "ClassOfWarrantOrRightOutstanding",
        "WarrantsAndRightsOutstanding",
        "ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights",
    )
    return val


def fetch_edgar_floats(tickers: list, split_map: dict) -> dict:
    print(f"\n[7/7] EDGAR XBRL float çekiliyor — {len(tickers)} ticker...")
    cik_map   = _cik_map(tickers)
    print(f"    {len(cik_map)}/{len(tickers)} CIK eşlendi")
    result    = {}
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

            # Float / shares outstanding
            float_val, float_date, float_form = _latest_xbrl(
                facts,
                "CommonStockSharesOutstanding",
                "EntityCommonStockSharesOutstanding",
                "FloatShares",
                "CommonStockSharesIssued",
            )
            # Warrant overhang
            warrant_val = _warrant_shares(facts)

            # Post-split tahmini
            sp          = split_map.get(ticker, {})
            sp_ratio    = sp.get("ratio")
            sp_date     = sp.get("date","")
            edgar_dt    = (float_date or "")[:10]
            is_presplit = bool(sp_ratio and sp_date and edgar_dt and edgar_dt < sp_date)
            est_post    = int(float_val / sp_ratio) if (is_presplit and float_val and sp_ratio) else None

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
            print(f"    {ticker}: {e}")
            not_found.append(ticker)
        time.sleep(0.12)

    if not_found:
        msg = ", ".join(not_found[:10])
        if len(not_found) > 10:
            msg += f" ... +{len(not_found)-10}"
        print(f"    Bulunamayan: {msg}")

    save("floats.json", list(result.values()), min_records=1)
    print(f"    {len(result)} ticker için EDGAR float alındı")
    return result


# ══════════════════════════════════════════════
# SQUEEZE SKORU — 0-100 arası
# ══════════════════════════════════════════════
def squeeze_score(s: dict) -> tuple:
    """
    Faktörler:
      1. Short Float %   → max 25p
      2. C2B             → max 25p
      3. Float küçüklüğü → max 20p
      4. DTC             → max 15p  ← YENİ
      5. RegSHO          → max 10p
      6. SI değişimi     → max  5p
    Toplam max: 100p
    Döner: (score: int, reasons: list[str])
    """
    score, reasons = 0, []

    # 1. Short Float %
    sf = to_float(s.get("short_float_pct"))
    if sf is not None:
        if   sf >= 50: score += 25; reasons.append("SI%≥50")
        elif sf >= 30: score += 18; reasons.append("SI%≥30")
        elif sf >= 15: score += 10; reasons.append("SI%≥15")
        elif sf >=  5: score +=  4

    # 2. C2B (borrow rate %)
    c2b = to_float(s.get("c2b"))
    if c2b is not None:
        if   c2b >= 200: score += 25; reasons.append("C2B≥200%")
        elif c2b >= 100: score += 18; reasons.append("C2B≥100%")
        elif c2b >=  50: score += 12; reasons.append("C2B≥50%")
        elif c2b >=  20: score +=  6
        elif c2b >=  10: score +=  3

    # 3. Float küçüklüğü — diluted float kullan (warrant dahil)
    fl = to_float(s.get("diluted_float") or s.get("float"))
    if fl is not None:
        if   fl <   500_000: score += 20; reasons.append("Float<500K")
        elif fl < 1_000_000: score += 15; reasons.append("Float<1M")
        elif fl < 2_000_000: score += 10; reasons.append("Float<2M")
        elif fl < 5_000_000: score +=  5

    # 4. DTC (Days to Cover)
    dtc = to_float(s.get("dtc"))
    if dtc is not None:
        if   dtc >= 10: score += 15; reasons.append("DTC≥10")
        elif dtc >=  5: score += 10; reasons.append("DTC≥5")
        elif dtc >=  2: score +=  5

    # 5. RegSHO
    if s.get("reg_sho") == "✅":
        score += 10; reasons.append("RegSHO")

    # 6. SI değişimi
    si_chg = to_float(s.get("si_change"))
    if si_chg is not None:
        if   si_chg >= 50: score +=  5; reasons.append("SI+%≥50")
        elif si_chg >= 20: score +=  3; reasons.append("SI+%≥20")
        elif si_chg < -20: score -=  3  # short kapanıyorsa düşür

    return max(0, min(100, score)), reasons


# ══════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════
if __name__ == "__main__":
    run_start = datetime.now(timezone.utc)
    print(f"\n{'='*58}")
    print(f"  SHORT RADAR — {run_start.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*58}")

    ce  = fetch_chartexchange()
    rs  = fetch_regsho()
    sp  = fetch_splits()
    ins = fetch_insider()
    s1  = fetch_sec_s1()
    finra_si = fetch_finra_short_interest()

    # ── Ticker setleri ──────────────────────────
    regsho_tickers = {r.get("Symbol", r.get("Ticker","")) for r in rs}

    split_tickers = set()
    split_map     = {}   # {ticker: {ratio, date}}
    for row in sp:
        if not row.get("is_reverse"):
            continue
        t = (row.get("Symbol") or row.get("Ticker")
             or row.get("symbol") or row.get("ticker",""))
        if not t:
            continue
        split_tickers.add(t)
        if t not in split_map and row.get("split_ratio"):
            split_map[t] = {
                "ratio": row["split_ratio"],
                "date":  row.get("split_date",""),
            }

    top_c2b_tickers = set()
    for row in ce[:30]:
        t = row.get("symbol") or row.get("ticker") or row.get("Symbol","")
        if t:
            top_c2b_tickers.add(t)

    # ── Ortalama hacim haritası (Chartexchange'den) ──
    avg_vol_map = {}
    for row in ce:
        t = row.get("symbol") or row.get("ticker") or row.get("Symbol","")
        if not t:
            continue
        avg_vol = to_float(row.get("10_day_avg_vol") or row.get("tenDayAvgVol"))
        if avg_vol:
            avg_vol_map[t] = avg_vol

    # ── EDGAR Float ─────────────────────────────
    float_tickers = list(regsho_tickers | split_tickers | top_c2b_tickers)
    float_map     = fetch_edgar_floats(float_tickers, split_map)

    # FINRA SI + short float % + DTC → float_map'e ekle
    for ticker, fd in float_map.items():
        fi          = finra_si.get(ticker, {})
        si_shares   = fi.get("short_interest")
        eff_float   = fd.get("est_post_split_float") or fd.get("float_shares")
        warrant     = fd.get("warrant_shares") or 0
        # Diluted float = float + warrant (post-offering riski için)
        diluted     = int(eff_float + warrant) if eff_float else None
        sf_pct      = round(si_shares / eff_float * 100, 2) if (si_shares and eff_float) else None
        # DTC = FINRA SI ÷ 10-günlük ortalama hacim
        avg_vol     = avg_vol_map.get(ticker)
        dtc         = round(si_shares / avg_vol, 2) if (si_shares and avg_vol and avg_vol > 0) else None

        fd.update({
            "finra_si":        si_shares,
            "finra_si_date":   fi.get("si_date",""),
            "short_float_pct": sf_pct,
            "diluted_float":   diluted,
            "warrant_shares":  fd.get("warrant_shares"),
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

    # ── S1 haritası (ticker varsa) ───────────────
    s1_map = {}
    for s in s1:
        t = s.get("ticker","")
        if t:
            s1_map[t] = s

    # ── Özet tablosu ─────────────────────────────
    summary_map = {}
    for ticker, row in ce_map.items():
        fd   = float_map.get(ticker, {})
        fi   = finra_si.get(ticker, {})
        s1r  = s1_map.get(ticker, {})
        eff_float = fd.get("effective_float") or to_float(row.get("shares_float"))

        # Short float %: FINRA öncelikli, yoksa Chartexchange
        sf_pct = fd.get("short_float_pct") or to_float(row.get("shortint_pct"))

        # DTC
        dtc    = fd.get("dtc")

        rec = {
            "ticker":               ticker,
            # C2B
            "c2b":                  to_float(row.get("borrow_fee_rate_ib") or row.get("borrowFeeRateIb")),
            "shares_avail":         to_float(row.get("borrow_fee_avail_ib")),
            # Float
            "float":                fd.get("est_post_split_float") or fd.get("float_shares") or to_float(row.get("shares_float")),
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
            # Short volume (günlük Chartexchange)
            "short_vol_pct":        to_float(row.get("shortvol_all_short_pct")),
            "short_vol_30d_pct":    to_float(row.get("shortvol_all_short_pct_30d")),
            # DTC
            "dtc":                  dtc,
            "avg_vol_10d":          avg_vol_map.get(ticker),
            # Fiyat
            "price":                to_float(row.get("reg_price")),
            "change_pct":           to_float(row.get("reg_change_pct")),
            "pre_price":            to_float(row.get("pre_price")),
            "pre_change":           to_float(row.get("pre_change_pct")),
            # Flags
            "reg_sho":              "✅" if ticker in regsho_tickers else "❌",
            "has_split":            "✅" if ticker in split_tickers   else "-",
            "split_ratio":          split_map.get(ticker, {}).get("ratio"),
            "split_date":           split_map.get(ticker, {}).get("date",""),
            # S1
            "s1_date":              s1r.get("filed_date",""),
            "s1_form":              s1r.get("form",""),
            # Offering sonrası uyarı: S-1/A varsa float artmış olabilir
            "offering_warning":     bool(s1r and s1r.get("form") == "S-1/A"),
        }
        # Squeeze skoru
        sc, reasons = squeeze_score(rec)
        rec["squeeze_score"]   = sc
        rec["squeeze_reasons"] = ", ".join(reasons)
        summary_map[ticker] = rec

    # S1 kayıtlarını zenginleştir
    for s in s1:
        t  = s.get("ticker","")
        fd = float_map.get(t, {})
        s["float"]          = fd.get("est_post_split_float") or fd.get("float_shares")
        s["diluted_float"]  = fd.get("diluted_float")
        s["float_date"]     = fd.get("float_date","")
        s["float_form"]     = fd.get("float_form","")
        s["short_float_pct"]= fd.get("short_float_pct")
        s["reg_sho"]        = "✅" if t in regsho_tickers else "❌"
        s["in_summary"]     = t in summary_map

    run_end = datetime.now(timezone.utc)
    elapsed = round((run_end - run_start).total_seconds())

    # save() returns False when old file is preserved (fetch returned too few records)
    results = {
        "summary":  save("summary.json", list(summary_map.values()), min_records=10),
        "regsho_t": save("regsho_tickers.json", list(regsho_tickers), min_records=1),
        "s1":       save("s1_edgar.json", s1, min_records=1),
    }
    critical_ok = results["summary"]  # summary yazılamazsa scraper_ok=False

    save_meta({
        "updated_at":      run_end.isoformat(),
        "elapsed_sec":     elapsed,
        "scraper_ok":      critical_ok,
        "protected_files": [k for k, v in results.items() if not v],
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

    estimated = sum(1 for f in float_map.values() if f.get("est_post_split_float"))
    warned    = sum(1 for s in summary_map.values() if s.get("offering_warning"))
    print(f"\n{'='*58}")
    print(f"  ✅  Tamamlandı ({elapsed}s)")
    print(f"  C2B        : {len(ce)} ticker")
    print(f"  RegSHO     : {len(rs)} ticker")
    print(f"  Rev.Split  : {sum(1 for x in sp if x.get('is_reverse'))} adet")
    print(f"  S1 (EDGAR) : {len(s1)} başvuru")
    print(f"  Float      : {len(float_map)} ticker (EDGAR XBRL)")
    print(f"  FINRA SI   : {len(finra_si)} ticker")
    if estimated: print(f"  RS Tahmin  : {estimated} ticker")
    if warned:    print(f"  Offering ⚠ : {warned} ticker (S-1/A var)")
    print(f"{'='*58}\n")
