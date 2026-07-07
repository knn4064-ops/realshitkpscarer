#!/usr/bin/env python3
"""
KupujemProdajem — leads scraper (v2, citanje iz __NEXT_DATA__ JSON-a).
Testirano na stvarnim stranicama KP-a.

Kolone u leads.csv: ime, link, broj_ocena, aktivnih_oglasa, kategorija.
(Bez telefona — KP ga krije iza zasticenog poziva na klik; telefon se vadi
rucno klikom na link ili kasnije ako se uhvati taj poziv.)

KAKO RADI (efikasno):
  1) PRONALAZENJE: prolazi kroz sve kategorije+podkategorije. Svaka strana
     liste u __NEXT_DATA__ nosi 30 oglasa sa userId — bez otvaranja oglasa.
     Broji koliko se puta koji userId pojavi (~ broj aktivnih oglasa) i pamti
     primarnu kategoriju.
  2) PROVERA: samo za prodavce koji se pojave >= CANDIDATE_MIN puta otvara
     profil (1 zahtev) -> ime, tacne ocene, tacan broj oglasa. Filter 50+/20+.

Sharding (SHARD_ID/SHARD_COUNT) za 20 paralelnih GitHub poslova.
Vremenski limit (MAX_RUNTIME_SECONDS) da cisto stane pre 6h.
"""

import asyncio, csv, json, os, re, sys, time
from pathlib import Path
from curl_cffi.requests import AsyncSession

BASE = "https://www.kupujemprodajem.com"
MIN_RATINGS      = int(os.getenv("MIN_RATINGS", "50"))
MIN_ACTIVE_ADS   = int(os.getenv("MIN_ACTIVE_ADS", "20"))
CANDIDATE_MIN    = int(os.getenv("CANDIDATE_MIN", "5"))   # koliko puta da se pojavi da otvorimo profil
CONCURRENCY      = int(os.getenv("CONCURRENCY", "6"))
REQUEST_TIMEOUT  = float(os.getenv("REQUEST_TIMEOUT", "30"))
MAX_RUNTIME      = int(os.getenv("MAX_RUNTIME_SECONDS", "0"))   # 0 = neograniceno
SHARD_ID         = int(os.getenv("SHARD_ID", "0"))
SHARD_COUNT      = int(os.getenv("SHARD_COUNT", "1"))
START = time.time()

OUTPUT_CSV = Path("leads.csv")
EXTRA_HEADERS = {"Accept-Language": "sr-RS,sr;q=0.9,en;q=0.8"}

# Rucni popis (baze BEZ broja strane). Prazno = automatsko otkrivanje.
CATEGORIES = []

CAT_LINK_RE   = re.compile(r'/[a-z0-9\-]+/kategorija/\d+')
GROUP_BASE_RE = re.compile(r'/[a-z0-9\-]+/[a-z0-9\-]+/grupa/\d+/\d+')
SLUG_RE       = re.compile(r'/([a-z0-9\-]+)/svi-oglasi/(\d+)')


def past_deadline():
    return MAX_RUNTIME and (time.time() - START) > MAX_RUNTIME

def toi(x):
    d = re.sub(r"[^\d]", "", str(x or "")); return int(d) if d else 0

def next_data(html):
    if not html: return None
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
    if not m: return None
    try: return json.loads(m.group(1))
    except Exception: return None

def parse_listing(html):
    """-> (list_of_(userId, adUrl, category), total_pages)"""
    d = next_data(html)
    if not d: return [], 0
    rs = d.get("props", {}).get("initialReduxState", {})
    s = rs.get("search", {})
    pages = int(s.get("pages") or 0)
    def from_byid(byId):
        out = []
        for adId, ad in (byId or {}).items():
            if not isinstance(ad, dict): continue
            uid = ad.get("userId")
            if uid in (None, "", 0): continue
            out.append((str(uid), ad.get("adUrl") or "",
                        ad.get("groupName") or ad.get("categoryName") or ""))
        return out
    out = from_byid(s.get("byId"))
    if not out:
        out = from_byid(rs.get("adNavigation", {}).get("byId"))
    if not out:
        for a in (s.get("lastSearchResult", {}).get("ads") or []):
            uid = a.get("user_id") or a.get("userId")
            if uid in (None, "", 0): continue
            out.append((str(uid), a.get("ad_url") or a.get("adUrl") or "",
                        a.get("group_name") or a.get("category_name") or ""))
        if not pages:
            pages = int(s.get("lastSearchResult", {}).get("pages") or 0)
    return out, pages

def parse_profile(html):
    d = next_data(html)
    if not d: return None
    rs = d.get("props", {}).get("initialReduxState", {})
    su = rs.get("user", {}).get("summary")
    if not su: return None
    return dict(
        username=su.get("username", "") or "",
        userId=str(su.get("userId", "") or ""),
        reviews=toi(su.get("reviewsPositive")),
        activeAds=toi(su.get("userActiveAdCount")),
        link=rs.get("meta", {}).get("pageUrl", "") or "",
    )


# ------------------------------- HTTP --------------------------------
async def fetch(session, url, retries=4):
    for attempt in range(retries):
        try:
            r = await session.get(url)
        except Exception:
            await asyncio.sleep(2 ** attempt + 0.5); continue
        if r.status_code == 200:
            t = r.text
            if "zastareli" in t[:3000].lower():
                print("[!] 'zastareli pretrazivac' — proveri.", file=sys.stderr)
            return t
        if r.status_code in (403, 429, 503):
            if attempt == retries - 1:
                print(f"[!] HTTP {r.status_code} na {url} (blokada/limit)", file=sys.stderr)
            await asyncio.sleep(4 * (2 ** attempt)); continue
        return ""
    return ""


async def discover_bases(session):
    bases = set()
    home = await fetch(session, BASE + "/")
    bases.update(GROUP_BASE_RE.findall(home or ""))
    cat_links = sorted(set(CAT_LINK_RE.findall(home or "")))
    print(f"[i] Glavnih kategorija nadjeno: {len(cat_links)}")
    for c in cat_links:
        if past_deadline(): break
        h = await fetch(session, BASE + c)
        bases.update(GROUP_BASE_RE.findall(h or ""))
    return sorted(bases)


# ---------------------------- profil po userId -----------------------
_id_url_works = {"v": None}   # da li /-/svi-oglasi/{id}/1 radi (redirect)

async def get_profile(session, uid, sample_adurl):
    if _id_url_works["v"] in (None, True):
        html = await fetch(session, f"{BASE}/-/svi-oglasi/{uid}/1")
        p = parse_profile(html)
        if p and p["userId"] == uid:
            _id_url_works["v"] = True
            return p
        if _id_url_works["v"] is None:
            _id_url_works["v"] = False
    # fallback: oglas -> slug -> profil
    if sample_adurl:
        adhtml = await fetch(session, BASE + sample_adurl)
        m = SLUG_RE.search(adhtml or "")
        if m:
            slug, sid = m.group(1), m.group(2)
            html = await fetch(session, f"{BASE}/{slug}/svi-oglasi/{sid}/1")
            p = parse_profile(html)
            if p:
                if not p["link"]:
                    p["link"] = f"/{slug}/svi-oglasi/{sid}/1"
                return p
    return None


# ------------------------------ CSV ----------------------------------
def ensure_csv():
    if not OUTPUT_CSV.exists():
        with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["ime", "link", "broj_ocena", "aktivnih_oglasa", "kategorija"])

def append_row(row):
    with OUTPUT_CSV.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(row)


# ----------------------------- radnici -------------------------------
async def base_worker(session, base_q, tally, sample_ad):
    while True:
        base = await base_q.get()
        try:
            if past_deadline(): continue
            page = 1
            html = await fetch(session, f"{BASE}{base}/{page}")
            ads, pages = parse_listing(html)
            while True:
                for uid, adurl, cat in ads:
                    rec = tally.get(uid)
                    if rec is None:
                        tally[uid] = [1, {cat: 1}]
                        sample_ad[uid] = adurl
                    else:
                        rec[0] += 1
                        rec[1][cat] = rec[1].get(cat, 0) + 1
                if page >= pages or past_deadline(): break
                page += 1
                html = await fetch(session, f"{BASE}{base}/{page}")
                ads, _ = parse_listing(html)
                if not ads: break
        except Exception as e:
            print(f"[x] base_worker: {e}", file=sys.stderr)
        finally:
            base_q.task_done()


async def qual_worker(session, cand_q, tally, sample_ad, counters):
    while True:
        uid = await cand_q.get()
        try:
            if past_deadline(): continue
            p = await get_profile(session, uid, sample_ad.get(uid))
            counters["checked"] += 1
            if p and p["reviews"] >= MIN_RATINGS and p["activeAds"] >= MIN_ACTIVE_ADS:
                cat = max(tally[uid][1].items(), key=lambda x: x[1])[0] if tally.get(uid) else ""
                link = BASE + p["link"] if p["link"].startswith("/") else p["link"]
                append_row([p["username"] or uid, link, p["reviews"], p["activeAds"], cat])
                counters["written"] += 1
                print(f"[+] {p['username']} — ocene {p['reviews']}, oglasa {p['activeAds']}")
        except Exception as e:
            print(f"[x] qual_worker: {e}", file=sys.stderr)
        finally:
            cand_q.task_done()



async def diagnostika(session):
    print("========== DIJAGNOSTIKA ==========")
    home = await fetch(session, BASE + "/")
    print(f"[dbg] pocetna: duzina={len(home or '')}  "
          f"kategorija-linkova={len(set(CAT_LINK_RE.findall(home or '')))}  "
          f"grupa-linkova={len(set(GROUP_BASE_RE.findall(home or '')))}")
    test_base = "/mobilni-tel-oprema-i-delovi/brojevi/grupa/1017/538"
    h = await fetch(session, f"{BASE}{test_base}/1")
    nd = next_data(h)
    ads, pages = parse_listing(h)
    print(f"[dbg] test-lista: duzina={len(h or '')}  __NEXT_DATA__={'DA' if nd else 'NE'}  "
          f"oglasa_procitano={len(ads)}  strana={pages}")
    if ads: print(f"[dbg] primer: {ads[0]}")
    print("==================================")

async def main():
    ensure_csv()
    session = AsyncSession(impersonate="chrome", headers=EXTRA_HEADERS, timeout=REQUEST_TIMEOUT)
    try:
        await diagnostika(session)
        bases = CATEGORIES or await discover_bases(session)
        if SHARD_COUNT > 1:
            bases = [b for i, b in enumerate(bases) if i % SHARD_COUNT == SHARD_ID]
        if not bases:
            print("[!] Nema kategorija za obradu.", file=sys.stderr); return
        print(f"[i] Shard {SHARD_ID}/{SHARD_COUNT} | podkategorija: {len(bases)} "
              f"| prag {MIN_RATINGS}+/{MIN_ACTIVE_ADS}+ | kandidat>= {CANDIDATE_MIN} pojava")

        # ---- FAZA 1: pronalazenje ----
        tally, sample_ad = {}, {}
        base_q = asyncio.Queue()
        for b in bases: base_q.put_nowait(b)
        workers = [asyncio.create_task(base_worker(session, base_q, tally, sample_ad))
                   for _ in range(CONCURRENCY)]
        await base_q.join()
        for w in workers: w.cancel()
        print(f"[i] Faza 1 gotova: jedinstvenih prodavaca {len(tally)}")

        # ---- FAZA 2: provera kandidata ----
        candidates = [uid for uid, rec in tally.items() if rec[0] >= CANDIDATE_MIN]
        print(f"[i] Kandidata za profil (>= {CANDIDATE_MIN} oglasa): {len(candidates)}")
        cand_q = asyncio.Queue()
        for uid in candidates: cand_q.put_nowait(uid)
        counters = {"checked": 0, "written": 0}
        workers = [asyncio.create_task(qual_worker(session, cand_q, tally, sample_ad, counters))
                   for _ in range(CONCURRENCY)]
        await cand_q.join()
        for w in workers: w.cancel()
        print(f"[✓] Gotovo. Provereno {counters['checked']}, upisano {counters['written']} -> {OUTPUT_CSV}")
    finally:
        await session.close()


if __name__ == "__main__":
    asyncio.run(main())
