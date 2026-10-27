"""Lead crawler: Google Maps listings (via DataForSEO) + public business emails from their websites.

All requests need header  X-API-Key: <API_KEY env>.

  POST /maps-jobs   {"strategy": "adaptive", "category": "plumber", "states": ["TX", "GA"], "split_at": 100,
                     "depth": 200, "max_pages": 10}
                    Adaptive: one search per county; a search that returns >= split_at places is split into
                    its cities, a full city into its ZIPs. Sparse areas = 1 search, dense metros = every ZIP.
                    (or "strategy": "zip" with "tasks": [{"city": "Austin 78701", "category": ..., "state_code": "TX"}])
                    Then dedupes by CID, crawls each website for emails and pulls 1-star reviews.

  POST /jobs        {"sites": [{"id": "<anything>", "url": "https://..."}], "max_pages": 10}
                    -> {"job_id": "..."}   (emails only, for websites you already have)

  GET  /jobs/{id}        -> status + stats (no rows)
  GET  /jobs/{id}/csv           -> the finished rows as CSV (maps jobs) or JSON results (email-only jobs)
  GET  /jobs/{id}/reviews.csv   -> low-star reviews (CID, SCORE, TEXT, PICTURES, USER NAME, ...) for businesses with an email

  Maps jobs also take "reviews": {"enabled": true, "depth": 20, "keep": 5, "max_rating": 1}
  (DataForSEO reviews sorted lowest_rating; keeps up to `keep` reviews rated <= max_rating, ones with text first).
  POST /jobs/{id}/resume        -> continue a paused job now (paused jobs also retry by themselves every 10 min)
  POST /jobs/{id}/recrawl       -> finished job: re-crawl websites that gave no email (free); reviews only for new ones
  GET  /locations?state=Texas    -> {"cities": ["Austin 78701", ...]}  every active standard ZIP in the state
  GET  /health

  Maps jobs are saved to DATA_DIR. If DataForSEO rejects the key / runs out of balance / is unreachable, the job
  is marked "paused" (nothing paid for is lost). It resumes on its own every RESUME_EVERY_SECONDS, and on startup,
  so after fixing .env:  docker compose up -d  and it carries on. "job_key" (e.g. "plumber|g2") makes re-sending the
  same job return the existing one instead of paying again.
"""
import asyncio
import csv
import io
import json
import os
import re
import shutil
import time
import traceback
import uuid
from urllib.parse import urljoin, urlparse

import aiohttp
from aiohttp import web

from extract import (FREE_PROVIDERS, GUESSED_PATHS, contact_links, extract_emails, normalize_site,
                     rank_emails_typed, registrable, sitemap_links)
import mxcheck
import maps
import export as exporter

API_KEY = os.environ.get("API_KEY", "")
CONCURRENCY = int(os.environ.get("CONCURRENCY", "80"))
PAGE_TIMEOUT = float(os.environ.get("PAGE_TIMEOUT", "12"))
SITE_BUDGET = float(os.environ.get("SITE_BUDGET", "45"))
MAX_EMAILS = 5
MAX_BYTES = 2_500_000
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36")
FB_RE = re.compile(r"https?://(?:www\.|m\.)?facebook\.com/(?!sharer|share|plugins|tr\b|dialog|login|policy|privacy)[A-Za-z0-9_.\-/?=]+", re.I)

JOBS = {}
SEM = asyncio.Semaphore(CONCURRENCY)
# Two separate thread pools: page scanning (CPU) must never queue behind / block DNS lookups, which
# aiohttp also runs in threads. The default pool is only ~cpu+4 threads, far too few for 80+ open sites.
from concurrent.futures import ThreadPoolExecutor
SCAN_POOL = ThreadPoolExecutor(max_workers=max(2, min(8, (os.cpu_count() or 2))), thread_name_prefix="scan")
DNS_POOL = ThreadPoolExecutor(max_workers=max(64, CONCURRENCY), thread_name_prefix="dns")


async def scan(fn, *args):
    return await asyncio.get_running_loop().run_in_executor(SCAN_POOL, fn, *args)


# ---------------------------------------------------------------- website crawling
RETRYABLE = {"dns", "timeout", "connect", "other"}
# Third pass for sites that refused us (bot protection / rate limits): fetched again looking exactly like Chrome
# (TLS + HTTP/2 fingerprint and headers) via curl_cffi. BLOCKED_RETRY=1 turns it on.
BLOCKED_RETRY = os.environ.get("BLOCKED_RETRY", "0") in ("1", "true", "yes")  # off until tested with blocked_probe.py
BLOCKED_CONCURRENCY = int(os.environ.get("BLOCKED_CONCURRENCY", "20"))


def is_blocked(err):
    """A refusal worth retrying as a real browser: 401/403/406/429/503 and the like, not a missing page."""
    err = err or ""
    if err == "challenge":
        return True
    if err == "http_4xx":  # results saved before status codes were recorded
        return True
    if not err.startswith("http_"):
        return False
    try:
        code = int(err.split("_")[1])
    except ValueError:
        return False
    return code in (401, 403, 405, 406, 409, 418, 429, 451, 503, 520, 521, 522, 523, 524, 525, 526)


_CHALLENGE = ("<title>just a moment...</title>", "cf-browser-verification", "/cdn-cgi/challenge-platform/h/",
              "<title>attention required! | cloudflare</title>", "sucuri website firewall - access denied",
              "please enable js and disable any ad blocker")


def _is_challenge(page):
    head = page[:20000].lower()
    return len(page) < 60000 and any(m in head for m in _CHALLENGE)


def _blocker(headers):
    """Which bot protection answered, from the response headers ('' when unknown)."""
    h = {k.lower(): str(v).lower() for k, v in headers.items()}
    server = h.get("server", "")
    if "cf-ray" in h or "cloudflare" in server or "cf-mitigated" in h:
        return "cloudflare"
    if "x-sucuri-id" in h or "sucuri" in server:
        return "sucuri"
    if "akamai" in server or "x-akamai-transformed" in h:
        return "akamai"
    if "x-iinfo" in h or "incap_ses" in h.get("set-cookie", ""):
        return "imperva"
    if "x-amzn-waf-action" in h or "awselb" in server:
        return "aws"
    if "wpe-backend" in h or "wp engine" in server:
        return "wpengine"
    if "x-wix-request-id" in h or "pepyaka" in server:
        return "wix"
    if "squarespace" in server:
        return "squarespace"
    if "x-litespeed-cache" in h or "litespeed" in server:
        return "litespeed"
    if "nginx" in server:
        return "nginx"
    if "apache" in server:
        return "apache"
    return ""


def _err_kind(e):
    name = type(e).__name__
    if isinstance(e, asyncio.TimeoutError) or "Timeout" in name:
        return "timeout"
    msg = str(e)
    # curl_cffi (browser pass) errors carry curl's error number
    if "curl: (28)" in msg:
        return "timeout"
    if "curl: (6)" in msg:
        return "dns"
    if "curl: (7)" in msg:
        return "connect"
    if "curl: (35)" in msg or "curl: (60)" in msg:
        return "ssl"
    if "curl: (47)" in msg:
        return "redirects"
    if "DNS" in name or "gaierror" in str(e) or "Name or service not known" in str(e) or "Temporary failure in name resolution" in str(e):
        return "dns"
    if isinstance(e, aiohttp.ClientConnectorError) or "Connect" in name or isinstance(e, ConnectionError):
        return "connect"
    if "SSL" in name or "Certificate" in name:
        return "ssl"
    if isinstance(e, aiohttp.TooManyRedirects):
        return "redirects"
    return "other"


class BrowserSession:
    """Fetches the way real Chrome does (TLS and HTTP/2 fingerprint plus headers), for sites that refuse
    ordinary clients. Only used for the blocked-sites pass, so most crawling stays on fast aiohttp."""

    def __init__(self, concurrency, timeout):
        from curl_cffi.requests import AsyncSession
        self.s = AsyncSession(impersonate="chrome", max_clients=concurrency, timeout=timeout, verify=False,
                              allow_redirects=True, max_redirects=6)

    async def fetch_ex(self, url):
        try:
            async with self.s.stream("GET", url) as r:
                if r.status_code >= 400:
                    b = _blocker(r.headers)
                    return None, str(r.url), f"http_{r.status_code}" + (f"@{b}" if b else "")
                ctype = r.headers.get("content-type", "") or ""
                if ctype and not any(t in ctype for t in ("html", "text", "xml")):
                    return None, str(r.url), "not_html"
                buf = bytearray()
                async for chunk in r.aiter_content():
                    buf += chunk
                    if len(buf) >= MAX_BYTES:
                        break
                m = re.search(r"charset=([\w-]+)", ctype)
                enc = m.group(1) if m else "utf-8"
                try:
                    return bytes(buf[:MAX_BYTES]).decode(enc, errors="replace"), str(r.url), ""
                except LookupError:
                    return bytes(buf[:MAX_BYTES]).decode("utf-8", errors="replace"), str(r.url), ""
        except Exception as e:
            return None, url, _err_kind(e)

    async def close(self):
        await self.s.close()


async def fetch_ex(session, url):
    """(body or None, final_url, error_kind or ''). A refusal comes back as e.g. 'http_403@cloudflare'."""
    if isinstance(session, BrowserSession):
        return await session.fetch_ex(url)
    try:
        async with session.get(url, allow_redirects=True, max_redirects=6, ssl=False) as r:
            ctype = r.headers.get("content-type", "")
            if r.status >= 400:
                b = _blocker(r.headers)
                return None, str(r.url), f"http_{r.status}" + (f"@{b}" if b else "")
            if ctype and not any(t in ctype for t in ("html", "text", "xml")):
                return None, str(r.url), "not_html"
            body = await r.content.read(MAX_BYTES)
            return body.decode(r.charset or "utf-8", errors="replace"), str(r.url), ""
    except Exception as e:
        return None, url, _err_kind(e)


async def fetch(session, url):
    body, final, _ = await fetch_ex(session, url)
    return body, final


async def crawl_site(session, site, max_pages, sem=None):
    sid, raw_url = site.get("id", ""), site.get("url", "")
    res = {"id": sid, "url": raw_url, "emails": [], "types": [], "facebook": "", "pages": 0, "error": "", "blocker": ""}
    home = normalize_site(raw_url)
    if not home:
        res["error"] = "skipped"
        return res
    async with (sem or SEM):
        # the time budget starts once this site gets a crawl slot, not while it waits in line behind others
        started = time.monotonic()
        page, final, err = await fetch_ex(session, home)
        if page is None:
            alt = "https://" + home[7:] if home.startswith("http://") else "http://" + home[8:]
            page, final, err2 = await fetch_ex(session, alt)
            err = err2 if err in ("", "other") else err
        if page is None:
            res["error"], _, res["blocker"] = (err or "unreachable").partition("@")
            return res
        if _is_challenge(page):
            # a bot check ("Just a moment...") served with a 200: there is no real page to read
            res["error"], res["blocker"] = "challenge", "cloudflare" if "cloudflare" in page.lower() else ""
            return res
        res["pages"] = 1
        # page scanning is CPU work: run it off the event loop so status checks from n8n stay instant
        emails = await scan(extract_emails, page)
        fb = FB_RE.search(page)
        res["facebook"] = fb.group(0).split("?")[0] if fb else ""

        root = "{0.scheme}://{0.netloc}".format(urlparse(final))
        queue = await scan(contact_links, page, final)
        # Google's own "contact" link for the listing goes first, if it's on the same site
        cu = site.get("contact_url") or ""
        if cu and registrable(urlparse(cu).hostname or "") == registrable(urlparse(final).hostname or ""):
            queue = [cu] + [u for u in queue if u.rstrip("/") != cu.rstrip("/")]
        if len(queue) < 2 and max_pages > 1:
            # menus built by JavaScript hide their links; the sitemap still lists the contact/about pages
            sm, _ = await fetch(session, urljoin(root, "/sitemap.xml"))
            if sm and "<loc>" in sm.lower():
                pages, children = sitemap_links(sm, final)
                if not pages and children:
                    sub_sm, _ = await fetch(session, children[0])
                    pages, _ = sitemap_links(sub_sm or "", final)
                queue += [u for u in pages if u not in queue]
        for g in GUESSED_PATHS:
            u = urljoin(root, g)
            if u not in queue:
                queue.append(u)
        visited = {final.rstrip("/"), home.rstrip("/")}
        site_reg = registrable(urlparse(final).hostname or "")
        for u in queue:
            if res["pages"] >= max_pages or time.monotonic() - started > SITE_BUDGET:
                break
            # once we have an on-domain email and have checked at least one more page, stop early
            if res["pages"] >= 3 and any(registrable(e.split("@")[1]) == site_reg for e in emails):
                break
            if u.rstrip("/") in visited:
                continue
            visited.add(u.rstrip("/"))
            sub, _ = await fetch(session, u)
            if sub is None:
                continue
            res["pages"] += 1
            for e in await scan(extract_emails, sub):
                if e not in emails:
                    emails.append(e)
        typed = rank_emails_typed(emails, final)[:MAX_EMAILS]
        res["emails"] = [e for e, _ in typed]
        res["types"] = [t for _, t in typed]
    return res


async def crawl_many(sites, max_pages, on_done=None, concurrency=None, page_timeout=None, browser=False):
    """concurrency/page_timeout given -> a gentler pass with its own limit (used to retry failed sites).
    browser=True -> fetch as real Chrome (blocked-sites pass)."""
    sem = asyncio.Semaphore(concurrency) if concurrency else None
    results = []
    if browser:
        session = BrowserSession(concurrency or BLOCKED_CONCURRENCY, page_timeout or RETRY_TIMEOUT)
        try:
            await _run_pool(session, sites, max_pages, on_done, sem, concurrency, results)
        finally:
            await session.close()
        return results
    timeout = aiohttp.ClientTimeout(total=page_timeout or PAGE_TIMEOUT, connect=min(15, (page_timeout or PAGE_TIMEOUT)))
    conn = aiohttp.TCPConnector(limit=(concurrency or CONCURRENCY) * 2, ttl_dns_cache=3600, ssl=False)
    headers = {"User-Agent": UA, "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8", "Accept-Language": "en-US,en;q=0.9"}
    async with aiohttp.ClientSession(timeout=timeout, connector=conn, headers=headers) as session:
        await _run_pool(session, sites, max_pages, on_done, sem, concurrency, results)
    return results


async def _run_pool(session, sites, max_pages, on_done, sem, concurrency, results):
    async def one(s):
        try:
            r = await crawl_site(session, s, max_pages, sem)
        except Exception as e:  # one bad site never kills the job
            r = {"id": s.get("id", ""), "url": s.get("url", ""), "emails": [], "facebook": "", "pages": 0, "error": str(e)[:200]}
        results.append(r)
        if on_done:
            on_done(r)
    # a fixed pool of workers pulling from the list, instead of one task per site: an 18,000-site
    # job no longer creates 18,000 tasks (and their memory) up front
    queue = list(sites)
    queue.reverse()

    async def worker():
        while queue:
            await one(queue.pop())
    await asyncio.gather(*(worker() for _ in range(min(len(queue), concurrency or CONCURRENCY) or 1)))


# ---------------------------------------------------------------- job persistence
# Every maps job is saved to DATA_DIR as JSON, so a pause (bad DataForSEO key / no balance) or a container
# restart never loses paid-for work. Paused and interrupted jobs resume automatically.
DATA_DIR = os.environ.get("DATA_DIR", "/data")
RESUME_EVERY = int(os.environ.get("RESUME_EVERY_SECONDS", "600"))
KEEP_DAYS = 7
_last_save = {}


def job_path(job_id):
    return os.path.join(DATA_DIR, f"job-{job_id}.json")


def save_job(job, force=False):
    if job.get("_light"):
        return  # never overwrite the full file with the lightened in-memory copy
    now = time.monotonic()
    if not force and now - _last_save.get(job["id"], 0) < 60:
        return
    _last_save[job["id"]] = now
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        tmp = job_path(job["id"]) + ".tmp"
        with open(tmp, "w") as f:
            json.dump(job, f)
        os.replace(tmp, job_path(job["id"]))
    except Exception:
        traceback.print_exc()


# Big per-job data. A finished job keeps only its status and stats in memory; the rest stays on disk and is
# read back when a file is downloaded or a re-crawl starts. (Holding every finished job's full data in memory
# is what pushed the crawler past its memory limit on the first full-state run.)
HEAVY = ("rows", "reviews", "results", "crawl_results", "maps_rows", "plan", "node_counts", "searched_kw",
         "deep_done", "frontier", "deep_round")


def lighten(job):
    if job.get("_light") or job.get("status") != "done":
        return
    job["_counts"] = {"rows": len(job.get("rows") or []), "reviews": len(job.get("reviews") or [])}
    for k in HEAVY:
        job.pop(k, None)
    job["_light"] = True


def full_job(job):
    """The job with all its data (a copy read from disk when the in-memory one was lightened)."""
    if not job.get("_light"):
        return job
    with open(job_path(job["id"])) as f:
        return json.load(f)


def _count(job, what):
    return job["_counts"][what] if job.get("_light") else len(job.get(what) or [])


def load_jobs():
    if not os.path.isdir(DATA_DIR):
        return
    for name in os.listdir(DATA_DIR):
        if not (name.startswith("job-") and name.endswith(".json")):
            continue
        try:
            with open(os.path.join(DATA_DIR, name)) as f:
                job = json.load(f)
            lighten(job)
            JOBS[job["id"]] = job
        except Exception:
            traceback.print_exc()


# ---------------------------------------------------------------- jobs
def new_job(kind, **extra):
    job_id = uuid.uuid4().hex
    JOBS[job_id] = {"id": job_id, "kind": kind, "status": "running", "phase": "queued", "created_at": time.time(),
                    "active_s": 0.0, "stats": {}, "rows": [], "reviews": [], "results": [], "error": "",
                    "pause_reason": "", "paused_at": None, **extra}
    return job_id, JOBS[job_id]


async def run_email_job(job):
    p = job["params"]
    job["phase"] = "emails"
    job["stats"] = {"sites": len(p["sites"]), "crawled": 0}

    def tick(_r):
        job["stats"]["crawled"] += 1
    job["results"] = await crawl_many(p["sites"], p["max_pages"], tick)
    job["stats"]["with_email"] = sum(1 for r in job["results"] if r["emails"])


AGENCY_MIN_SITES = int(os.environ.get("AGENCY_MIN_SITES", "3"))
RETRY_CONCURRENCY = int(os.environ.get("RETRY_CONCURRENCY", "25"))
RETRY_TIMEOUT = float(os.environ.get("RETRY_TIMEOUT", "25"))


def _blocker_counts(done):
    """Refusals by who refused us, e.g. {"cloudflare": 812, "": 301}."""
    c = {}
    for v in done.values():
        if is_blocked(v.get("error")):
            k = v.get("blocker") or "unknown"
            c[k] = c.get(k, 0) + 1
    return c


def _error_counts(done):
    c = {}
    for v in done.values():
        k = v.get("error") or ("ok_with_email" if v["emails"] else "ok_no_email")
        c[k] = c.get(k, 0) + 1
    return c


async def clean_job_emails(done):
    """Job-wide cleanup after crawling (also re-applies the current rules to results crawled by older code):
    1. clean + dedupe every address
    2. an address, or a non-free domain that isn't the site's own, showing up on AGENCY_MIN_SITES+ different
       websites is a web-design agency / theme / template footer, not the business: drop it
    3. drop emails whose domain can't receive mail (no MX and no A record), so MillionVerifier credits aren't wasted
    4. re-rank (personal > role > free > low > other; other-domain only kept when nothing better exists)"""
    from extract import clean_email
    for res in done.values():
        seen, cleaned = set(), []
        for e in res["emails"]:
            c = clean_email(e)
            if c and c not in seen:
                seen.add(c)
                cleaned.append(c)
        res["emails"] = cleaned
    by_email, by_domain = {}, {}
    for site, res in done.items():
        site_reg = site.split("/")[0]
        for e in res["emails"]:
            by_email.setdefault(e, set()).add(site)
            dom = e.split("@")[1]
            if dom not in FREE_PROVIDERS and registrable(dom) != site_reg:
                by_domain.setdefault(registrable(dom), set()).add(site)
    agency_emails = {e for e, sites in by_email.items() if len(sites) >= AGENCY_MIN_SITES}
    agency_domains = {d for d, sites in by_domain.items() if len(sites) >= AGENCY_MIN_SITES}
    domains = {e.split("@")[1] for res in done.values() for e in res["emails"]}
    dead = await mxcheck.dead_domains(domains)
    dropped = {"agency_or_template": 0, "dead_domain": 0}
    for site, res in done.items():
        site_reg = site.split("/")[0]
        keep = []
        for e in res["emails"]:
            dom = e.split("@")[1]
            if e in agency_emails or (registrable(dom) in agency_domains and registrable(dom) != site_reg):
                dropped["agency_or_template"] += 1
                continue
            if dom in dead:
                dropped["dead_domain"] += 1
                continue
            keep.append(e)
        typed = rank_emails_typed(keep, "http://" + site_reg)
        res["emails"], res["types"] = [e for e, _ in typed], [t for _, t in typed]
    dropped["agency_domains"] = sorted(agency_domains)[:50]
    return dropped


def _legacy_site_key(url):
    """Site key as older versions computed it (hosted subdomains collapsed), so jobs crawled before an
    upgrade still match their saved results."""
    home = normalize_site(url)
    if not home:
        return ""
    u = urlparse(home)
    parts = (u.hostname or "").lower().removeprefix("www.").split(".")
    reg = ".".join(parts[-3:]) if len(parts) >= 3 and len(parts[-2]) <= 3 and len(parts[-1]) == 2 else ".".join(parts[-2:])
    return reg + u.path.rstrip("/")


def _site_key(url):
    home = normalize_site(url)
    if not home:
        return ""
    u = urlparse(home)
    return registrable(u.hostname or "") + u.path.rstrip("/")


async def run_maps_job(job):
    """Resumable: each stage records what it finished, so calling this again continues where it stopped."""
    p = job["params"]
    save = lambda force=False: save_job(job, force)  # noqa: E731

    # 1) DataForSEO Maps
    if not job.get("maps_done"):
        job["phase"] = "maps"
        if p.get("strategy") == "adaptive":
            await run_adaptive_maps(job, save)
        else:
            if "maps_state" not in job:
                job["maps_state"] = maps.new_stage(maps.maps_tasks(p["tasks"], p["depth"], p["language_code"]))
                job["maps_rows"] = {}
            await maps.run_maps_stage(job["maps_state"], job["maps_rows"], save)
        job["maps_done"] = True
        save(True)

    # rows come from maps_rows the first time; afterwards maps_rows is emptied and job["rows"] is the source
    # (one copy of the list in memory and on disk instead of two)
    from_maps = bool(job.get("maps_rows"))
    rows = list(job["maps_rows"].values()) if from_maps else list(job.get("rows") or [])
    # Google pads thin areas with businesses from other states (national chains, telehealth, mobile services).
    # Those belong to another group's run, so drop them here, before paying for their reviews.
    wanted = {str(s).upper() for s in p.get("states") or []}
    if wanted:
        keep = [r for r in rows if not r.get("STATE CODE") or str(r["STATE CODE"]).upper() in wanted]
        if from_maps:
            job["stats"]["dropped_outside_states"] = len(rows) - len(keep)
        rows = keep

    # 2) website emails (free, but still remembered so a restart doesn't redo finished sites)
    if not job.get("emails_done"):
        job["phase"] = "emails"
        by_site = {}
        for r in rows:
            k = _site_key(r["WEBSITE"])
            if k:
                e = by_site.setdefault(k, {"url": r["WEBSITE"], "n": 0, "contact_url": ""})
                e["n"] += 1
                e["contact_url"] = e["contact_url"] or r.get("CONTACT URL", "")
        done = job.setdefault("crawl_results", {})
        est = job["stats"].setdefault("emails", {})
        est.update({"listings": len(rows), "with_website": sum(v["n"] for v in by_site.values()),
                    "unique_sites": len(by_site), "crawled": len(done)})

        t0, n0 = time.time(), len(done)

        def tick(r):
            done[r["id"]] = {"emails": r["emails"], "types": r.get("types", []), "facebook": r["facebook"],
                             "error": r.get("error", ""), "pages": r.get("pages", 0), "blocker": r.get("blocker", "")}
            est["crawled"] = len(done)
            est["with_email_so_far"] = sum(1 for v in done.values() if v["emails"])
            mins = max((time.time() - t0) / 60, 0.1)
            est["sites_per_min"] = round((len(done) - n0) / mins, 1)
            save()
        todo = [{"id": k, "url": v["url"], "contact_url": v["contact_url"]} for k, v in by_site.items() if k not in done]
        await crawl_many(todo, p["max_pages"], tick)
        est["errors_first_pass"] = _error_counts(done)
        # second, gentler pass for sites that failed on a network problem (DNS / timeout / connection):
        # busy moments on the VPS or a slow host shouldn't cost us the email
        retry = [{"id": k, "url": by_site[k]["url"], "contact_url": by_site[k]["contact_url"]}
                 for k, v in done.items() if k in by_site and v.get("error") in RETRYABLE]
        if retry:
            job["phase"] = "emails (retrying failed sites)"
            est["retrying"] = len(retry)
            before = sum(1 for v in done.values() if v["emails"])
            await crawl_many(retry, p["max_pages"], tick, concurrency=RETRY_CONCURRENCY, page_timeout=RETRY_TIMEOUT)
            est["recovered_by_retry"] = sum(1 for v in done.values() if v["emails"]) - before
            job["phase"] = "emails"
        # third pass: sites that refused us (bot protection, rate limits) get one more try looking like Chrome
        blocked = [{"id": k, "url": by_site[k]["url"], "contact_url": by_site[k]["contact_url"]}
                   for k, v in done.items() if k in by_site and is_blocked(v.get("error"))]
        if blocked and BLOCKED_RETRY:
            job["phase"] = "emails (retrying blocked sites)"
            est["blocked_retrying"] = len(blocked)
            before = sum(1 for v in done.values() if v["emails"])
            loaded_before = sum(1 for v in done.values() if not v.get("error"))
            await crawl_many(blocked, p["max_pages"], tick, concurrency=BLOCKED_CONCURRENCY,
                             page_timeout=RETRY_TIMEOUT, browser=True)
            est["blocked_unlocked"] = sum(1 for v in done.values() if not v.get("error")) - loaded_before
            est["recovered_from_blocked"] = sum(1 for v in done.values() if v["emails"]) - before
            job["phase"] = "emails"
        est["errors"] = _error_counts(done)
        est["blockers"] = _blocker_counts(done)
        job["email_filter"] = await clean_job_emails(done)
        job["emails_done"] = True
        save(True)

    done = job.get("crawl_results", {})
    for r in rows:
        res = done.get(_site_key(r["WEBSITE"])) or done.get(_legacy_site_key(r["WEBSITE"]))
        r["EMAIL"] = ", ".join(res["emails"]) if res else ""
        r["EMAIL TYPES"] = ", ".join(res.get("types", [])) if res else ""
        r["FACEBOOK"] = res["facebook"] if res else ""
    job["stats"].setdefault("emails", {})["filtered"] = job.get("email_filter", {})
    job["stats"].setdefault("emails", {})["listings_with_email"] = sum(1 for r in rows if r["EMAIL"])
    job["rows"] = rows
    job["maps_rows"] = {}
    for k in ("plan", "node_counts", "searched_kw", "deep_done", "frontier", "deep_round"):
        job.pop(k, None)  # only needed while the Maps searches run

    # 3) DataForSEO reviews, lowest rating first, only for businesses we can email
    rv = p["reviews"]
    if rv["enabled"] and not job.get("reviews_done"):
        job["phase"] = "reviews"
        st = job.get("reviews_state")
        if st and not st["todo"] and not st["pending"]:
            # a finished review stage from before a re-crawl: start a new one for businesses not reviewed yet
            job["reviews_cost_prev"] = round(job.get("reviews_cost_prev", 0) + st["stats"].get("cost", 0), 5)
            job["reviews_kept_prev"] = job.get("reviews_kept_prev", 0) + st["stats"].get("kept", 0)
            del job["reviews_state"]
        if "reviews_state" not in job:
            # skip businesses that can't have a review at or below max_rating (no point paying for their reviews)
            def wants(r):
                return not no_low_review_possible(r, rv["max_rating"])
            reviewed = set(job.get("reviewed_cids", []))
            cids, same_biz = pick_review_cids(rows, reviewed, wants)
            job["stats"]["reviews_skipped_no_1star"] = sum(1 for r in rows if r["EMAIL"] and r["CID"] and not wants(r))
            job["stats"]["reviews_skipped_same_business"] = same_biz
            job["reviews_state"] = maps.new_stage(maps.reviews_tasks(cids, rv["depth"], p["language_code"]))
            job["reviewed_cids"] = sorted(reviewed | set(cids))
            job.setdefault("reviews", [])
        await maps.run_reviews_stage(job["reviews_state"], job["reviews"], rv["keep"], rv["max_rating"], save)
        job["reviews_done"] = True


def pick_review_cids(rows, reviewed, wants):
    """One review lookup per business: listings that share a website or an email (chain locations, duplicate
    Google listings) end up as one row per email in the final file anyway, so only the lowest-rated one is
    looked up. Listings already looked up (earlier pass) count as covering their website and emails."""
    def emails(r):
        return {e.strip().lower() for e in str(r.get("EMAIL") or "").split(",") if e.strip()}

    def rating(r):
        try:
            return float(r.get("RATING"))
        except (TypeError, ValueError):
            return 9.0
    seen_sites, seen_emails = set(), set()
    for r in rows:
        if r.get("CID") in reviewed:
            seen_sites.add(_site_key(r.get("WEBSITE") or "") or "")
            seen_emails |= emails(r)
    seen_sites.discard("")
    cands = [r for r in rows if r.get("EMAIL") and r.get("CID") and r["CID"] not in reviewed and wants(r)]
    cands.sort(key=lambda r: (rating(r), -int(r.get("REVIEWS COUNT") or 0) if str(r.get("REVIEWS COUNT") or "").isdigit() else 0))
    picked, skipped = [], 0
    for r in cands:
        site, em = _site_key(r.get("WEBSITE") or ""), emails(r)
        if r["CID"] in picked or (site and site in seen_sites) or (em & seen_emails):
            skipped += 0 if r["CID"] in picked else 1
            continue
        picked.append(r["CID"])
        if site:
            seen_sites.add(site)
        seen_emails |= em
    return picked, skipped


def no_low_review_possible(r, max_rating):
    """True when the listing can't contain a review rated <= max_rating: Google's star breakdown says so
    (1-star only), or the math does. One review of m stars caps the average at 5 - (5 - m) / n; the shown
    rating is rounded, so 0.1 of slack is left before ruling it out."""
    if max_rating <= 1 and str(r.get("ONE STAR RATINGS", "")).strip() == "0":
        return True
    try:
        n = int(str(r.get("REVIEWS COUNT", "")).strip())
    except ValueError:
        return False
    if n == 0:
        return True
    try:
        value = float(str(r.get("RATING", "")).strip())
    except ValueError:
        return False
    return value - 0.1 > 5 - (5 - max_rating) / n


async def run_adaptive_maps(job, save):
    """County -> city -> ZIP. Each round searches the current frontier; any search that came back with
    >= split_at places is split into its children for the next round. Fully resumable between rounds."""
    p = job["params"]
    if "plan" not in job:
        job["plan"] = maps.build_plan(p["states"], p["category"], p.get("test_limit", 0))
        job["maps_rows"], job["node_counts"], job["searched_kw"] = {}, {}, []
        job["frontier"], job["round"] = list(job["plan"]["roots"]), 0
        job["rounds"] = []
    plan = job["plan"]
    while job["frontier"]:
        if "maps_state" not in job:
            job["round"] += 1
            job["maps_state"] = maps.new_stage(maps.plan_round_tasks(plan, job["frontier"], p["depth"], p["language_code"],
                                                                     job.get("deep_round", [])))
            save(True)
        st = job["maps_state"]
        async with maps.session() as s:
            await maps.run_stage(s, f"{maps.DFS_BASE}/task_post", maps.DFS_BASE + "/task_get/advanced/{}", st,
                                 maps.adaptive_on_ready(plan, job["maps_rows"], job["node_counts"], st["stats"]), save)
        # decide what to split
        searched = set(job["searched_kw"]) | {plan["nodes"][n]["kw"] for n in job["frontier"]}
        deep_done = set(job.get("deep_done", [])) | set(job.get("deep_round", []))
        # an area that hit the result limit but has nothing smaller to split into (a ZIP) gets one
        # re-search at DataForSEO's max depth, so a cheap normal depth never cuts off businesses
        full_at = min(p["split_at"], int(p["depth"] * 0.9))
        nxt, deep, full = [], [], 0
        for nid in job["frontier"]:
            n = job["node_counts"].get(nid, 0)
            if n >= p["split_at"] and plan["nodes"][nid]["children"]:
                full += 1
                for c in plan["nodes"][nid]["children"]:
                    if plan["nodes"][c]["kw"] not in searched:
                        searched.add(plan["nodes"][c]["kw"])
                        nxt.append(c)
            elif n >= full_at and not plan["nodes"][nid]["children"] and nid not in deep_done and p["depth"] < maps.DEEP_DEPTH:
                deep.append(nid)
        job["rounds"].append({"round": job["round"], "searches": len(job["frontier"]), "split": full,
                              "deep_next": len(deep), "next": len(nxt) + len(deep), "cost": st["stats"]["cost"],
                              "unique_places": len(job["maps_rows"])})
        job["searched_kw"] = sorted(searched)
        job["deep_done"] = sorted(deep_done)
        job["deep_round"] = deep
        job["frontier"] = nxt + deep
        del job["maps_state"]
        save(True)


def _maps_stats(job):
    if job.get("params", {}).get("strategy") == "adaptive" and "rounds" in job:
        done = job["rounds"]
        cur = (job.get("maps_state") or {}).get("stats", {})
        return {"strategy": "adaptive", "round": job.get("round", 0),
                "searches": sum(r["searches"] for r in done) + cur.get("tasks", 0),
                "current_round": cur, "rounds": done,
                "cost": round(sum(r["cost"] for r in done) + cur.get("cost", 0), 4),
                "unique_places": len(job.get("maps_rows", {}))}
    return job.get("maps_state", {}).get("stats")


def _collect_stats(job):
    st = dict(job.get("stats") or {})
    ms = _maps_stats(job)
    if ms:
        st["maps"] = ms
    if "reviews_state" in job:
        st["reviews"] = dict(job["reviews_state"]["stats"])
        st["reviews"]["cost_total"] = round(job.get("reviews_cost_prev", 0) + st["reviews"].get("cost", 0), 5)
        st["reviews"]["kept_total"] = job.get("reviews_kept_prev", 0) + st["reviews"].get("kept", 0)
    return st


async def guard(job, runner):
    job["status"], job["error"], job["pause_reason"] = "running", "", ""
    started = job["_started"] = time.time()
    try:
        await runner(job)
        job["status"] = "done"
        job["phase"] = "done"
        job["finished_at"] = time.time()
    except maps.DFSPaused as e:
        job["status"] = "paused"
        job["pause_reason"] = str(e)
        job["paused_at"] = time.time()
        print(f"job {job['id']} paused: {e}", flush=True)
    except Exception as e:
        job["status"] = "error"
        job["phase"] = "error"
        job["error"] = f"{type(e).__name__}: {e}"
        traceback.print_exc()
    job["active_s"] = job.get("active_s", 0) + (time.time() - started)
    if job["kind"] == "maps":
        save_job(job, force=True)


RUNNING = set()
# How many maps jobs run at the same time. Extra jobs (e.g. 10 sheet rows added at once) wait as "queued"
# and start automatically, oldest first, so the VPS never has to hold 10 big crawls in memory at once.
MAX_ACTIVE_JOBS = int(os.environ.get("MAX_ACTIVE_JOBS", "2"))


def _active_maps():
    return [j for j in JOBS.values() if j["id"] in RUNNING and j["kind"] == "maps"]


def start(job):
    if job["id"] in RUNNING:
        return
    if job["kind"] == "maps" and len(_active_maps()) >= MAX_ACTIVE_JOBS:
        if job["status"] != "queued":
            job["status"], job["phase"] = "queued", "queued"
            save_job(job, force=True)
        return
    RUNNING.add(job["id"])
    runner = run_maps_job if job["kind"] == "maps" else run_email_job

    async def go():
        try:
            await guard(job, runner)
            if job["kind"] == "maps":
                lighten(job)
        finally:
            RUNNING.discard(job["id"])
            start_next()
    asyncio.get_running_loop().create_task(go())


def start_next():
    for j in sorted((j for j in JOBS.values() if j["status"] == "queued"), key=lambda j: j["created_at"]):
        if len(_active_maps()) >= MAX_ACTIVE_JOBS:
            return
        start(j)


# ---------------------------------------------------------------- http
def authed(request):
    return not API_KEY or request.headers.get("X-API-Key") == API_KEY


def deny():
    return web.json_response({"error": "unauthorized"}, status=401)


async def create_maps_job(request):
    if not authed(request):
        return deny()
    body = await request.json()
    strategy = str(body.get("strategy", "zip")).lower()
    tasks = [t for t in body.get("tasks", []) if isinstance(t, dict) and (t.get("keyword") or t.get("city"))]
    states = [STATE_NAMES.get(x, str(x).upper()) for x in (body.get("states") or [])]
    if strategy == "adaptive" and not (states and body.get("category")):
        return web.json_response({"error": "adaptive needs states and category"}, status=400)
    if strategy != "adaptive" and not tasks:
        return web.json_response({"error": "no tasks"}, status=400)
    job_key = str(body.get("job_key", "")).strip().lower()[:200]
    # same job_key (e.g. "plumber|g2") within KEEP_DAYS -> hand back that job instead of paying again
    if job_key:
        for j in sorted(JOBS.values(), key=lambda j: j["created_at"], reverse=True):
            if j.get("job_key") == job_key and j["status"] != "error" and time.time() - j["created_at"] < KEEP_DAYS * 86400:
                if j["status"] == "paused":
                    start(j)
                return web.json_response({"job_id": j["id"], "reused": True, "status": j["status"]})
    rv = body.get("reviews") or {}
    params = {
        "strategy": strategy,
        "tasks": tasks,
        "states": states,
        "category": str(body.get("category", "")).strip(),
        "split_at": max(20, min(int(body.get("split_at", 100)), 700)),
        "test_limit": max(0, int(body.get("test_limit", 0))),
        "depth": max(20, min(int(body.get("depth", 200)), 700)),
        "max_pages": max(1, min(int(body.get("max_pages", 10)), 20)),
        "language_code": body.get("language_code", "en"),
        "reviews": {"enabled": bool(rv.get("enabled", True)),
                    "depth": max(10, min(int(rv.get("depth", 20)), 200)),
                    "keep": max(1, min(int(rv.get("keep", 5)), 50)),
                    "max_rating": float(rv.get("max_rating", 1))},
    }
    job_id, job = new_job("maps", label=str(body.get("label", ""))[:200], job_key=job_key, params=params)
    save_job(job, force=True)
    start(job)
    return web.json_response({"job_id": job_id, "strategy": strategy, "reused": False})


async def create_email_job(request):
    if not authed(request):
        return deny()
    body = await request.json()
    sites = [s for s in body.get("sites", []) if isinstance(s, dict) and s.get("url")]
    job_id, job = new_job("emails", params={"sites": sites, "max_pages": max(1, min(int(body.get("max_pages", 10)), 20))})
    start(job)
    return web.json_response({"job_id": job_id, "total": len(sites)})


def get_job_or_404(request):
    job = JOBS.get(request.match_info["job_id"])
    if not job:
        return None, web.json_response({"error": "unknown job"}, status=404)
    return job, None


def _progress(job, stats):
    ph = job.get("phase")
    if job.get("status") == "queued":
        ahead = sum(1 for j in JOBS.values() if j["status"] == "queued" and j["created_at"] < job["created_at"])
        return f"queued behind {len(_active_maps())} running job(s)" + (f" and {ahead} queued" if ahead else "")
    if ph == "maps" and "maps" in stats:
        m = stats["maps"]
        if m.get("strategy") == "adaptive":
            c = m.get("current_round") or {}
            return (f"maps round {m.get('round', 0)}: {c.get('collected', 0)}/{c.get('tasks', 0)} searches, "
                    f"{m.get('unique_places', 0)} places so far")
        return f"maps {m.get('collected', 0)}/{m.get('tasks', 0)} searches"
    if ph == "emails" and "emails" in stats:
        e = stats["emails"]
        return (f"emails {e.get('crawled', 0)}/{e.get('unique_sites', 0)} sites, {e.get('with_email_so_far', 0)} with email"
                + (f", {e['sites_per_min']}/min" if e.get("sites_per_min") else ""))
    if ph == "reviews" and "reviews" in stats:
        r = stats["reviews"]
        return f"reviews {r.get('collected', 0)}/{r.get('tasks', 0)} businesses"
    return ph or ""


async def job_status(request):
    if not authed(request):
        return deny()
    job, err = get_job_or_404(request)
    if err:
        return err
    stats = _collect_stats(job)
    live = job["id"] in RUNNING and job["status"] == "running"
    active = job.get("active_s", 0) + ((time.time() - job.get("_started", time.time())) if live else 0)
    return web.json_response({
        "kind": job["kind"], "status": job["status"], "phase": job["phase"], "progress": _progress(job, stats),
        "error": job["error"], "pause_reason": job.get("pause_reason", ""), "stats": stats,
        "rows": _count(job, "rows"), "review_rows": _count(job, "reviews"),
        "elapsed_s": round((job.get("finished_at") or time.time()) - job["created_at"]), "active_s": round(active),
        "crawler_mem_mb": _rss_mb()})


def _rss_mb():
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) // 1024
    except OSError:
        pass
    return None


async def resume_job(request):
    if not authed(request):
        return deny()
    job, err = get_job_or_404(request)
    if err:
        return err
    if job["status"] in ("paused", "error") and job["kind"] == "maps":
        start(job)
        return web.json_response({"resumed": True})
    return web.json_response({"resumed": False, "status": job["status"]})


async def recrawl_job(request):
    """Re-crawl every website in a finished job that gave no email (free), then look up reviews only for
    businesses that newly got an email. Maps searches and earlier reviews are not paid for again."""
    if not authed(request):
        return deny()
    job, err = get_job_or_404(request)
    if err:
        return err
    if job["kind"] != "maps" or job["status"] not in ("done", "error"):
        return web.json_response({"recrawl": False, "status": job["status"]}, status=409)
    job = full_job(job)
    JOBS[job["id"]] = job
    if "reviewed_cids" not in job:  # jobs from before this version: everything with an email was reviewed
        job["reviewed_cids"] = sorted({r["CID"] for r in job.get("rows", []) if r.get("EMAIL") and r.get("CID")})
    done = job.get("crawl_results", {})
    redo = [k for k, v in done.items() if not v["emails"] and v.get("error") != "skipped"]
    for k in redo:
        done.pop(k, None)
    job["emails_done"] = False
    job["reviews_done"] = False
    job["status"], job["phase"] = "queued", "queued"
    job.pop("finished_at", None)
    save_job(job, force=True)
    start(job)
    return web.json_response({"recrawl": True, "sites_to_recrawl": len(redo)})


async def job_output(request):
    if not authed(request):
        return deny()
    job, err = get_job_or_404(request)
    if err:
        return err
    if job["status"] != "done":
        return web.json_response({"error": "job not finished", "status": job["status"]}, status=409)
    if job["kind"] == "emails":
        return web.json_response({"results": job["results"]})
    want_reviews = request.match_info.get("what") == "reviews"
    job = await asyncio.get_running_loop().run_in_executor(SCAN_POOL, full_job, job)
    rows = job["reviews"] if want_reviews else job["rows"]
    buf = io.StringIO()
    if want_reviews:
        cols = ["CID", "SCORE", "TEXT", "PICTURES", "USER NAME", "REVIEW DATE", "REVIEW URL"]
    else:
        cols = list(rows[0].keys()) if rows else list(maps.slim({}, "").keys())
    w = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
    return web.Response(text=buf.getvalue(), content_type="text/csv",
                        headers={"Content-Disposition": f"attachment; filename={'reviews' if want_reviews else 'maps'}.csv"})


# ---------------------------------------------------------------- final files (built here, not in n8n)
# n8n used to download the whole list and merge it in Code nodes, which runs n8n out of memory on full-state
# scrapes. Now n8n only moves the small pieces: the emails to verify, MillionVerifier's results, the business
# names to clean, and the finished files.

def _export_base(job):
    job = full_job(job)
    p = job["params"]
    codes = [str(c).upper() for c in p.get("states") or []]
    names = [k for k, v in STATE_NAMES.items() if v in set(codes)]
    fb_no, no, withe, tstats = exporter.transform(job.get("rows", []), codes, names)
    keep = int((p.get("reviews") or {}).get("keep") or 5)
    merged, mstats = exporter.merge(withe, job.get("reviews", []), keep)
    return fb_no, no, merged, {"transform": tstats, "merge": mstats}


def _done_job(request):
    job, err = get_job_or_404(request)
    if err:
        return None, err
    if job["kind"] != "maps" or job["status"] != "done":
        return None, web.json_response({"error": "job not finished", "status": job["status"]}, status=409)
    return job, None


async def export_summary(request):
    if not authed(request):
        return deny()
    job, err = _done_job(request)
    if err:
        return err
    _, _, merged, st = await asyncio.get_running_loop().run_in_executor(SCAN_POOL, _export_base, job)
    return web.json_response({"mv_emails": len(exporter.mv_emails(merged)), "rows": len(merged), **st})


async def export_mv_csv(request):
    if not authed(request):
        return deny()
    job, err = _done_job(request)
    if err:
        return err
    _, _, merged, _ = await asyncio.get_running_loop().run_in_executor(SCAN_POOL, _export_base, job)
    text = "EMAIL\n" + "\n".join(exporter.mv_emails(merged)) + "\n"
    return web.Response(text=text, content_type="text/csv",
                        headers={"Content-Disposition": f"attachment; filename={job['id']}-mv.csv"})


async def export_mv_results(request):
    """Body: MillionVerifier's result CSV (raw), or ?skip=1 when there was nothing to verify.
    Returns the business names n8n should clean with AI."""
    if not authed(request):
        return deny()
    job, err = _done_job(request)
    if err:
        return err
    skipped = request.query.get("skip") in ("1", "true")
    mv = {} if skipped else exporter.parse_mv_csv((await request.read()).decode("utf-8", errors="replace"))
    if not skipped and not mv:
        return web.json_response({"error": "MillionVerifier results had no rows"}, status=400)
    def persist():
        j = full_job(job)
        j["mv_results"], j["mv_skipped"] = mv, skipped
        if j is not job:  # lightened job: write the full copy back to disk
            with open(job_path(j["id"]) + ".tmp", "w") as f:
                json.dump(j, f)
            os.replace(job_path(j["id"]) + ".tmp", job_path(j["id"]))
    await asyncio.get_running_loop().run_in_executor(SCAN_POOL, persist)
    job["mv_results"], job["mv_skipped"] = mv, skipped
    save_job(job, force=True)
    keep = request.query.get("keep", "ok,catch_all").split(",")

    def work():
        _, _, merged, _ = _export_base(job)
        rows, counts = exporter.apply_mv(merged, mv, keep, skipped)
        return exporter.names_to_clean(rows), counts, len(rows)
    names, counts, n = await asyncio.get_running_loop().run_in_executor(SCAN_POOL, work)
    return web.json_response({"names": names, "mv_counts": counts, "rows_after_mv": n})


def _export_dir(job_id):
    return os.path.join(DATA_DIR, "exports", job_id)


async def export_build(request):
    """Body: {"names": {original: cleaned}, "base": "medspa - G5", "ts": "10-26", "keep_results": [...]}.
    Writes the final CSVs and returns their names and download paths."""
    if not authed(request):
        return deny()
    job, err = _done_job(request)
    if err:
        return err
    body = await request.json()
    if "mv_results" not in job:
        return web.json_response({"error": "send MillionVerifier results first"}, status=409)
    base, ts = str(body.get("base") or job["params"].get("label") or job["id"]), str(body.get("ts") or "")
    keep = body.get("keep_results") or ["ok", "catch_all"]
    names = body.get("names") or {}

    def work():
        fb_no, no, merged, st = _export_base(job)
        rows, counts = exporter.apply_mv(merged, job["mv_results"], keep, job.get("mv_skipped", False))
        files = exporter.final_files(fb_no, no, rows, names, base, ts)
        d = _export_dir(job["id"])
        os.makedirs(d, exist_ok=True)
        for f in os.listdir(d):
            os.remove(os.path.join(d, f))
        out = []
        for i, (name, n, text) in enumerate(files):
            fn = f"{i}.csv"
            with open(os.path.join(d, fn), "w", encoding="utf-8", newline="") as fh:
                fh.write(text)
            out.append({"fileName": name, "rows": n, "path": f"/jobs/{job['id']}/export/files/{fn}"})
        return out, counts, st
    files, counts, st = await asyncio.get_running_loop().run_in_executor(SCAN_POOL, work)
    return web.json_response({"files": files, "mv_counts": counts, **st})


async def export_file(request):
    if not authed(request):
        return deny()
    fn = request.match_info["fn"]
    if not re.fullmatch(r"\d+\.csv", fn):
        return web.json_response({"error": "bad file"}, status=400)
    path = os.path.join(_export_dir(request.match_info["job_id"]), fn)
    if not os.path.exists(path):
        return web.json_response({"error": "no such file"}, status=404)
    def read():
        with open(path, "rb") as fh:
            return fh.read()
    body = await asyncio.get_running_loop().run_in_executor(SCAN_POOL, read)
    return web.Response(body=body, content_type="text/csv", charset="utf-8")


STATE_NAMES = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA", "Colorado": "CO",
    "Connecticut": "CT", "Delaware": "DE", "District of Columbia": "DC", "Florida": "FL", "Georgia": "GA",
    "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD", "Massachusetts": "MA",
    "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS", "Missouri": "MO", "Montana": "MT",
    "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM",
    "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK",
    "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD",
    "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
}
_ZIPS = None


def state_cities(state):
    """Every active standard (street-delivery) ZIP in a state as "City 12345", the same format lobstr used."""
    global _ZIPS
    if _ZIPS is None:
        import zipcodes
        _ZIPS = [z for z in zipcodes.list_all() if z["zip_code_type"] == "STANDARD" and z["active"]]
    st = str(state or "").strip()
    code = STATE_NAMES.get(st) or next((v for k, v in STATE_NAMES.items() if k.lower() == st.lower()), st.upper())
    return sorted({f"{z['city'].title()} {z['zip_code']}" for z in _ZIPS if z["state"] == code}), code


async def locations(request):
    if not authed(request):
        return deny()
    cities, code = state_cities(request.query.get("state", ""))
    return web.json_response({"state": request.query.get("state", ""), "state_code": code, "count": len(cities), "cities": cities})


async def health(_):
    paused = [j["id"] for j in JOBS.values() if j["status"] == "paused"]
    queued = [j["id"] for j in JOBS.values() if j["status"] == "queued"]
    return web.json_response({"ok": True, "jobs": len(JOBS), "running": len(RUNNING), "paused": paused, "queued": queued,
                              "max_active_jobs": MAX_ACTIVE_JOBS,
                              "dfs_configured": bool(maps.DFS_LOGIN)})


async def background(app):
    """On startup: reload saved jobs and continue any that were running or paused.
    Then every RESUME_EVERY seconds retry paused jobs (e.g. after a DataForSEO top-up) and clean up old ones."""
    asyncio.get_running_loop().set_default_executor(DNS_POOL)
    load_jobs()
    for j in sorted(JOBS.values(), key=lambda j: j["created_at"]):
        if j["kind"] == "maps" and j["status"] in ("running", "paused", "queued"):
            if j["status"] == "running":
                j["status"] = "queued"  # interrupted by a restart: re-queue, keeping its saved progress
            start(j)

    async def loop():
        while True:
            await asyncio.sleep(RESUME_EVERY)
            now = time.time()
            for j in list(JOBS.values()):
                if j["status"] == "paused":
                    start(j)
                elif now - j["created_at"] > KEEP_DAYS * 86400 and j["id"] not in RUNNING:
                    JOBS.pop(j["id"], None)
                    try:
                        os.remove(job_path(j["id"]))
                    except OSError:
                        pass
                    shutil.rmtree(_export_dir(j["id"]), ignore_errors=True)
            start_next()
    task = asyncio.get_running_loop().create_task(loop())
    yield
    task.cancel()


def make_app():
    app = web.Application(client_max_size=50 * 1024 * 1024)
    app.router.add_post("/maps-jobs", create_maps_job)
    app.router.add_post("/jobs", create_email_job)
    app.router.add_get("/jobs/{job_id}", job_status)
    app.router.add_post("/jobs/{job_id}/resume", resume_job)
    app.router.add_post("/jobs/{job_id}/recrawl", recrawl_job)
    app.router.add_get("/jobs/{job_id}/csv", job_output)
    app.router.add_get("/jobs/{job_id}/{what:reviews}.csv", job_output)
    app.router.add_get("/jobs/{job_id}/export/summary", export_summary)
    app.router.add_get("/jobs/{job_id}/export/mv.csv", export_mv_csv)
    app.router.add_post("/jobs/{job_id}/export/mv-results", export_mv_results)
    app.router.add_post("/jobs/{job_id}/export/build", export_build)
    app.router.add_get("/jobs/{job_id}/export/files/{fn}", export_file)
    app.router.add_get("/locations", locations)
    app.router.add_get("/health", health)
    app.cleanup_ctx.append(background)
    return app


if __name__ == "__main__":
    web.run_app(make_app(), host="0.0.0.0", port=int(os.environ.get("PORT", "8088")))
