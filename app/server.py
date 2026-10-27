"""Lead crawler: Google Maps listings (via DataForSEO) + public business emails from their websites.

All requests need header  X-API-Key: <API_KEY env>.

  POST /maps-jobs   {"tasks": [{"city": "Austin 78701", "category": "plumber", "state_code": "TX"}],
                     "depth": 200, "max_pages": 10}
                    -> {"job_id": "...", "tasks": N}
                    Runs DataForSEO Maps for every task, dedupes by CID, then crawls each website for emails.

  POST /jobs        {"sites": [{"id": "<anything>", "url": "https://..."}], "max_pages": 10}
                    -> {"job_id": "..."}   (emails only, for websites you already have)

  GET  /jobs/{id}        -> status + stats (no rows)
  GET  /jobs/{id}/csv           -> the finished rows as CSV (maps jobs) or JSON results (email-only jobs)
  GET  /jobs/{id}/reviews.csv   -> low-star reviews (CID, SCORE, TEXT, PICTURES, USER NAME, ...) for businesses with an email

  Maps jobs also take "reviews": {"enabled": true, "depth": 20, "keep": 5, "max_rating": 1}
  (DataForSEO reviews sorted lowest_rating; keeps up to `keep` reviews rated <= max_rating, ones with text first).
  GET  /health
"""
import asyncio
import csv
import io
import os
import re
import time
import traceback
import uuid
from urllib.parse import urljoin, urlparse

import aiohttp
from aiohttp import web

from extract import (GUESSED_PATHS, contact_links, extract_emails, normalize_site,
                     rank_emails, registrable)
import maps

API_KEY = os.environ.get("API_KEY", "")
CONCURRENCY = int(os.environ.get("CONCURRENCY", "80"))
PAGE_TIMEOUT = float(os.environ.get("PAGE_TIMEOUT", "12"))
SITE_BUDGET = float(os.environ.get("SITE_BUDGET", "45"))
MAX_EMAILS = 5
MAX_BYTES = 2_500_000
JOB_TTL = 24 * 3600
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36")
FB_RE = re.compile(r"https?://(?:www\.|m\.)?facebook\.com/(?!sharer|share|plugins|tr\b|dialog|login|policy|privacy)[A-Za-z0-9_.\-/?=]+", re.I)

JOBS = {}
SEM = asyncio.Semaphore(CONCURRENCY)


# ---------------------------------------------------------------- website crawling
async def fetch(session, url):
    try:
        async with session.get(url, allow_redirects=True, max_redirects=6, ssl=False) as r:
            ctype = r.headers.get("content-type", "")
            if r.status >= 400 or (ctype and "html" not in ctype and "text" not in ctype):
                return None, str(r.url)
            body = await r.content.read(MAX_BYTES)
            return body.decode(r.charset or "utf-8", errors="replace"), str(r.url)
    except Exception:
        return None, url


async def crawl_site(session, site, max_pages):
    sid, raw_url = site.get("id", ""), site.get("url", "")
    res = {"id": sid, "url": raw_url, "emails": [], "facebook": "", "pages": 0, "error": ""}
    home = normalize_site(raw_url)
    if not home:
        res["error"] = "skipped"
        return res
    started = time.monotonic()
    async with SEM:
        page, final = await fetch(session, home)
        if page is None:
            alt = "https://" + home[7:] if home.startswith("http://") else "http://" + home[8:]
            page, final = await fetch(session, alt)
        if page is None:
            res["error"] = "unreachable"
            return res
        res["pages"] = 1
        emails = extract_emails(page)
        fb = FB_RE.search(page)
        res["facebook"] = fb.group(0).split("?")[0] if fb else ""

        root = "{0.scheme}://{0.netloc}".format(urlparse(final))
        queue = contact_links(page, final)
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
            if res["pages"] >= 2 and any(registrable(e.split("@")[1]) == site_reg for e in emails):
                break
            if u.rstrip("/") in visited:
                continue
            visited.add(u.rstrip("/"))
            sub, _ = await fetch(session, u)
            if sub is None:
                continue
            res["pages"] += 1
            for e in extract_emails(sub):
                if e not in emails:
                    emails.append(e)
        res["emails"] = rank_emails(emails, final)[:MAX_EMAILS]
    return res


async def crawl_many(sites, max_pages, on_done=None):
    timeout = aiohttp.ClientTimeout(total=PAGE_TIMEOUT, connect=8)
    conn = aiohttp.TCPConnector(limit=CONCURRENCY * 2, ttl_dns_cache=600, ssl=False)
    headers = {"User-Agent": UA, "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8", "Accept-Language": "en-US,en;q=0.9"}
    results = []
    async with aiohttp.ClientSession(timeout=timeout, connector=conn, headers=headers) as session:
        async def one(s):
            try:
                r = await crawl_site(session, s, max_pages)
            except Exception as e:  # one bad site never kills the job
                r = {"id": s.get("id", ""), "url": s.get("url", ""), "emails": [], "facebook": "", "pages": 0, "error": str(e)[:200]}
            results.append(r)
            if on_done:
                on_done()
        await asyncio.gather(*(one(s) for s in sites))
    return results


# ---------------------------------------------------------------- jobs
def new_job(kind, **extra):
    job_id = uuid.uuid4().hex
    JOBS[job_id] = {"kind": kind, "status": "running", "phase": "queued", "created_at": time.time(),
                    "stats": {}, "rows": [], "reviews": [], "results": [], "error": "", **extra}
    return job_id, JOBS[job_id]


async def run_email_job(job, sites, max_pages):
    job["phase"] = "emails"
    job["stats"] = {"sites": len(sites), "crawled": 0}

    def tick():
        job["stats"]["crawled"] += 1
    job["results"] = await crawl_many(sites, max_pages, tick)
    job["stats"]["with_email"] = sum(1 for r in job["results"] if r["emails"])


async def run_maps_job(job, tasks, depth, max_pages, language_code, reviews):
    job["phase"] = "maps"
    rows, mstats = await maps.run_maps(tasks, depth, language_code, lambda s: job["stats"].update(maps=dict(s)))
    job["stats"]["maps"] = mstats

    # one crawl per unique website (chains / franchises share a site)
    by_site = {}
    for r in rows:
        home = normalize_site(r["WEBSITE"])
        if home:
            key = registrable(urlparse(home).hostname or "") + urlparse(home).path.rstrip("/")
            by_site.setdefault(key, {"id": key, "url": r["WEBSITE"], "rows": []})["rows"].append(r)
    job["phase"] = "emails"
    job["stats"]["emails"] = {"listings": len(rows), "with_website": sum(len(v["rows"]) for v in by_site.values()),
                              "unique_sites": len(by_site), "crawled": 0}

    def tick():
        job["stats"]["emails"]["crawled"] += 1
    results = await crawl_many([{"id": k, "url": v["url"]} for k, v in by_site.items()], max_pages, tick)
    for res in results:
        for r in by_site[res["id"]]["rows"]:
            r["EMAIL"] = ", ".join(res["emails"])
            r["FACEBOOK"] = res["facebook"]
    job["stats"]["emails"]["listings_with_email"] = sum(1 for r in rows if r["EMAIL"])
    job["rows"] = rows

    # lowest-rated reviews, only for businesses we can actually email
    if reviews.get("enabled", True):
        cids = list(dict.fromkeys(r["CID"] for r in rows if r["EMAIL"] and r["CID"]))
        job["phase"] = "reviews"
        job["reviews"], rstats = await maps.run_reviews(
            cids, reviews["depth"], reviews["keep"], reviews["max_rating"], language_code,
            lambda st: job["stats"].update(reviews=dict(st)))
        job["stats"]["reviews"] = rstats


async def guard(job, coro):
    try:
        await coro
        job["status"] = "done"
    except Exception as e:
        job["status"] = "error"
        job["error"] = f"{type(e).__name__}: {e}"
        traceback.print_exc()
    job["phase"] = job["status"]
    job["finished_at"] = time.time()


# ---------------------------------------------------------------- http
def authed(request):
    return not API_KEY or request.headers.get("X-API-Key") == API_KEY


def deny():
    return web.json_response({"error": "unauthorized"}, status=401)


async def create_maps_job(request):
    if not authed(request):
        return deny()
    body = await request.json()
    tasks = [t for t in body.get("tasks", []) if isinstance(t, dict) and (t.get("keyword") or t.get("city"))]
    if not tasks:
        return web.json_response({"error": "no tasks"}, status=400)
    depth = max(20, min(int(body.get("depth", 200)), 700))
    max_pages = max(1, min(int(body.get("max_pages", 10)), 20))
    rv = body.get("reviews") or {}
    reviews = {"enabled": bool(rv.get("enabled", True)),
               "depth": max(10, min(int(rv.get("depth", 20)), 200)),
               "keep": max(1, min(int(rv.get("keep", 5)), 50)),
               "max_rating": float(rv.get("max_rating", 1))}
    job_id, job = new_job("maps", label=str(body.get("label", ""))[:200])
    asyncio.get_running_loop().create_task(guard(job, run_maps_job(job, tasks, depth, max_pages, body.get("language_code", "en"), reviews)))
    return web.json_response({"job_id": job_id, "tasks": len(tasks)})


async def create_email_job(request):
    if not authed(request):
        return deny()
    body = await request.json()
    sites = [s for s in body.get("sites", []) if isinstance(s, dict) and s.get("url")]
    max_pages = max(1, min(int(body.get("max_pages", 10)), 20))
    job_id, job = new_job("emails")
    asyncio.get_running_loop().create_task(guard(job, run_email_job(job, sites, max_pages)))
    return web.json_response({"job_id": job_id, "total": len(sites)})


def get_job_or_404(request):
    job = JOBS.get(request.match_info["job_id"])
    if not job:
        return None, web.json_response({"error": "unknown job (expired, or the crawler restarted)"}, status=404)
    return job, None


async def job_status(request):
    if not authed(request):
        return deny()
    job, err = get_job_or_404(request)
    if err:
        return err
    return web.json_response({"kind": job["kind"], "status": job["status"], "phase": job["phase"], "error": job["error"],
                              "stats": job["stats"], "rows": len(job["rows"]), "review_rows": len(job["reviews"]),
                              "elapsed_s": round((job.get("finished_at") or time.time()) - job["created_at"])})


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


async def health(_):
    return web.json_response({"ok": True, "jobs": len(JOBS), "dfs_configured": bool(maps.DFS_LOGIN)})


async def janitor(app):
    async def loop():
        while True:
            await asyncio.sleep(600)
            now = time.time()
            for k in [k for k, j in JOBS.items() if now - j["created_at"] > JOB_TTL]:
                JOBS.pop(k, None)
    task = asyncio.get_running_loop().create_task(loop())
    yield
    task.cancel()


def make_app():
    app = web.Application(client_max_size=50 * 1024 * 1024)
    app.router.add_post("/maps-jobs", create_maps_job)
    app.router.add_post("/jobs", create_email_job)
    app.router.add_get("/jobs/{job_id}", job_status)
    app.router.add_get("/jobs/{job_id}/csv", job_output)
    app.router.add_get("/jobs/{job_id}/{what:reviews}.csv", job_output)
    app.router.add_get("/health", health)
    app.cleanup_ctx.append(janitor)
    return app


if __name__ == "__main__":
    web.run_app(make_app(), host="0.0.0.0", port=int(os.environ.get("PORT", "8088")))
