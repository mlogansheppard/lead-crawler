"""Lead crawler: finds public business emails on websites.

API (all requests need header  X-API-Key: <API_KEY env>):
  POST /jobs        {"sites": [{"id": "<cid>", "url": "https://..."}], "max_pages": 10}
                    -> {"job_id": "...", "total": N}
  GET  /jobs/{id}   -> {"status": "running"|"done", "done": n, "total": N, "results": [...] (only when done)}
  GET  /health      -> {"ok": true}

Each result: {"id", "url", "emails": [...ranked...], "facebook": "", "pages": n, "error": ""}
"""
import asyncio
import os
import re
import time
import uuid
from urllib.parse import urljoin, urlparse

import aiohttp
from aiohttp import web

from extract import (GUESSED_PATHS, contact_links, extract_emails, normalize_site,
                     rank_emails, registrable)

API_KEY = os.environ.get("API_KEY", "")
CONCURRENCY = int(os.environ.get("CONCURRENCY", "80"))
PAGE_TIMEOUT = float(os.environ.get("PAGE_TIMEOUT", "12"))
SITE_BUDGET = float(os.environ.get("SITE_BUDGET", "45"))
MAX_BYTES = 2_500_000
JOB_TTL = 6 * 3600
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36")
FB_RE = re.compile(r"https?://(?:www\.|m\.)?facebook\.com/(?!sharer|share|plugins|tr\b|dialog|login|policy|privacy)[A-Za-z0-9_.\-/?=]+", re.I)

JOBS = {}
SEM = asyncio.Semaphore(CONCURRENCY)


async def fetch(session, url):
    try:
        async with session.get(url, allow_redirects=True, max_redirects=6, ssl=False) as r:
            ctype = r.headers.get("content-type", "")
            if r.status >= 400 or ("html" not in ctype and "text" not in ctype and ctype):
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
        if page is None and home.startswith("http://"):
            page, final = await fetch(session, "https://" + home[7:])
        elif page is None and home.startswith("https://"):
            page, final = await fetch(session, "http://" + home[8:])
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
            # once we have an on-domain email and have checked a contact page, stop early
            if any(registrable(e.split("@")[1]) == site_reg for e in emails) and res["pages"] >= 2:
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
        res["emails"] = rank_emails(emails, final)
    return res


async def run_job(job_id, sites, max_pages):
    job = JOBS[job_id]
    timeout = aiohttp.ClientTimeout(total=PAGE_TIMEOUT, connect=8)
    conn = aiohttp.TCPConnector(limit=CONCURRENCY * 2, ttl_dns_cache=600, ssl=False)
    headers = {"User-Agent": UA, "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8", "Accept-Language": "en-US,en;q=0.9"}
    async with aiohttp.ClientSession(timeout=timeout, connector=conn, headers=headers) as session:
        async def one(s):
            try:
                r = await crawl_site(session, s, max_pages)
            except Exception as e:  # never let one site kill the job
                r = {"id": s.get("id", ""), "url": s.get("url", ""), "emails": [], "facebook": "", "pages": 0, "error": str(e)[:200]}
            job["results"].append(r)
            job["done"] += 1
        await asyncio.gather(*(one(s) for s in sites))
    job["status"] = "done"
    job["finished_at"] = time.time()


def authed(request):
    return not API_KEY or request.headers.get("X-API-Key") == API_KEY


async def create_job(request):
    if not authed(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    body = await request.json()
    sites = [s for s in body.get("sites", []) if isinstance(s, dict) and s.get("url")]
    max_pages = max(1, min(int(body.get("max_pages", 10)), 20))
    job_id = uuid.uuid4().hex
    JOBS[job_id] = {"status": "running", "done": 0, "total": len(sites), "results": [], "created_at": time.time()}
    asyncio.get_running_loop().create_task(run_job(job_id, sites, max_pages))
    return web.json_response({"job_id": job_id, "total": len(sites)})


async def get_job(request):
    if not authed(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    job = JOBS.get(request.match_info["job_id"])
    if not job:
        return web.json_response({"error": "unknown job (expired or server restarted)"}, status=404)
    out = {"status": job["status"], "done": job["done"], "total": job["total"]}
    if job["status"] == "done":
        found = sum(1 for r in job["results"] if r["emails"])
        out["with_email"] = found
        out["results"] = job["results"]
    return web.json_response(out)


async def health(_):
    return web.json_response({"ok": True, "jobs": len(JOBS)})


async def janitor(app):
    async def loop():
        while True:
            await asyncio.sleep(600)
            now = time.time()
            for k in [k for k, j in JOBS.items() if now - j.get("created_at", now) > JOB_TTL]:
                JOBS.pop(k, None)
    task = asyncio.get_running_loop().create_task(loop())
    yield
    task.cancel()


def make_app():
    app = web.Application(client_max_size=50 * 1024 * 1024)
    app.router.add_post("/jobs", create_job)
    app.router.add_get("/jobs/{job_id}", get_job)
    app.router.add_get("/health", health)
    app.cleanup_ctx.append(janitor)
    return app


if __name__ == "__main__":
    web.run_app(make_app(), host="0.0.0.0", port=int(os.environ.get("PORT", "8088")))
