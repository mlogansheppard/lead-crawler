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
                on_done(r)
        await asyncio.gather(*(one(s) for s in sites))
    return results


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
    now = time.monotonic()
    if not force and now - _last_save.get(job["id"], 0) < 30:
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


def load_jobs():
    if not os.path.isdir(DATA_DIR):
        return
    for name in os.listdir(DATA_DIR):
        if not (name.startswith("job-") and name.endswith(".json")):
            continue
        try:
            with open(os.path.join(DATA_DIR, name)) as f:
                job = json.load(f)
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

    rows = list(job["maps_rows"].values())

    # 2) website emails (free, but still remembered so a restart doesn't redo finished sites)
    if not job.get("emails_done"):
        job["phase"] = "emails"
        by_site = {}
        for r in rows:
            k = _site_key(r["WEBSITE"])
            if k:
                by_site.setdefault(k, {"url": r["WEBSITE"], "n": 0})["n"] += 1
        done = job.setdefault("crawl_results", {})
        est = job["stats"].setdefault("emails", {})
        est.update({"listings": len(rows), "with_website": sum(v["n"] for v in by_site.values()),
                    "unique_sites": len(by_site), "crawled": len(done)})

        def tick(r):
            done[r["id"]] = {"emails": r["emails"], "facebook": r["facebook"]}
            est["crawled"] = len(done)
            save()
        todo = [{"id": k, "url": v["url"]} for k, v in by_site.items() if k not in done]
        await crawl_many(todo, p["max_pages"], tick)
        job["emails_done"] = True
        save(True)

    done = job.get("crawl_results", {})
    for r in rows:
        res = done.get(_site_key(r["WEBSITE"]))
        r["EMAIL"] = ", ".join(res["emails"]) if res else ""
        r["FACEBOOK"] = res["facebook"] if res else ""
    job["stats"].setdefault("emails", {})["listings_with_email"] = sum(1 for r in rows if r["EMAIL"])
    job["rows"] = rows

    # 3) DataForSEO reviews, lowest rating first, only for businesses we can email
    rv = p["reviews"]
    if rv["enabled"] and not job.get("reviews_done"):
        job["phase"] = "reviews"
        if "reviews_state" not in job:
            cids = list(dict.fromkeys(r["CID"] for r in rows if r["EMAIL"] and r["CID"]))
            job["reviews_state"] = maps.new_stage(maps.reviews_tasks(cids, rv["depth"], p["language_code"]))
            job["reviews"] = []
        await maps.run_reviews_stage(job["reviews_state"], job["reviews"], rv["keep"], rv["max_rating"], save)
        job["reviews_done"] = True


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
            job["maps_state"] = maps.new_stage(maps.plan_round_tasks(plan, job["frontier"], p["depth"], p["language_code"]))
            save(True)
        st = job["maps_state"]
        async with maps.session() as s:
            await maps.run_stage(s, f"{maps.DFS_BASE}/task_post", maps.DFS_BASE + "/task_get/advanced/{}", st,
                                 maps.adaptive_on_ready(plan, job["maps_rows"], job["node_counts"], st["stats"]), save)
        # decide what to split
        searched = set(job["searched_kw"]) | {plan["nodes"][n]["kw"] for n in job["frontier"]}
        nxt, full = [], 0
        for nid in job["frontier"]:
            if job["node_counts"].get(nid, 0) >= p["split_at"] and plan["nodes"][nid]["children"]:
                full += 1
                for c in plan["nodes"][nid]["children"]:
                    if plan["nodes"][c]["kw"] not in searched:
                        searched.add(plan["nodes"][c]["kw"])
                        nxt.append(c)
        job["rounds"].append({"round": job["round"], "searches": len(job["frontier"]), "split": full,
                              "next": len(nxt), "cost": st["stats"]["cost"], "unique_places": len(job["maps_rows"])})
        job["searched_kw"] = sorted(searched)
        job["frontier"] = nxt
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
        st["reviews"] = job["reviews_state"]["stats"]
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


def start(job):
    if job["id"] in RUNNING:
        return
    RUNNING.add(job["id"])
    runner = run_maps_job if job["kind"] == "maps" else run_email_job

    async def go():
        try:
            await guard(job, runner)
        finally:
            RUNNING.discard(job["id"])
    asyncio.get_running_loop().create_task(go())


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
    if ph == "maps" and "maps" in stats:
        m = stats["maps"]
        if m.get("strategy") == "adaptive":
            c = m.get("current_round") or {}
            return (f"maps round {m.get('round', 0)}: {c.get('collected', 0)}/{c.get('tasks', 0)} searches, "
                    f"{m.get('unique_places', 0)} places so far")
        return f"maps {m.get('collected', 0)}/{m.get('tasks', 0)} searches"
    if ph == "emails" and "emails" in stats:
        e = stats["emails"]
        return f"emails {e.get('crawled', 0)}/{e.get('unique_sites', 0)} sites"
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
        "rows": len(job["rows"]), "review_rows": len(job["reviews"]),
        "elapsed_s": round((job.get("finished_at") or time.time()) - job["created_at"]), "active_s": round(active)})


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
    return web.json_response({"ok": True, "jobs": len(JOBS), "running": len(RUNNING), "paused": paused,
                              "dfs_configured": bool(maps.DFS_LOGIN)})


async def background(app):
    """On startup: reload saved jobs and continue any that were running or paused.
    Then every RESUME_EVERY seconds retry paused jobs (e.g. after a DataForSEO top-up) and clean up old ones."""
    load_jobs()
    for j in list(JOBS.values()):
        if j["kind"] == "maps" and j["status"] in ("running", "paused"):
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
    task = asyncio.get_running_loop().create_task(loop())
    yield
    task.cancel()


def make_app():
    app = web.Application(client_max_size=50 * 1024 * 1024)
    app.router.add_post("/maps-jobs", create_maps_job)
    app.router.add_post("/jobs", create_email_job)
    app.router.add_get("/jobs/{job_id}", job_status)
    app.router.add_post("/jobs/{job_id}/resume", resume_job)
    app.router.add_get("/jobs/{job_id}/csv", job_output)
    app.router.add_get("/jobs/{job_id}/{what:reviews}.csv", job_output)
    app.router.add_get("/locations", locations)
    app.router.add_get("/health", health)
    app.cleanup_ctx.append(background)
    return app


if __name__ == "__main__":
    web.run_app(make_app(), host="0.0.0.0", port=int(os.environ.get("PORT", "8088")))
