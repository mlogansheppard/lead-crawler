"""DataForSEO Google Maps search + slimming into the row format the n8n workflow expects."""
import asyncio
import base64
import os
import re

import aiohttp

DFS_LOGIN = os.environ.get("DFS_LOGIN", "")
DFS_PASSWORD = os.environ.get("DFS_PASSWORD", "")
DFS_BASE = "https://api.dataforseo.com/v3/serp/google/maps"
DFS_REVIEWS_BASE = "https://api.dataforseo.com/v3/business_data/google/reviews"
POLL_SECONDS = int(os.environ.get("DFS_POLL_SECONDS", "45"))
MAX_WAIT_SECONDS = int(os.environ.get("DFS_MAX_WAIT_SECONDS", "5400"))

STATE_RE = re.compile(r",\s*([A-Z]{2})\s+\d{5}(?:-\d{4})?\s*(?:,\s*United States)?\s*$")


def _auth_header():
    token = base64.b64encode(f"{DFS_LOGIN}:{DFS_PASSWORD}".encode()).decode()
    return {"Authorization": f"Basic {token}", "Content-Type": "application/json"}


def build_keyword(task):
    if task.get("keyword"):
        return task["keyword"]
    city = str(task.get("city", "")).strip()
    m = re.match(r"^(.*?)\s+(\d{4,6})$", city)
    name, zipc = (m.group(1), m.group(2)) if m else (city, "")
    code = task.get("state_code") or task.get("region", "")
    loc = ", ".join(x for x in [name, f"{code} {zipc}".strip()] if x)
    return f"{task.get('category', '').strip()} in {loc}".strip()


def slim(item, search_city):
    """One DataForSEO maps_search item -> one flat row (column names match the old lobstr CSV where it matters)."""
    ai = item.get("address_info") or {}
    rating = item.get("rating") or {}
    cid = str(item.get("cid") or "")
    address = item.get("address") or ""
    m = STATE_RE.search(address)
    state_code = m.group(1) if m else ""
    cats = [item.get("category")] + list(item.get("additional_categories") or [])
    cats = [c for c in cats if c]
    return {
        "NAME": item.get("title") or "",
        "CATEGORY": ", ".join(dict.fromkeys(cats)),
        "ADDRESS": ai.get("address") or "",
        "FULL ADDRESS": address,
        "CITY": ai.get("city") or "",
        "ZIP CODE": ai.get("zip") or "",
        "STATE CODE": state_code,
        "REGION": "",
        "PHONE": item.get("phone") or "",
        "WEBSITE": item.get("url") or "",
        "DOMAIN": item.get("domain") or "",
        "EMAIL": "",
        "FACEBOOK": "",
        "RATING": rating.get("value") if rating.get("value") is not None else "",
        "REVIEWS COUNT": rating.get("votes_count") if rating.get("votes_count") is not None else "",
        "IS CLAIMED": item.get("is_claimed") if item.get("is_claimed") is not None else "",
        "CID": cid,
        "PLACE ID": item.get("place_id") or "",
        "URL": f"https://maps.google.com/?cid={cid}" if cid else "",
        "LATITUDE": item.get("latitude") or "",
        "LONGITUDE": item.get("longitude") or "",
        "SEARCH CITY": search_city,
    }


async def _post_and_collect(s, post_url, get_url, tasks, on_ready, stats, progress):
    """Generic DataForSEO standard-queue flow.
    tasks: list of (body_dict, key). on_ready(key, task_json) is called for every finished task."""
    pending = {}
    for i in range(0, len(tasks), 100):
        chunk = tasks[i:i + 100]
        body = [b for b, _ in chunk]
        data = None
        for attempt in range(3):
            try:
                async with s.post(post_url, json=body) as r:
                    data = await r.json(content_type=None)
                break
            except Exception:
                await asyncio.sleep(3 * (attempt + 1))
        got = (data or {}).get("tasks") or []
        for idx, t in enumerate(got):
            if t.get("status_code") == 20100 and t.get("id") and idx < len(chunk):
                pending[t["id"]] = chunk[idx][1]
                stats["posted"] += 1
                stats["cost"] = round(stats["cost"] + float(t.get("cost") or 0), 5)
            else:
                stats["post_errors"] += 1
        if data is None:
            stats["post_errors"] += len(chunk)
        progress(stats)

    sem = asyncio.Semaphore(25)

    async def get_one(tid):
        async with sem:
            try:
                async with s.get(get_url.format(tid)) as r:
                    return tid, await r.json(content_type=None)
            except Exception:
                return tid, None

    waited = 0
    await asyncio.sleep(min(60, POLL_SECONDS))
    while pending and waited < MAX_WAIT_SECONDS:
        for tid, data in await asyncio.gather(*(get_one(t) for t in list(pending))):
            t = ((data or {}).get("tasks") or [None])[0]
            if not t:
                continue
            code = t.get("status_code")
            if code == 20000:
                key = pending.pop(tid)
                stats["collected"] += 1
                on_ready(key, t)
            elif code in (40601, 40602):
                continue  # still in queue / handed
            else:
                pending.pop(tid)
                stats["failed"] += 1
        stats["pending"] = len(pending)
        progress(stats)
        if pending:
            await asyncio.sleep(POLL_SECONDS)
            waited += POLL_SECONDS
    stats["timed_out"] = len(pending)


def _session():
    if not DFS_LOGIN:
        raise RuntimeError("DFS_LOGIN / DFS_PASSWORD are not set on the crawler")
    return aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60), headers=_auth_header())


async def run_maps(tasks, depth, language_code, progress):
    """Google Maps search for every task. Returns (rows deduped by CID, stats)."""
    stats = {"tasks": len(tasks), "posted": 0, "post_errors": 0, "collected": 0, "failed": 0, "items": 0, "ads_skipped": 0, "cost": 0.0}
    rows_by_cid = {}

    def on_ready(city, t):
        for res in t.get("result") or []:
            for it in res.get("items") or []:
                if it.get("type") != "maps_search":
                    stats["ads_skipped"] += 1
                    continue
                stats["items"] += 1
                row = slim(it, city)
                rows_by_cid.setdefault(row["CID"] or (row["NAME"] + "|" + row["FULL ADDRESS"]), row)

    bodies = [({"keyword": build_keyword(t), "location_code": 2840, "language_code": language_code,
                "depth": depth, "tag": str(t.get("city", ""))[:250]}, str(t.get("city", ""))) for t in tasks]
    async with _session() as s:
        await _post_and_collect(s, f"{DFS_BASE}/task_post", DFS_BASE + "/task_get/advanced/{}", bodies, on_ready, stats, progress)
    stats["unique_places"] = len(rows_by_cid)
    return list(rows_by_cid.values()), stats


def _review_images(it):
    imgs = it.get("images") or []
    urls = []
    for im in imgs:
        if isinstance(im, dict):
            u = im.get("image_url") or im.get("url") or ""
        else:
            u = str(im)
        if u:
            urls.append(u)
    return ", ".join(urls)


async def run_reviews(cids, depth, keep, max_rating, language_code, progress):
    """Lowest-rated reviews per CID. Keeps up to `keep` reviews with rating <= max_rating
    (reviews with text first). Returns (review rows, stats)."""
    stats = {"businesses": len(cids), "posted": 0, "post_errors": 0, "collected": 0, "failed": 0,
             "reviews_seen": 0, "kept": 0, "businesses_with_kept": 0, "cost": 0.0}
    out = []

    def on_ready(cid, t):
        picked = []
        for res in t.get("result") or []:
            for it in res.get("items") or []:
                stats["reviews_seen"] += 1
                val = (it.get("rating") or {}).get("value")
                try:
                    val = float(val)
                except (TypeError, ValueError):
                    continue
                if val > max_rating:
                    continue
                text = (it.get("review_text") or it.get("original_review_text") or "").strip()
                picked.append({"CID": cid, "SCORE": int(val) if val == int(val) else val, "TEXT": text,
                               "PICTURES": _review_images(it), "USER NAME": it.get("profile_name") or "",
                               "REVIEW DATE": it.get("timestamp") or "", "REVIEW URL": it.get("review_url") or ""})
        picked.sort(key=lambda r: 0 if r["TEXT"] else 1)  # stable: keeps DataForSEO's lowest-first order
        picked = picked[:keep]
        if picked:
            stats["businesses_with_kept"] += 1
            stats["kept"] += len(picked)
            out.extend(picked)

    bodies = [({"cid": c, "location_code": 2840, "language_code": language_code, "depth": depth,
                "sort_by": "lowest_rating", "tag": c}, c) for c in cids]
    async with _session() as s:
        await _post_and_collect(s, f"{DFS_REVIEWS_BASE}/task_post", DFS_REVIEWS_BASE + "/task_get/{}", bodies, on_ready, stats, progress)
    return out, stats
