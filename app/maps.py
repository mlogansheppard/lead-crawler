"""DataForSEO Google Maps search + slimming into the row format the n8n workflow expects."""
import asyncio
import base64
import os
import re

import aiohttp

DFS_LOGIN = os.environ.get("DFS_LOGIN", "")
DFS_PASSWORD = os.environ.get("DFS_PASSWORD", "")
DFS_BASE = "https://api.dataforseo.com/v3/serp/google/maps"
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


async def run_maps(tasks, depth, language_code, progress):
    """Post tasks to the standard queue, poll until all are collected. Returns (rows, stats)."""
    if not DFS_LOGIN:
        raise RuntimeError("DFS_LOGIN / DFS_PASSWORD are not set on the crawler")
    headers = _auth_header()
    timeout = aiohttp.ClientTimeout(total=60)
    pending = {}  # task id -> search city
    stats = {"tasks": len(tasks), "posted": 0, "post_errors": 0, "collected": 0, "failed": 0, "items": 0, "ads_skipped": 0, "cost": 0.0}
    rows_by_cid = {}
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as s:
        # 1) post in chunks of 100 (DataForSEO limit per call)
        for i in range(0, len(tasks), 100):
            chunk = tasks[i:i + 100]
            body = [{"keyword": build_keyword(t), "location_code": 2840, "language_code": language_code,
                     "depth": depth, "tag": str(t.get("city", ""))[:250]} for t in chunk]
            for attempt in range(3):
                try:
                    async with s.post(f"{DFS_BASE}/task_post", json=body) as r:
                        data = await r.json(content_type=None)
                    break
                except Exception:
                    data = None
                    await asyncio.sleep(3 * (attempt + 1))
            for t in (data or {}).get("tasks") or []:
                if t.get("status_code") == 20100 and t.get("id"):
                    pending[t["id"]] = (t.get("data") or {}).get("tag", "")
                    stats["posted"] += 1
                    stats["cost"] += float(t.get("cost") or 0)
                else:
                    stats["post_errors"] += 1
            if data is None:
                stats["post_errors"] += len(chunk)
            progress(stats)

        # 2) poll task_get until everything is collected or we time out
        waited = 0
        sem = asyncio.Semaphore(25)

        async def get_one(tid):
            async with sem:
                try:
                    async with s.get(f"{DFS_BASE}/task_get/advanced/{tid}") as r:
                        return tid, await r.json(content_type=None)
                except Exception:
                    return tid, None

        await asyncio.sleep(min(60, POLL_SECONDS))
        while pending and waited < MAX_WAIT_SECONDS:
            results = await asyncio.gather(*(get_one(t) for t in list(pending)))
            for tid, data in results:
                t = ((data or {}).get("tasks") or [None])[0]
                if not t:
                    continue
                code = t.get("status_code")
                if code == 20000:
                    city = pending.pop(tid)
                    stats["collected"] += 1
                    for res in t.get("result") or []:
                        for it in res.get("items") or []:
                            if it.get("type") != "maps_search":
                                stats["ads_skipped"] += 1
                                continue
                            stats["items"] += 1
                            row = slim(it, city)
                            key = row["CID"] or (row["NAME"] + "|" + row["FULL ADDRESS"])
                            rows_by_cid.setdefault(key, row)
                elif code in (40601, 40602):
                    continue  # still in queue / handed to the crawler
                else:
                    pending.pop(tid)
                    stats["failed"] += 1
            stats["pending"] = len(pending)
            progress(stats)
            if pending:
                await asyncio.sleep(POLL_SECONDS)
                waited += POLL_SECONDS
        stats["timed_out"] = len(pending)
    stats["unique_places"] = len(rows_by_cid)
    return list(rows_by_cid.values()), stats
