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


def _one_star(item):
    dist = item.get("rating_distribution")
    if isinstance(dist, dict) and dist:
        try:
            return int(dist.get("1") or 0)
        except (TypeError, ValueError):
            return ""
    # No distribution from the Maps search: work it out from the average when the math rules a 1-star out.
    # One 1-star among n reviews caps the average at 5 - 4/n. The shown rating is rounded, so leave 0.1 of slack.
    rating = item.get("rating") or {}
    if rating.get("votes_count") is None:
        return ""
    try:
        n = int(rating.get("votes_count"))
        value = float(rating.get("value")) if rating.get("value") is not None else None
    except (TypeError, ValueError):
        return ""
    if n == 0:
        return 0
    if value is not None and value - 0.1 > 5 - 4 / n:
        return 0
    return ""  # unknown


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
        "ONE STAR RATINGS": _one_star(item),
        "CONTACT URL": item.get("contact_url") or "",
        "CID": cid,
        "PLACE ID": item.get("place_id") or "",
        "URL": f"https://maps.google.com/?cid={cid}" if cid else "",
        "LATITUDE": item.get("latitude") or "",
        "LONGITUDE": item.get("longitude") or "",
        "SEARCH CITY": search_city,
    }


class DFSPaused(Exception):
    """DataForSEO refused us (bad key, no balance, or unreachable). Job state is kept so it can resume."""


NO_RESULTS = 40102  # "No Search Results": a normal, empty search (e.g. a rural county), not an account problem


def is_account_problem(code, msg=""):
    """True only for key / auth / balance problems, which pause the job.
    DataForSEO also uses 401xx for ordinary results like 40102 'No Search Results', so match narrowly."""
    try:
        code = int(code or 0)
    except (TypeError, ValueError):
        code = 0
    if code == NO_RESULTS:
        return False
    if 40200 <= code < 40300:  # payment required / insufficient funds
        return True
    if code in (40100, 40101, 40104):  # not authorized / authentication failed / access denied
        return True
    m = (msg or "").lower()
    return code // 100 == 401 and any(w in m for w in ("authoriz", "authent", "credential", "login", "password", "access denied"))


def _pause_reason(http_status, data):
    code = (data or {}).get("status_code") if isinstance(data, dict) else None
    msg = (data or {}).get("status_message", "") if isinstance(data, dict) else ""
    if http_status in (401, 402, 403) or is_account_problem(code, msg):
        return f"DataForSEO {code or http_status}: {msg or 'not authorized / payment required'}"
    return None


def new_stage(tasks):
    """Persistable state for one DataForSEO stage. tasks: list of (body_dict, key)."""
    return {"todo": [[b, k] for b, k in tasks], "pending": {}, "stats": {
        "tasks": len(tasks), "posted": 0, "post_errors": 0, "collected": 0, "failed": 0, "cost": 0.0}}


async def run_stage(s, post_url, get_url, state, on_ready, save):
    """Post everything in state['todo'] and collect everything in state['pending'].
    Safe to call again after a pause or restart: already-posted tasks are never posted (or paid for) twice.
    Raises DFSPaused on auth/balance/network problems with state left intact."""
    stats = state["stats"]
    # 1) post what's left, 100 tasks per call
    while state["todo"]:
        chunk = state["todo"][:100]
        data, http_status = None, None
        for attempt in range(3):
            try:
                async with s.post(post_url, json=[b for b, _ in chunk]) as r:
                    http_status = r.status
                    data = await r.json(content_type=None)
                break
            except Exception:
                await asyncio.sleep(3 * (attempt + 1))
        if data is None:
            save(force=True)
            raise DFSPaused("DataForSEO unreachable while posting tasks")
        reason = _pause_reason(http_status, data)
        if reason:
            save(force=True)
            raise DFSPaused(reason)
        got = data.get("tasks") or []
        retry, pause = [], None
        for idx, item in enumerate(chunk):
            t = got[idx] if idx < len(got) else None
            code = (t or {}).get("status_code")
            if t and code == 20100 and t.get("id"):
                state["pending"][t["id"]] = item[1]
                stats["posted"] += 1
                stats["cost"] = round(stats["cost"] + float(t.get("cost") or 0), 5)
            elif is_account_problem(code, (t or {}).get("status_message", "")):
                retry.append(item)
                pause = f"DataForSEO {code}: {t.get('status_message', '')}"
            else:
                stats["post_errors"] += 1  # bad task (e.g. invalid field) - skip it
        state["todo"] = retry + state["todo"][len(chunk):]
        save()
        if pause:
            save(force=True)
            raise DFSPaused(pause)

    # 2) collect
    sem = asyncio.Semaphore(25)

    async def get_one(tid):
        async with sem:
            try:
                async with s.get(get_url.format(tid)) as r:
                    return tid, r.status, await r.json(content_type=None)
            except Exception:
                return tid, None, None

    waited, first = 0, True
    while state["pending"] and waited < MAX_WAIT_SECONDS:
        if first:
            await asyncio.sleep(min(60, POLL_SECONDS))
            first = False
        results = await asyncio.gather(*(get_one(t) for t in list(state["pending"])))
        if all(d is None for _, _, d in results):
            save(force=True)
            raise DFSPaused("DataForSEO unreachable while collecting results")
        pause = None
        for tid, http_status, data in results:
            if data is None:
                continue
            reason = _pause_reason(http_status, data)
            if reason:
                pause = reason
                continue
            t = (data.get("tasks") or [None])[0]
            if not t:
                continue
            code = t.get("status_code")
            if code == 20000:
                key = state["pending"].pop(tid)
                stats["collected"] += 1
                on_ready(key, t)
            elif code in (40601, 40602):
                continue  # still queued
            elif code == NO_RESULTS:
                key = state["pending"].pop(tid)
                stats["collected"] += 1
                stats["no_results"] = stats.get("no_results", 0) + 1
                on_ready(key, {"result": []})
            elif is_account_problem(code, t.get("status_message", "")):
                pause = f"DataForSEO {code}: {t.get('status_message', '')}"
            else:
                state["pending"].pop(tid)
                stats["failed"] += 1
        stats["pending"] = len(state["pending"])
        save()
        if pause:
            save(force=True)
            raise DFSPaused(pause)
        if state["pending"]:
            await asyncio.sleep(POLL_SECONDS)
            waited += POLL_SECONDS
    stats["timed_out"] = len(state["pending"])
    state["pending"] = {}
    save(force=True)


def session():
    if not DFS_LOGIN:
        raise DFSPaused("DFS_LOGIN / DFS_PASSWORD are not set on the crawler (.env)")
    return aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60), headers=_auth_header())


def maps_tasks(tasks, depth, language_code):
    return [({"keyword": build_keyword(t), "location_code": 2840, "language_code": language_code,
              "depth": depth, "tag": str(t.get("city", ""))[:250]}, str(t.get("city", ""))) for t in tasks]


def maps_on_ready(rows_by_cid, stats):
    def on_ready(city, t):
        for res in t.get("result") or []:
            for it in res.get("items") or []:
                if it.get("type") != "maps_search":
                    stats["ads_skipped"] = stats.get("ads_skipped", 0) + 1
                    continue
                stats["items"] = stats.get("items", 0) + 1
                row = slim(it, city)
                rows_by_cid.setdefault(row["CID"] or (row["NAME"] + "|" + row["FULL ADDRESS"]), row)
    return on_ready


async def run_maps_stage(state, rows_by_cid, save):
    async with session() as s:
        await run_stage(s, f"{DFS_BASE}/task_post", DFS_BASE + "/task_get/advanced/{}", state,
                        maps_on_ready(rows_by_cid, state["stats"]), save)
    state["stats"]["unique_places"] = len(rows_by_cid)


def _review_images(it):
    urls = []
    for im in it.get("images") or []:
        u = (im.get("image_url") or im.get("url") or "") if isinstance(im, dict) else str(im)
        if u:
            urls.append(u)
    return ", ".join(urls)


def reviews_tasks(cids, depth, language_code):
    # sort_by is always lowest_rating: the whole point is the worst reviews
    return [({"cid": c, "location_code": 2840, "language_code": language_code, "depth": depth,
              "sort_by": "lowest_rating", "tag": c}, c) for c in cids]


def reviews_on_ready(out, stats, keep, max_rating):
    def on_ready(cid, t):
        if ((t.get("data") or {}).get("sort_by") or "lowest_rating") != "lowest_rating":
            stats["not_lowest_first"] = stats.get("not_lowest_first", 0) + 1
        picked = []
        for res in t.get("result") or []:
            for it in res.get("items") or []:
                stats["reviews_seen"] = stats.get("reviews_seen", 0) + 1
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
        # reviews with text first (lowest rating first among them), then text-less ones (lowest first);
        # a 2-star with text beats a 1-star with none. Stable, so DataForSEO's order is kept inside each group
        picked.sort(key=lambda r: (0 if r["TEXT"] else 1, r["SCORE"]))
        picked = picked[:keep]
        if picked:
            stats["businesses_with_kept"] = stats.get("businesses_with_kept", 0) + 1
            stats["kept"] = stats.get("kept", 0) + len(picked)
            out.extend(picked)
    return on_ready


async def run_reviews_stage(state, out, keep, max_rating, save):
    async with session() as s:
        await run_stage(s, f"{DFS_REVIEWS_BASE}/task_post", DFS_REVIEWS_BASE + "/task_get/{}", state,
                        reviews_on_ready(out, state["stats"], keep, max_rating), save)


# ---------------------------------------------------------------- adaptive search plan
# Start with one search per county. Any search that comes back "full" (>= split_at places) is split into
# its cities, and any full city into its ZIPs. Sparse areas cost one search; dense metros still get every ZIP.

def build_plan(state_codes, category, test_limit=0):
    """Returns {"nodes": {id: {"kw", "label", "children"}}, "roots": [ids]} for the given states."""
    import zipcodes
    zips = [z for z in zipcodes.list_all() if z["zip_code_type"] == "STANDARD" and z["active"]]
    cat = category.strip()
    nodes, roots = {}, []
    for st in state_codes:
        counties = {}
        for z in zips:
            if z["state"] != st:
                continue
            county = z["county"] or z["city"].title()
            counties.setdefault(county, {}).setdefault(z["city"].title(), []).append(z["zip_code"])
        names = sorted(counties)
        if test_limit and test_limit > 0:
            names = names[:test_limit]
        for county in names:
            cities = counties[county]
            city_ids = []
            for city in sorted(cities):
                zip_ids = []
                for zc in sorted(cities[city]):
                    zid = f"z:{st}:{zc}"
                    nodes[zid] = {"kw": f"{cat} in {city}, {st} {zc}", "label": f"{city} {zc}", "children": []}
                    zip_ids.append(zid)
                if len(zip_ids) == 1:
                    city_ids.append(zip_ids[0])  # one-ZIP town: the ZIP search is the city search
                else:
                    cid = f"c:{st}:{county}:{city}"
                    nodes[cid] = {"kw": f"{cat} in {city}, {st}", "label": f"{city}, {st}", "children": zip_ids}
                    city_ids.append(cid)
            if len(city_ids) == 1:
                roots.append(city_ids[0])  # one-city county: start at the city
            else:
                kid = f"k:{st}:{county}"
                nodes[kid] = {"kw": f"{cat} in {county}, {st}", "label": f"{county}, {st}", "children": city_ids}
                roots.append(kid)
    return {"nodes": nodes, "roots": roots}


DEEP_DEPTH = 700  # DataForSEO's max; only used to re-search an area that came back full and can't be split


def plan_round_tasks(plan, frontier, depth, language_code, deep=()):
    deep = set(deep)
    return [({"keyword": plan["nodes"][nid]["kw"], "location_code": 2840, "language_code": language_code,
              "depth": DEEP_DEPTH if nid in deep else depth, "tag": nid[:250]}, nid) for nid in frontier]


def adaptive_on_ready(plan, rows_by_cid, counts, stats):
    def on_ready(nid, t):
        n = 0
        label = plan["nodes"][nid]["label"]
        for res in t.get("result") or []:
            for it in res.get("items") or []:
                if it.get("type") != "maps_search":
                    stats["ads_skipped"] = stats.get("ads_skipped", 0) + 1
                    continue
                n += 1
                stats["items"] = stats.get("items", 0) + 1
                row = slim(it, label)
                rows_by_cid.setdefault(row["CID"] or (row["NAME"] + "|" + row["FULL ADDRESS"]), row)
        counts[nid] = n
    return on_ready
