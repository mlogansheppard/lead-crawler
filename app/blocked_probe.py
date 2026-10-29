"""Test the blocked-sites pass on real sites before trusting it in production.

  docker exec lead-crawler python3 blocked_probe.py [sample_size] [job_id] [error]

error: blocked (default; also re-tests SSL failures) or any saved error such as connect, timeout, dns -
those are re-tried with the normal crawler first, then as Chrome.

Also re-tests sites whose Chrome pass ended in an SSL error, to see whether they load when tried later.

Takes sites that refused the normal crawler in the newest finished job (or the job given), fetches a
random sample again as real Chrome, and prints how many now load and how many give an email.
Nothing is saved; the job is not changed."""
import asyncio
import collections
import glob
import json
import os
import random
import sys
import time

import server


WHICH = sys.argv[3] if len(sys.argv) > 3 else "blocked"


def _want(err):
    if WHICH == "blocked":
        return server.is_blocked(err) or err == "ssl"
    return (err or "") == WHICH


def pick_job(job_id=None):
    if job_id:
        return json.load(open(server.job_path(job_id)))
    paths = sorted(glob.glob(os.path.join(server.DATA_DIR, "job-*.json")), key=os.path.getmtime, reverse=True)
    for p in paths:
        j = json.load(open(p))
        if any(_want(v.get("error")) for v in (j.get("crawl_results") or {}).values()):
            return j
    sys.exit("no job with blocked sites found")


async def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    job = pick_job(sys.argv[2] if len(sys.argv) > 2 else None)
    done = job["crawl_results"]
    urls = {}
    for r in job.get("rows") or list((job.get("maps_rows") or {}).values()):
        k = server._site_key(r.get("WEBSITE") or "")
        if k and k not in urls:
            urls[k] = {"url": r["WEBSITE"], "contact_url": r.get("CONTACT URL", "")}
    blocked = [k for k, v in done.items() if _want(v.get("error")) and k in urls]
    random.seed(7)
    sample = random.sample(blocked, min(n, len(blocked)))
    print(f"job {job['id']} ({job['params'].get('label') or job['params'].get('category')}): "
          f"{len(blocked)} blocked of {len(done)} sites; testing {len(sample)}")
    print("before:", dict(collections.Counter(done[k].get("error") for k in sample).most_common(8)))
    sites = [{"id": k, **urls[k]} for k in sample]
    if WHICH != "blocked":
        t = time.time()
        plain = await server.crawl_many(sites, job["params"].get("max_pages", 10), concurrency=20)
        ok = [r for r in plain if not r.get("error")]
        print(f"normal crawler now: {len(ok)}/{len(plain)} loaded, {sum(1 for r in plain if r['emails'])} with an email "
              f"({time.time() - t:.0f}s); still failing: {dict(collections.Counter(r['error'] for r in plain if r.get('error')).most_common(6))}")
        # where do the failing sites live? many on one network = that host is refusing our IP
        res = server.SharedResolver()
        blocks, ips = collections.Counter(), {}
        for st in sites:
            b = await res.block_of(st["url"])
            blocks[b or "no IPv4 address"] += 1
            ips[st["url"]] = b
        await res.close()
        print("failing sites by network (/24):", dict(blocks.most_common(8)))
        print("open a few of these in your own browser; if they load there, the VPS's IP is being refused:")
        for st in sites[:8]:
            print("   ", st["url"], "->", ips[st["url"]])
    t = time.time()
    res = await server.crawl_many(sites, job["params"].get("max_pages", 10), concurrency=server.BLOCKED_CONCURRENCY,
                                  page_timeout=server.RETRY_TIMEOUT, browser=True)
    loaded = [r for r in res if not r.get("error")]
    got = [r for r in res if r["emails"]]
    print(f"as Chrome: {len(loaded)}/{len(res)} loaded, {len(got)} with an email ({time.time() - t:.0f}s)")
    print("still refused:", dict(collections.Counter(f"{r['error']}@{r.get('blocker') or '?'}" for r in res if r.get("error")).most_common(10)))
    for r in got[:10]:
        print("  ", r["url"], "->", ", ".join(r["emails"][:2]))


if __name__ == "__main__":
    asyncio.run(main())
