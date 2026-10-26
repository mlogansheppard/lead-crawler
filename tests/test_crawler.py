import asyncio
import csv
import io
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from aiohttp import web  # noqa: E402
from aiohttp.test_utils import TestClient, TestServer  # noqa: E402

import extract  # noqa: E402
import server  # noqa: E402


def test_extract_variants():
    page = """
    <html><head><script type="application/ld+json">{"@type":"LocalBusiness","email":"mailto:office@acmeplumbing.com"}</script>
    <style>.x{background:url(logo@2x.png)}</style></head>
    <body>
      <a href="mailto:info@acmeplumbing.com?subject=hi">Email us</a>
      <span class="__cf_email__" data-cfemail="%s">[email&#160;protected]</span>
      Reach Bob at bob [at] gmail [dot] com or BILLING@ACMEPLUMBING.COM.
      <img src="hero@2x.jpg"> noreply@acmeplumbing.com 5f1e2d3c4b5a69788796a5b4c3d2e1f0@sentry.wixpress.com
      user@example.com
    </body></html>
    """ % ("".join("%02x" % b for b in [0x42] + [ord(c) ^ 0x42 for c in "jobs@acmeplumbing.com"]))
    got = extract.extract_emails(page)
    assert set(got) == {"info@acmeplumbing.com", "jobs@acmeplumbing.com", "office@acmeplumbing.com",
                        "billing@acmeplumbing.com", "bob@gmail.com"}, got


def test_rank_and_normalize():
    ranked = extract.rank_emails(["x@agency.com", "a@gmail.com", "b@acme.com"], "https://www.acme.com/")
    assert ranked == ["b@acme.com", "a@gmail.com", "x@agency.com"]
    assert extract.normalize_site("facebook.com/acme") == ""
    assert extract.normalize_site("acme.com") == "http://acme.com/"
    assert extract.normalize_site("https://WWW.Acme.com/home") == "https://www.acme.com/home"


def test_contact_links():
    page = '<a href="/about-us">About</a><a href="/services">Services</a><a href="https://acme.com/contact">Get in touch</a><a href="https://other.com/contact">x</a>'
    links = extract.contact_links(page, "https://acme.com/")
    assert links[0] == "https://acme.com/contact"
    assert "https://acme.com/about-us" in links
    assert all("other.com" not in u and "services" not in u for u in links)


async def _start_site():
    async def home(r):
        return web.Response(text='<a href="/contact-us">Contact</a><a href="https://facebook.com/acmeplumb">fb</a>', content_type="text/html")

    async def contact(r):
        return web.Response(text="Call or email owner@gmail.com", content_type="text/html")
    site = web.Application()
    site.router.add_get("/", home)
    site.router.add_get("/contact-us", contact)
    srv = TestServer(site)
    await srv.start_server()
    return srv, f"http://127.0.0.1:{srv.port}/"


async def _wait(client, job):
    for _ in range(200):
        j = await (await client.get(f"/jobs/{job}")).json()
        if j["status"] != "running":
            return j
        await asyncio.sleep(0.05)
    raise AssertionError("job did not finish")


def test_email_job():
    async def run():
        site, base = await _start_site()
        client = TestClient(TestServer(server.make_app()))
        await client.start_server()
        r = await client.post("/jobs", json={"sites": [{"id": "123", "url": base}, {"id": "9", "url": "https://yelp.com/biz/x"}]})
        j = await _wait(client, (await r.json())["job_id"])
        out = await (await client.get(f"/jobs/{(await r.json())['job_id']}/csv")).json()
        await client.close()
        await site.close()
        return j, out

    j, out = asyncio.run(run())
    assert j["status"] == "done"
    by = {r["id"]: r for r in out["results"]}
    assert by["123"]["emails"] == ["owner@gmail.com"], by["123"]
    assert by["123"]["facebook"] == "https://facebook.com/acmeplumb"
    assert by["9"]["error"] == "skipped"


def test_maps_job(monkeypatch):
    async def run():
        site, base = await _start_site()
        posted = []

        async def task_post(r):
            body = await r.json()
            posted.extend(body)
            return web.json_response({"tasks": [{"id": f"t{i}", "status_code": 20100, "cost": 0.0006, "data": {"tag": b["tag"]}} for i, b in enumerate(body)]})

        calls = {"n": 0}

        async def task_get(r):
            calls["n"] += 1
            tid = r.match_info["tid"]
            if calls["n"] == 1:
                return web.json_response({"tasks": [{"status_code": 40602}]})
            items = [
                {"type": "maps_paid_item", "title": "Ad"},
                {"type": "maps_search", "title": "Acme Plumbing LLC", "cid": "111", "url": base, "phone": "+1 512",
                 "address": "1 Main St, Austin, TX 78701", "address_info": {"address": "1 Main St", "city": "Austin", "zip": "78701"},
                 "category": "Plumber", "additional_categories": ["Plumber", "Drainage service"], "rating": {"value": 4.1, "votes_count": 20}},
            ]
            if tid == "t1":
                items.append({"type": "maps_search", "title": "No Site Co", "cid": "222", "address": "", "category": "Plumber"})
            return web.json_response({"tasks": [{"status_code": 20000, "result": [{"items": items}]}]})

        dfs = web.Application()
        dfs.router.add_post("/task_post", task_post)
        dfs.router.add_get("/task_get/advanced/{tid}", task_get)
        dfs_srv = TestServer(dfs)
        await dfs_srv.start_server()
        monkeypatch.setattr(server.maps, "DFS_BASE", f"http://127.0.0.1:{dfs_srv.port}")
        monkeypatch.setattr(server.maps, "DFS_LOGIN", "x")
        monkeypatch.setattr(server.maps, "POLL_SECONDS", 0)
        monkeypatch.setattr(server.maps.asyncio, "sleep", _fast_sleep)

        client = TestClient(TestServer(server.make_app()))
        await client.start_server()
        r = await client.post("/maps-jobs", json={"tasks": [{"city": "Austin 78701", "category": "plumber", "state_code": "TX"},
                                                            {"city": "Round Rock 78664", "category": "plumber", "state_code": "TX"}]})
        jid = (await r.json())["job_id"]
        j = await _wait(client, jid)
        text = await (await client.get(f"/jobs/{jid}/csv")).text()
        await client.close()
        await site.close()
        await dfs_srv.close()
        return posted, j, text

    posted, j, text = asyncio.run(run())
    assert posted[0]["keyword"] == "plumber in Austin, TX 78701" and posted[0]["depth"] == 200
    assert j["status"] == "done", j
    rows = list(csv.DictReader(io.StringIO(text)))
    assert len(rows) == 2  # deduped by CID, ad skipped
    acme = [r for r in rows if r["CID"] == "111"][0]
    assert acme["EMAIL"] == "owner@gmail.com" and acme["STATE CODE"] == "TX"
    assert acme["URL"] == "https://maps.google.com/?cid=111"
    assert acme["CATEGORY"] == "Plumber, Drainage service"
    assert j["stats"]["maps"]["ads_skipped"] == 2


_real_sleep = asyncio.sleep


async def _fast_sleep(_s):
    await _real_sleep(0)
