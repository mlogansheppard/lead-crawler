import asyncio
import csv
import re
import io
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from aiohttp import web  # noqa: E402
from aiohttp.test_utils import TestClient, TestServer  # noqa: E402

import extract  # noqa: E402
import mxcheck  # noqa: E402
import server  # noqa: E402

mxcheck.ENABLED = False  # no real DNS in tests


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
    assert ranked == ["b@acme.com", "a@gmail.com"]  # other-domain dropped when better ones exist
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


def test_email_job(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "DATA_DIR", str(tmp_path))

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


def test_maps_job(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "DATA_DIR", str(tmp_path))

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
                {"type": "maps_search", "title": "Perfect Co", "cid": "333", "url": base, "rating_distribution": {"1": 0, "5": 40},
                 "address": "2 Main St, Austin, TX 78701", "category": "Plumber"},
                {"type": "maps_search", "title": "Acme Plumbing LLC", "cid": "111", "url": base, "phone": "+1 512",
                 "rating_distribution": {"1": 3, "5": 10}, "contact_url": base + "contact-us",
                 "address": "1 Main St, Austin, TX 78701", "address_info": {"address": "1 Main St", "city": "Austin", "zip": "78701"},
                 "category": "Plumber", "additional_categories": ["Plumber", "Drainage service"], "rating": {"value": 4.1, "votes_count": 20}},
            ]
            if tid == "t1":
                items.append({"type": "maps_search", "title": "No Site Co", "cid": "222", "address": "", "category": "Plumber"})
            return web.json_response({"tasks": [{"status_code": 20000, "result": [{"items": items}]}]})

        rposted = []

        async def r_task_post(r):
            body = await r.json()
            rposted.extend(body)
            return web.json_response({"tasks": [{"id": f"r{i}", "status_code": 20100, "cost": 0.00075} for i, b in enumerate(body)]})

        async def r_task_get(r):
            items = [{"rating": {"value": 1}, "review_text": "", "profile_name": "NoText"}]
            items += [{"rating": {"value": 1}, "review_text": f"bad {i}", "profile_name": f"U{i}",
                       "images": [{"image_url": f"https://img/{i}.jpg", "url": "x"}] if i == 0 else None} for i in range(7)]
            items += [{"rating": {"value": 2}, "review_text": "meh", "profile_name": "Two"}]
            return web.json_response({"tasks": [{"status_code": 20000, "result": [{"items": items}]}]})

        dfs = web.Application()
        dfs.router.add_post("/r/task_post", r_task_post)
        dfs.router.add_get("/r/task_get/{tid}", r_task_get)
        dfs.router.add_post("/task_post", task_post)
        dfs.router.add_get("/task_get/advanced/{tid}", task_get)
        dfs_srv = TestServer(dfs)
        await dfs_srv.start_server()
        monkeypatch.setattr(server.maps, "DFS_BASE", f"http://127.0.0.1:{dfs_srv.port}")
        monkeypatch.setattr(server.maps, "DFS_REVIEWS_BASE", f"http://127.0.0.1:{dfs_srv.port}/r")
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
        rtext = await (await client.get(f"/jobs/{jid}/reviews.csv")).text()
        await client.close()
        await site.close()
        await dfs_srv.close()
        return posted, j, text, rposted, rtext

    posted, j, text, rposted, rtext = asyncio.run(run())
    assert posted[0]["keyword"] == "plumber in Austin, TX 78701" and posted[0]["depth"] == 200
    assert j["status"] == "done", j
    rows = list(csv.DictReader(io.StringIO(text)))
    assert len(rows) == 3  # deduped by CID, ad skipped
    acme = [r for r in rows if r["CID"] == "111"][0]
    assert acme["EMAIL"] == "owner@gmail.com" and acme["STATE CODE"] == "TX"
    assert acme["URL"] == "https://maps.google.com/?cid=111"
    assert acme["CATEGORY"] == "Plumber, Drainage service"
    assert j["stats"]["maps"]["ads_skipped"] == 2
    # reviews: only the business with an email, sorted lowest, 5 kept, 1-star only, text first
    assert len(rposted) == 1 and rposted[0]["cid"] == "111" and rposted[0]["sort_by"] == "lowest_rating"  # 333 has no 1-star ratings
    assert acme["ONE STAR RATINGS"] == "3" and j["stats"]["reviews_skipped_no_1star"] == 1
    revs = list(csv.DictReader(io.StringIO(rtext)))
    assert len(revs) == 5 and all(r["SCORE"] == "1" and r["TEXT"] for r in revs), revs
    assert revs[0]["TEXT"] == "bad 0" and revs[0]["PICTURES"] == "https://img/0.jpg"
    assert j["review_rows"] == 5


_real_sleep = asyncio.sleep


async def _fast_sleep(_s):
    await _real_sleep(0)


def test_pause_on_no_balance_then_resume_after_restart(monkeypatch, tmp_path):
    """DataForSEO says 'insufficient funds' partway through. The job pauses, keeps what it paid for,
    survives a crawler restart, and finishes without re-posting tasks that were already accepted."""
    async def run():
        site, base = await _start_site()
        state = {"broke": True}
        posted_keywords = []
        reviews_posted = []

        async def task_post(r):
            body = await r.json()
            # first chunk is accepted; while "broke", the next chunk is refused
            if state["broke"] and posted_keywords:
                return web.json_response({"status_code": 40210, "status_message": "Insufficient funds.", "tasks": []}, status=402)
            out = []
            for b in body:
                posted_keywords.append(b["keyword"])
                out.append({"id": "m%d" % len(posted_keywords), "status_code": 20100, "cost": 0.0006})
            return web.json_response({"status_code": 20000, "tasks": out})

        async def task_get(r):
            n = r.match_info["tid"]
            return web.json_response({"tasks": [{"status_code": 20000, "result": [{"items": [
                {"type": "maps_search", "title": "Biz " + n, "cid": n, "url": base, "address": "1 Main St, Austin, TX 78701",
                 "address_info": {"address": "1 Main St"}, "category": "Plumber"}]}]}]})

        async def r_task_post(r):
            body = await r.json()
            reviews_posted.extend(body)
            return web.json_response({"tasks": [{"id": "r" + b["cid"], "status_code": 20100} for b in body]})

        async def r_task_get(r):
            return web.json_response({"tasks": [{"status_code": 20000, "data": {"sort_by": "lowest_rating"},
                                                 "result": [{"items": [{"rating": {"value": 1}, "review_text": "awful"}]}]}]})

        dfs = web.Application()
        dfs.router.add_post("/task_post", task_post)
        dfs.router.add_get("/task_get/advanced/{tid}", task_get)
        dfs.router.add_post("/r/task_post", r_task_post)
        dfs.router.add_get("/r/task_get/{tid}", r_task_get)
        dfs_srv = TestServer(dfs)
        await dfs_srv.start_server()
        monkeypatch.setattr(server.maps, "DFS_BASE", f"http://127.0.0.1:{dfs_srv.port}")
        monkeypatch.setattr(server.maps, "DFS_REVIEWS_BASE", f"http://127.0.0.1:{dfs_srv.port}/r")
        monkeypatch.setattr(server.maps, "DFS_LOGIN", "x")
        monkeypatch.setattr(server.maps, "POLL_SECONDS", 0)
        monkeypatch.setattr(server.maps.asyncio, "sleep", _fast_sleep)
        monkeypatch.setattr(server, "DATA_DIR", str(tmp_path))
        monkeypatch.setattr(server, "RESUME_EVERY", 3600)
        server.JOBS.clear()

        # 150 city tasks -> 2 post chunks (100 + 50); the second one hits "insufficient funds"
        tasks = [{"city": f"Town{i} 7{i:04d}", "category": "plumber", "state_code": "TX"} for i in range(150)]
        client = TestClient(TestServer(server.make_app()))
        await client.start_server()
        jid = (await (await client.post("/maps-jobs", json={"tasks": tasks, "job_key": "plumber|g2"})).json())["job_id"]
        j = await _wait(client, jid)
        assert j["status"] == "paused" and "40210" in j["pause_reason"], j
        assert len(posted_keywords) == 100
        await client.close()  # crawler stops

        # balance topped up, crawler restarts: it reloads the saved job from disk and continues by itself
        state["broke"] = False
        server.JOBS.clear()
        client = TestClient(TestServer(server.make_app()))
        await client.start_server()
        j = await _wait(client, jid)
        # re-sending the same job_key hands back the same job instead of paying again
        again = await (await client.post("/maps-jobs", json={"tasks": tasks, "job_key": "plumber|g2"})).json()
        text = await (await client.get(f"/jobs/{jid}/csv")).text()
        rtext = await (await client.get(f"/jobs/{jid}/reviews.csv")).text()
        await client.close()
        await site.close()
        await dfs_srv.close()
        return j, again, text, rtext, posted_keywords, reviews_posted

    j, again, text, rtext, posted_keywords, reviews_posted = asyncio.run(run())
    assert j["status"] == "done", j
    assert len(posted_keywords) == 150 and len(set(posted_keywords)) == 150  # nothing posted twice
    assert again["job_id"] and again["reused"] is True
    rows = list(csv.DictReader(io.StringIO(text)))
    assert len(rows) == 150 and all(r["EMAIL"] == "owner@gmail.com" for r in rows)
    assert all(b["sort_by"] == "lowest_rating" for b in reviews_posted) and len(reviews_posted) == 150
    assert len(list(csv.DictReader(io.StringIO(rtext)))) == 150


def test_locations():
    cities, code = server.state_cities("Texas")
    assert code == "TX" and len(cities) > 1500
    assert any(c.startswith("Austin 787") for c in cities)
    assert all(re.match(r"^.+ \d{5}$", c) for c in cities)
    assert server.state_cities("new york")[1] == "NY"


def test_adaptive_splits_only_full_areas_and_resumes(monkeypatch, tmp_path):
    """Delaware: county searches that come back full get split into cities/ZIPs, sparse ones don't.
    A balance problem in round 2 pauses the job; after a restart it finishes without re-posting anything."""
    plan = server.maps.build_plan(["DE"], "plumber")
    roots = plan["roots"]
    full_root = next(r for r in roots if plan["nodes"][r]["children"])  # a county we'll make "full"
    expected_round2 = len(plan["nodes"][full_root]["children"])
    empty_root = next(r for r in roots if r != full_root)  # this county returns "40102 No Search Results"

    async def run():
        site, base = await _start_site()
        state = {"broke_after": len(roots) + 1}  # refuse posts once round 2 has started
        posted = []
        by_id = {}

        async def task_post(r):
            body = await r.json()
            if len(posted) >= state["broke_after"]:
                return web.json_response({"status_code": 40210, "status_message": "Insufficient funds.", "tasks": []}, status=402)
            out = []
            for b in body:
                if len(posted) >= state["broke_after"]:
                    out.append({"status_code": 40210, "status_message": "Insufficient funds."})
                    continue
                posted.append(b["keyword"])
                tid = "t%d" % len(posted)
                by_id[tid] = b
                out.append({"id": tid, "status_code": 20100, "cost": 0.0006})
            return web.json_response({"status_code": 20000, "tasks": out})

        async def task_get(r):
            b = by_id[r.match_info["tid"]]
            if b["tag"] == empty_root:
                return web.json_response({"tasks": [{"status_code": 40102, "status_message": "No Search Results."}]})
            n = 120 if b["tag"] == full_root else 2
            items = [{"type": "maps_search", "title": f"{b['keyword']} #{i}", "cid": f"{b['tag']}-{i}", "url": base,
                      "address": "1 Main St, Dover, DE 19901", "category": "Plumber"} for i in range(n)]
            return web.json_response({"tasks": [{"status_code": 20000, "result": [{"items": items}]}]})

        async def r_task_post(r):
            body = await r.json()
            return web.json_response({"tasks": [{"id": "r" + b["cid"], "status_code": 20100} for b in body]})

        async def r_task_get(r):
            return web.json_response({"tasks": [{"status_code": 20000, "result": [{"items": []}]}]})

        dfs = web.Application()
        dfs.router.add_post("/task_post", task_post)
        dfs.router.add_get("/task_get/advanced/{tid}", task_get)
        dfs.router.add_post("/r/task_post", r_task_post)
        dfs.router.add_get("/r/task_get/{tid}", r_task_get)
        dfs_srv = TestServer(dfs)
        await dfs_srv.start_server()
        monkeypatch.setattr(server.maps, "DFS_BASE", f"http://127.0.0.1:{dfs_srv.port}")
        monkeypatch.setattr(server.maps, "DFS_REVIEWS_BASE", f"http://127.0.0.1:{dfs_srv.port}/r")
        monkeypatch.setattr(server.maps, "DFS_LOGIN", "x")
        monkeypatch.setattr(server.maps, "POLL_SECONDS", 0)
        monkeypatch.setattr(server.maps.asyncio, "sleep", _fast_sleep)
        monkeypatch.setattr(server, "DATA_DIR", str(tmp_path))
        monkeypatch.setattr(server, "RESUME_EVERY", 3600)
        server.JOBS.clear()

        client = TestClient(TestServer(server.make_app()))
        await client.start_server()
        body = {"strategy": "adaptive", "category": "plumber", "states": ["Delaware"], "split_at": 100}
        jid = (await (await client.post("/maps-jobs", json=body)).json())["job_id"]
        j1 = await _wait(client, jid)
        await client.close()

        state["broke_after"] = 10 ** 9  # topped up
        server.JOBS.clear()
        client = TestClient(TestServer(server.make_app()))
        await client.start_server()
        j2 = await _wait(client, jid)
        text = await (await client.get(f"/jobs/{jid}/csv")).text()
        await client.close()
        await site.close()
        await dfs_srv.close()
        return j1, j2, posted, text

    j1, j2, posted, text = asyncio.run(run())
    assert j1["status"] == "paused" and j1["stats"]["maps"]["round"] == 2, j1
    assert j2["status"] == "done", j2
    m = j2["stats"]["maps"]
    assert [r["searches"] for r in m["rounds"]][:2] == [len(roots), expected_round2], m["rounds"]
    assert m["rounds"][0]["split"] == 1
    assert len(posted) == len(set(posted)) == len(roots) + expected_round2  # nothing searched twice
    assert posted[0].startswith("plumber in ") and "County, DE" in posted[0]
    rows = list(csv.DictReader(io.StringIO(text)))
    assert len(rows) == 120 + 2 * (len(roots) - 2) + 2 * expected_round2
    assert m["rounds"][0]["searches"] == len(roots) and j2["stats"]["maps"]["rounds"][0]["unique_places"] > 0


def test_account_problem_codes():
    assert not server.maps.is_account_problem(40102, "No Search Results.")
    assert server.maps.is_account_problem(40210, "Insufficient funds.")
    assert server.maps.is_account_problem(40200, "Payment Required.")
    assert server.maps.is_account_problem(40100, "You are not authorized to access this resource.")
    assert not server.maps.is_account_problem(40501, "Invalid Field")
    assert server.maps._pause_reason(200, {"status_code": 40102, "status_message": "No Search Results."}) is None


def test_finds_builder_and_obfuscated_emails():
    wix = '<html><body><div id="app"></div><script>window.__DATA__={"contact":{"mail":"owner\\u0040acmeroofing.com"}}</script></body></html>'
    assert "owner@acmeroofing.com" in extract.extract_emails(wix)
    assert "office@acmeroofing.com" in extract.extract_emails("<p>Email: office @ acmeroofing.com</p>")
    assert "sam@acmeroofing.com" in extract.extract_emails('<a href="mailto:sam%40acmeroofing.com">mail</a>')
    junk = '<script>Sentry.init({dsn:"https://abc123def4567890abc123def4567890@o1.ingest.sentry.io/1"})</script><img srcset="logo@2x.png 2x">'
    assert extract.extract_emails(junk) == []


def test_email_ranking_and_role_drops():
    emails = ["support@agencyweb.com", "billing@acme.com", "acmeplumb@gmail.com", "info@acme.com",
              "careers@acme.com", "privacy@acme.com", "john.smith@acme.com", "webmaster@acme.com"]
    typed = extract.rank_emails_typed(emails, "https://www.acme.com/")
    assert typed == [("john.smith@acme.com", "personal"), ("info@acme.com", "role"), ("acmeplumb@gmail.com", "free"),
                     ("billing@acme.com", "low")], typed


def test_sitemap_links():
    xml = "<urlset><url><loc>https://acme.com/services</loc></url><url><loc>https://acme.com/contact-acme</loc></url><url><loc>https://other.com/contact</loc></url></urlset>"
    pages, children = extract.sitemap_links(xml, "https://acme.com/")
    assert pages == ["https://acme.com/contact-acme"] and children == []
    pages, children = extract.sitemap_links("<sitemapindex><sitemap><loc>https://acme.com/page-sitemap.xml</loc></sitemap></sitemapindex>", "https://acme.com/")
    assert children == ["https://acme.com/page-sitemap.xml"]


def test_sitemap_fallback_finds_hidden_contact_page(monkeypatch, tmp_path):
    async def run():
        async def home(r):
            return web.Response(text="<html><body><nav id=menu></nav></body></html>", content_type="text/html")

        async def sitemap(r):
            base = f"http://127.0.0.1:{r.url.port}"
            return web.Response(text=f"<urlset><url><loc>{base}/reach-our-team</loc></url></urlset>", content_type="application/xml")

        async def page(r):
            return web.Response(text="Write to dana@gmail.com", content_type="text/html")
        app = web.Application()
        app.router.add_get("/", home)
        app.router.add_get("/sitemap.xml", sitemap)
        app.router.add_get("/reach-our-team", page)
        srv = TestServer(app)
        await srv.start_server()
        res = await server.crawl_many([{"id": "a", "url": f"http://127.0.0.1:{srv.port}/"}], 10)
        await srv.close()
        return res[0]
    r = asyncio.run(run())
    assert r["emails"] == ["dana@gmail.com"] and r["types"] == ["free"], r


def test_agency_and_dead_domain_filter(monkeypatch):
    async def fake_dead(domains):
        return {"deadshop.com"}
    monkeypatch.setattr(server.mxcheck, "dead_domains", fake_dead)
    done = {
        "a.com": {"emails": ["info@a.com", "hello@pixelagency.com"], "types": ["role", "other"], "facebook": ""},
        "b.com": {"emails": ["bob@b.com", "hello@pixelagency.com"], "types": ["personal", "other"], "facebook": ""},
        "c.com": {"emails": ["team@pixelagency.com"], "types": ["other"], "facebook": ""},
        "deadshop.com": {"emails": ["info@deadshop.com", "deadshop@gmail.com"], "types": ["role", "free"], "facebook": ""},
    }
    stats = asyncio.run(server.clean_job_emails(done))
    assert done["a.com"]["emails"] == ["info@a.com"] and done["b.com"]["emails"] == ["bob@b.com"]
    assert done["c.com"]["emails"] == [] and done["deadshop.com"]["emails"] == ["deadshop@gmail.com"]
    assert stats["agency_or_template"] == 3 and stats["dead_domain"] == 1 and stats["agency_domains"] == ["pixelagency.com"]


def test_real_run_fixes():
    # escaped JSON leftovers are cleaned and deduped
    assert extract.clean_email("info@oramedicalspa.com\\") == "info@oramedicalspa.com"
    assert extract.extract_emails('<script>{"e":"hello@skinclinic.com\\"}</script><a href="mailto:hello@skinclinic.com">x</a>') == ["hello@skinclinic.com"]
    # every Square/Wix/GoDaddy subdomain is its own business
    assert extract.registrable("glowmedspa.square.site") == "glowmedspa.square.site"
    assert extract.registrable("www.lashbar.wixsite.com") == "lashbar.wixsite.com"
    assert extract.registrable("shop.acme.com") == "acme.com"
    # booking platform addresses are not the business's email
    assert extract.clean_email("support@vagaro.com") == "" and extract.clean_email("help@glossgenius.com") == ""
    # other-domain addresses only survive when nothing better exists
    typed = extract.rank_emails_typed(["susan.bruno@coastalcontours.com", "hello@alan-lang.com"], "https://coastalcontours.com/")
    assert typed == [("susan.bruno@coastalcontours.com", "personal")]
    assert extract.rank_emails_typed(["hello@alan-lang.com"], "https://coastalcontours.com/") == [("hello@alan-lang.com", "other")]


def test_job_cleanup_fixes_old_results(monkeypatch):
    async def none(domains):
        return set()
    monkeypatch.setattr(server.mxcheck, "dead_domains", none)
    done = {"oramedicalspa.com": {"emails": ["info@oramedicalspa.com", "info@oramedicalspa.com\\", "hi@designer.com"],
                                  "types": ["role", "role", "other"], "facebook": ""}}
    asyncio.run(server.clean_job_emails(done))
    assert done["oramedicalspa.com"]["emails"] == ["info@oramedicalspa.com"] and done["oramedicalspa.com"]["types"] == ["role"]


def test_queue_limits_parallel_jobs(monkeypatch, tmp_path):
    """5 sheet rows at once: only MAX_ACTIVE_JOBS run, the rest queue and start by themselves."""
    async def run():
        site, base = await _start_site()
        gate = asyncio.Event()
        seen_running = []

        async def task_post(r):
            body = await r.json()
            return web.json_response({"tasks": [{"id": b["keyword"], "status_code": 20100} for b in body]})

        async def task_get(r):
            await gate.wait()
            return web.json_response({"tasks": [{"status_code": 20000, "result": [{"items": []}]}]})

        dfs = web.Application()
        dfs.router.add_post("/task_post", task_post)
        dfs.router.add_get("/task_get/advanced/{tid}", task_get)
        dfs_srv = TestServer(dfs)
        await dfs_srv.start_server()
        monkeypatch.setattr(server.maps, "DFS_BASE", f"http://127.0.0.1:{dfs_srv.port}")
        monkeypatch.setattr(server.maps, "DFS_LOGIN", "x")
        monkeypatch.setattr(server.maps, "POLL_SECONDS", 0)
        monkeypatch.setattr(server.maps.asyncio, "sleep", _fast_sleep)
        monkeypatch.setattr(server, "DATA_DIR", str(tmp_path))
        monkeypatch.setattr(server, "MAX_ACTIVE_JOBS", 2)
        server.JOBS.clear()
        client = TestClient(TestServer(server.make_app()))
        await client.start_server()
        ids = []
        for cat in ["a", "b", "c", "d", "e"]:
            r = await client.post("/maps-jobs", json={"tasks": [{"city": "Austin 78701", "category": cat, "state_code": "TX"}],
                                                      "reviews": {"enabled": False}})
            ids.append((await r.json())["job_id"])
        await asyncio.sleep(0.05)
        st = [(await (await client.get(f"/jobs/{i}")).json())["status"] for i in ids]
        seen_running.append(st)
        gate.set()
        for i in ids:
            for _ in range(200):
                if (await (await client.get(f"/jobs/{i}")).json())["status"] == "done":
                    break
                await asyncio.sleep(0.02)
        final = [(await (await client.get(f"/jobs/{i}")).json())["status"] for i in ids]
        await client.close()
        await site.close()
        await dfs_srv.close()
        return seen_running[0], final
    first, final = asyncio.run(run())
    assert first.count("running") == 2 and first.count("queued") == 3, first
    assert final == ["done"] * 5, final


def test_no_cut_off_fragments_near_other_at_signs():
    html = '<li>' + 'x' * 60 + ' mary-ann@gmail.com, sales@bestroofing.net</li><img srcset="logo@2x.png 2x"><p>john.smith@gmail.com</p><style>@media(max-width:600px){.a{color:red}}</style>'
    assert sorted(extract.extract_emails(html)) == ["john.smith@gmail.com", "mary-ann@gmail.com", "sales@bestroofing.net"]
