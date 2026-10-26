import asyncio
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


def test_end_to_end():
    async def run():
        site = web.Application()
        site.router.add_get("/", lambda r: web.Response(text='<a href="/contact-us">Contact</a><a href="https://facebook.com/acmeplumb">fb</a>', content_type="text/html"))
        site.router.add_get("/contact-us", lambda r: web.Response(text="Call or email hello@127.0.0.1x.com or owner@gmail.com", content_type="text/html"))
        site_server = TestServer(site)
        await site_server.start_server()
        base = f"http://127.0.0.1:{site_server.port}/"

        os.environ["API_KEY"] = ""
        client = TestClient(TestServer(server.make_app()))
        await client.start_server()
        r = await client.post("/jobs", json={"sites": [{"id": "123", "url": base}, {"id": "9", "url": "https://yelp.com/biz/x"}]})
        job = (await r.json())["job_id"]
        for _ in range(100):
            j = await (await client.get(f"/jobs/{job}")).json()
            if j["status"] == "done":
                break
            await asyncio.sleep(0.1)
        await client.close()
        await site_server.close()
        return j

    j = asyncio.run(run())
    assert j["status"] == "done" and j["total"] == 2
    by = {r["id"]: r for r in j["results"]}
    assert "owner@gmail.com" in by["123"]["emails"], by["123"]
    assert by["123"]["facebook"] == "https://facebook.com/acmeplumb"
    assert by["9"]["error"] == "skipped"
