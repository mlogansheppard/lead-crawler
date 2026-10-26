"""Pure email/link extraction helpers (no network). Kept separate so they can be unit tested."""
import html
import json
import re
from urllib.parse import urljoin, urlparse

EMAIL_RE = re.compile(r"(?<![\w.+-])([a-z0-9][a-z0-9._%+-]{0,63}@[a-z0-9-]+(?:\.[a-z0-9-]+)*\.[a-z]{2,24})(?![\w-])", re.I)
MAILTO_RE = re.compile(r"mailto:([^\"'?>\s]+)", re.I)
CFEMAIL_RE = re.compile(r"data-cfemail=[\"']([0-9a-fA-F]+)[\"']")
CF_HREF_RE = re.compile(r"/cdn-cgi/l/email-protection#([0-9a-fA-F]+)")
HREF_RE = re.compile(r"<a\b[^>]*?href\s*=\s*[\"']([^\"'#]+)[\"'][^>]*>(.*?)</a>", re.I | re.S)
TAG_RE = re.compile(r"<[^>]+>")
SCRIPT_STYLE_RE = re.compile(r"<(script|style|noscript)\b[^>]*>.*?</\1>", re.I | re.S)
JSONLD_RE = re.compile(r"<script[^>]+application/ld\+json[^>]*>(.*?)</script>", re.I | re.S)
OBFUSCATED_RE = re.compile(
    r"([a-z0-9._%+-]+)\s*(?:\[at\]|\(at\)|\{at\}|\sat\s)\s*([a-z0-9-]+)\s*(?:\[dot\]|\(dot\)|\{dot\}|\sdot\s)\s*([a-z]{2,24})",
    re.I,
)

CONTACT_HINT_RE = re.compile(r"contact|about|team|staff|our-?people|imprint|impressum|legal|get-?in-?touch|reach-?us|location|privacy", re.I)
GUESSED_PATHS = ["/contact", "/contact-us", "/about", "/about-us"]

BAD_EXT = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".bmp", ".ico", ".css", ".js", ".avif", ".tif", ".tiff", ".pdf", ".mp4", ".woff", ".woff2")
BAD_DOMAINS = {
    "example.com", "example.org", "domain.com", "email.com", "yourdomain.com", "yoursite.com", "company.com",
    "sentry.io", "sentry.wixpress.com", "sentry-next.wixpress.com", "wixpress.com", "wix.com", "godaddy.com",
    "squarespace.com", "mysite.com", "website.com", "test.com", "address.com", "mail.com", "sample.com",
    "schema.org", "w3.org", "googlemail.co", "latofonts.com", "typemade.mx", "indiantypefoundry.com",
}
BAD_DOMAIN_SUFFIXES = (".wixpress.com", ".sentry.io", ".ingest.sentry.io")
BAD_LOCAL = re.compile(r"^(no-?reply|do-?not-?reply|donotreply|mailer-daemon|postmaster|abuse|user|name|email|your-?email|yourname|firstname|first\.last|john\.?doe|jane\.?doe|test|example)$", re.I)
HEXHASH_LOCAL = re.compile(r"^[0-9a-f]{16,}$", re.I)
FREE_PROVIDERS = {"gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "aol.com", "icloud.com", "live.com", "msn.com", "comcast.net", "att.net", "sbcglobal.net", "verizon.net", "bellsouth.net", "me.com", "protonmail.com", "ymail.com", "cox.net", "charter.net"}

SKIP_SITE_DOMAINS = (
    "facebook.com", "instagram.com", "yelp.com", "linktr.ee", "google.com", "business.site", "g.page",
    "youtube.com", "twitter.com", "x.com", "tiktok.com", "linkedin.com", "nextdoor.com", "angi.com",
    "homeadvisor.com", "thumbtack.com", "bbb.org", "yellowpages.com", "mapquest.com", "pinterest.com",
)


def decode_cfemail(hexstr: str) -> str:
    try:
        key = int(hexstr[:2], 16)
        return "".join(chr(int(hexstr[i:i + 2], 16) ^ key) for i in range(2, len(hexstr), 2))
    except Exception:
        return ""


def registrable(host: str) -> str:
    host = (host or "").lower().split(":")[0]
    if host.startswith("www."):
        host = host[4:]
    parts = host.split(".")
    # crude eTLD+1, good enough for US small-business sites (handles co.uk style too)
    if len(parts) >= 3 and len(parts[-2]) <= 3 and len(parts[-1]) == 2:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def normalize_site(url: str) -> str:
    """Return a fetchable homepage URL, or '' if the site should be skipped."""
    u = (url or "").strip()
    if not u:
        return ""
    if not re.match(r"^https?://", u, re.I):
        u = "http://" + u
    p = urlparse(u)
    host = (p.hostname or "").lower()
    if not host or "." not in host:
        return ""
    reg = registrable(host)
    if any(reg == d or host.endswith("." + d) for d in SKIP_SITE_DOMAINS):
        return ""
    netloc = host + (f":{p.port}" if p.port else "")
    return f"{p.scheme.lower()}://{netloc}{p.path or '/'}"


def clean_email(e: str) -> str:
    e = html.unescape(e or "").strip().strip(".,;:()[]<>\"'").lower()
    e = re.sub(r"^(?:%20|u003e|mailto:)+", "", e)
    if e.count("@") != 1 or len(e) > 100:
        return ""
    local, dom = e.split("@")
    if not local or not dom or ".." in e:
        return ""
    if dom.endswith(BAD_EXT) or e.endswith(BAD_EXT) or re.search(r"@\d+x\.", e):
        return ""
    if dom in BAD_DOMAINS or dom.endswith(BAD_DOMAIN_SUFFIXES):
        return ""
    if BAD_LOCAL.match(local) or HEXHASH_LOCAL.match(local):
        return ""
    if re.search(r"\d+x\d+", dom):
        return ""
    return e


def _jsonld_emails(raw: str):
    out = []
    for block in JSONLD_RE.findall(raw):
        try:
            data = json.loads(block.strip())
        except Exception:
            continue
        stack = [data]
        while stack:
            cur = stack.pop()
            if isinstance(cur, dict):
                for k, v in cur.items():
                    if k.lower() == "email" and isinstance(v, str):
                        out.append(v.replace("mailto:", ""))
                    elif isinstance(v, (dict, list)):
                        stack.append(v)
            elif isinstance(cur, list):
                stack.extend(cur)
    return out


def extract_emails(raw_html: str) -> list:
    found = []
    found += [m for m in MAILTO_RE.findall(raw_html)]
    found += [decode_cfemail(h) for h in CFEMAIL_RE.findall(raw_html)]
    found += [decode_cfemail(h) for h in CF_HREF_RE.findall(raw_html)]
    found += _jsonld_emails(raw_html)
    text = html.unescape(TAG_RE.sub(" ", SCRIPT_STYLE_RE.sub(" ", raw_html)))
    found += EMAIL_RE.findall(text)
    found += ["{}@{}.{}".format(*m) for m in OBFUSCATED_RE.findall(text)]
    seen, out = set(), []
    for e in found:
        e = clean_email(e.split("?")[0])
        if e and e not in seen:
            seen.add(e)
            out.append(e)
    return out


def contact_links(raw_html: str, base_url: str, limit: int = 12) -> list:
    base_host = registrable(urlparse(base_url).hostname or "")
    scored = []
    for href, label in HREF_RE.findall(raw_html):
        href = html.unescape(href.strip())
        if href.lower().startswith(("mailto:", "tel:", "javascript:", "sms:")):
            continue
        absu = urljoin(base_url, href)
        p = urlparse(absu)
        if p.scheme not in ("http", "https") or registrable(p.hostname or "") != base_host:
            continue
        if p.path.lower().endswith(BAD_EXT):
            continue
        text = TAG_RE.sub(" ", label)
        target = p.path + " " + text
        if CONTACT_HINT_RE.search(target):
            score = 0 if re.search(r"contact", target, re.I) else 1 if re.search(r"about|team|staff", target, re.I) else 2
            scored.append((score, p._replace(fragment="", query="").geturl()))
    seen, out = set(), []
    for _, u in sorted(scored, key=lambda x: x[0]):
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out[:limit]


def rank_emails(emails: list, site_url: str) -> list:
    """Same-domain first, then free providers (gmail etc), then anything else."""
    site_reg = registrable(urlparse(site_url).hostname or "")

    def key(e):
        dom = e.split("@")[1]
        if registrable(dom) == site_reg:
            return 0
        if dom in FREE_PROVIDERS:
            return 1
        return 2

    return sorted(emails, key=key)
