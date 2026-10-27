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

SPACED_AT_RE = re.compile(r"(?<![\w.+-])([a-z0-9][a-z0-9._%+-]{0,63})\s+@\s+([a-z0-9-]+(?:\.[a-z0-9-]+)*\.[a-z]{2,24})(?![\w-])", re.I)
LOC_RE = re.compile(r"<loc>\s*([^<\s]+)\s*</loc>", re.I)

CONTACT_HINT_RE = re.compile(r"contact|about|team|staff|our-?people|imprint|impressum|legal|get-?in-?touch|reach-?us|location|privacy", re.I)
GUESSED_PATHS = ["/contact", "/contact-us", "/contactus", "/about", "/about-us", "/team", "/our-team", "/privacy-policy"]

BAD_EXT = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".bmp", ".ico", ".css", ".js", ".avif", ".tif", ".tiff", ".pdf", ".mp4", ".woff", ".woff2")
BAD_DOMAINS = {
    "example.com", "example.org", "domain.com", "email.com", "yourdomain.com", "yoursite.com", "company.com",
    "sentry.io", "sentry.wixpress.com", "sentry-next.wixpress.com", "wixpress.com", "wix.com", "godaddy.com",
    "mysite.com", "website.com", "test.com", "address.com", "sample.com", "yourcompany.com",
    "yourbusiness.com", "businessname.com", "yourwebsite.com", "emailaddress.com", "site.com", "domainname.com",
    "schema.org", "w3.org", "googlemail.co", "latofonts.com", "typemade.mx", "indiantypefoundry.com",
}
BAD_DOMAIN_SUFFIXES = (".wixpress.com", ".sentry.io", ".ingest.sentry.io")
BAD_LOCAL = re.compile(r"^(no-?reply|do-?not-?reply|donotreply|mailer-daemon|postmaster|abuse|user|username|name|your-?email|yourname|your\.?name|firstname|first\.?last|first_last|lastname|john\.?doe|jane\.?doe|johnsmith|john\.smith|test|example|sample|someone|somebody|you|me|email-?address|emailaddress)$", re.I)
HEXHASH_LOCAL = re.compile(r"^[0-9a-f]{16,}$", re.I)
# role addresses that should never go into a cold email campaign
ROLE_DROP = {
    "careers", "career", "jobs", "job", "hr", "recruiting", "recruitment", "resumes", "resume", "apply", "employment",
    "hiring", "talent", "privacy", "legal", "compliance", "dmca", "copyright", "abuse", "webmaster", "hostmaster",
    "postmaster", "unsubscribe", "optout", "opt-out", "remove", "press", "pr", "ada", "accessibility", "security",
    "noc", "root", "wordpress", "wp", "devnull", "bounce", "bounces", "notifications", "notification", "alerts",
    "newsletter", "news", "donotreply", "no-reply", "noreply", "do-not-reply", "mailer", "daemon", "spam", "test",
}
# fine to email, but less likely to reach a decision maker
ROLE_LOW = {
    "billing", "accounting", "accounts", "accountspayable", "ap", "ar", "invoices", "invoice", "payments", "payment",
    "finance", "marketing", "orders", "order", "returns", "shipping", "parts", "warranty", "claims", "events",
    "donations", "donate", "volunteer", "media", "social", "reviews", "feedback", "careers-team", "admissions",
}
# shared inboxes that a small business owner usually reads
ROLE_GOOD = {
    "info", "contact", "contactus", "office", "hello", "hi", "admin", "sales", "service", "services", "support",
    "team", "mail", "email", "inquiries", "inquiry", "enquiries", "enquiry", "booking", "bookings", "book",
    "appointments", "appointment", "schedule", "scheduling", "frontdesk", "front.desk", "reception", "owner",
    "manager", "management", "help", "customerservice", "customercare", "care", "estimates", "estimate", "quotes",
    "quote", "dispatch", "general", "main", "questions", "ask", "connect", "staff", "clinic", "studio", "shop",
    "store", "orders-info", "request", "requests", "leads", "newpatients", "patients",
}
FREE_PROVIDERS = {"gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "aol.com", "icloud.com", "live.com", "msn.com", "comcast.net", "att.net", "sbcglobal.net", "verizon.net", "bellsouth.net", "me.com", "protonmail.com", "ymail.com", "cox.net", "charter.net", "rocketmail.com", "mac.com", "earthlink.net", "frontier.com", "windstream.net", "centurylink.net", "optonline.net", "roadrunner.com", "twc.com", "q.com", "gmx.com", "zoho.com", "aim.com", "juno.com", "netzero.net", "mail.com"}

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


# free/hosted website builders: every subdomain is a different business
HOSTED_SUFFIXES = (
    "square.site", "wixsite.com", "godaddysites.com", "weebly.com", "wordpress.com", "squarespace.com",
    "myshopify.com", "carrd.co", "webflow.io", "netlify.app", "vercel.app", "github.io", "blogspot.com",
    "site123.me", "mystrikingly.com", "strikingly.com", "jimdosite.com", "jimdofree.com", "ueniweb.com",
    "business.site", "mozellosite.com", "webnode.com", "yolasite.com", "simplesite.com", "wix.com",
    "glossgenius.com", "square.online", "editorx.io", "framer.website", "durablesites.com", "durable.co",
)
# booking / directory platforms: an address at their domain belongs to the platform, not the business
PLATFORM_EMAIL_DOMAINS = {
    "vagaro.com", "glossgenius.com", "squareup.com", "square.site", "booksy.com", "schedulicity.com", "mindbodyonline.com",
    "mindbody.io", "setmore.com", "acuityscheduling.com", "fresha.com", "styleseat.com", "boulevard.io", "zenoti.com",
    "janeapp.com", "aestheticsrecord.com", "moxiemedspa.com", "patientnow.com", "calendly.com", "wix.com",
    "godaddy.com", "weebly.com", "squarespace.com", "shopify.com", "yelp.com", "google.com", "ueni.com",
    "durable.co", "hibu.com", "thryv.com", "yodle.com", "scorpion.co", "birdeye.com", "podium.com",
}


def registrable(host: str) -> str:
    host = (host or "").lower().split(":")[0]
    if host.startswith("www."):
        host = host[4:]
    for suf in HOSTED_SUFFIXES:
        if host.endswith("." + suf) and host != suf:
            return host  # e.g. glowmedspa.square.site
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
    e = html.unescape(e or "").replace("\\", "").strip().strip(".,;:()[]<>\"'").lower()
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
    if dom in PLATFORM_EMAIL_DOMAINS or any(dom.endswith("." + d) for d in PLATFORM_EMAIL_DOMAINS):
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


def _decode_escapes(raw: str) -> str:
    return (raw.replace("\\u0040", "@").replace("\\x40", "@").replace("%40", "@").replace("&#64;", "@")
               .replace("&#x40;", "@").replace("&commat;", "@").replace("\\u002e", ".").replace("\\/", "/"))


OBF_HINT_RE = re.compile(r"[\[({]\s*at\s*[\])}]|\sdot\s", re.I)
MAX_AT_SIGNS = 4000


def extract_emails(raw_html: str) -> list:
    """Every email needs an "@", so instead of cleaning and searching the whole page (slow on 2 MB pages),
    jump to each "@" and only look at the text around it. Covers visible text, mailto links, and emails
    hidden in script/JSON data (Wix, GoDaddy, Squarespace) after decoding \\u0040, %40, &#64; etc."""
    decoded = _decode_escapes(raw_html)
    found = list(MAILTO_RE.findall(decoded))
    if "cfemail" in raw_html:
        found += [decode_cfemail(h) for h in CFEMAIL_RE.findall(raw_html)]
    if "email-protection#" in raw_html:
        found += [decode_cfemail(h) for h in CF_HREF_RE.findall(raw_html)]
    if "ld+json" in raw_html:
        found += _jsonld_emails(raw_html)

    i, n, last_end = decoded.find("@"), 0, -1
    while i != -1 and n < MAX_AT_SIGNS:
        n += 1
        if i >= last_end:  # skip "@" signs already inside the previous window
            lo, hi = max(0, i - 90), i + 140
            w = html.unescape(TAG_RE.sub(" ", decoded[lo:hi]))
            found += EMAIL_RE.findall(w)
            found += ["{}@{}".format(*m) for m in SPACED_AT_RE.findall(w)]
            last_end = i + 60
        i = decoded.find("@", i + 1)

    # "bob [at] gmail [dot] com" style: only pay for the full-text pass when the page hints at it
    if OBF_HINT_RE.search(decoded):
        text = html.unescape(TAG_RE.sub(" ", SCRIPT_STYLE_RE.sub(" ", decoded)))
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


def sitemap_links(xml: str, base_url: str, limit: int = 8) -> tuple:
    """(contact-ish page URLs, child sitemap URLs) from a sitemap.xml body."""
    base_host = registrable(urlparse(base_url).hostname or "")
    pages, children = [], []
    for u in LOC_RE.findall(xml or ""):
        u = html.unescape(u)
        p = urlparse(u)
        if registrable(p.hostname or "") != base_host:
            continue
        if p.path.lower().endswith(".xml"):
            children.append(u)
        elif CONTACT_HINT_RE.search(p.path):
            score = 0 if "contact" in p.path.lower() else 1 if re.search(r"about|team|staff", p.path, re.I) else 2
            pages.append((score, u))
    return [u for _, u in sorted(pages, key=lambda x: x[0])][:limit], children[:3]


def local_part_kind(local: str) -> str:
    """'drop' | 'low' | 'role' | 'personal' for the part before the @."""
    base = re.sub(r"[0-9]+$", "", local.lower())
    parts = set(re.split(r"[._+-]", base)) | {base}
    if parts & ROLE_DROP or base.startswith(("noreply", "no-reply", "donotreply", "bounce")):
        return "drop"
    if base in ROLE_GOOD:
        return "role"
    if base in ROLE_LOW or parts & ROLE_LOW:
        return "low"
    if parts & ROLE_GOOD:
        return "role"
    # looks like a person: letters with optional . _ -, e.g. john, jsmith, john.smith, mary-ann
    if re.fullmatch(r"[a-z]{2,20}([._-][a-z]{1,20})?", base):
        return "personal"
    return "role"


def email_type(email: str, site_url: str) -> str:
    """personal | role | free | low | other  (same-domain personal is the best cold email target)."""
    local, dom = email.split("@", 1)
    kind = local_part_kind(local)
    if kind == "drop":
        return "drop"
    if dom in FREE_PROVIDERS:
        return "free"
    if registrable(dom) != registrable(urlparse(site_url).hostname or ""):
        return "other"
    return {"personal": "personal", "role": "role", "low": "low"}[kind]


TYPE_ORDER = {"personal": 0, "role": 1, "free": 2, "low": 3, "other": 4}


def rank_emails_typed(emails: list, site_url: str) -> list:
    """[(email, type)] best first, with careers@/privacy@/webmaster@ style addresses removed.
    Order: same-domain person (john@acme.com), same-domain shared inbox (info@acme.com), free provider
    (acmeplumbing@gmail.com), same-domain billing/marketing style inboxes, then other domains."""
    typed = [(e, email_type(e, site_url)) for e in emails]
    typed = [t for t in typed if t[1] != "drop"]
    # an address on some other domain is usually the web designer or an old domain: only keep those
    # when the site gave us nothing at its own domain or a free provider
    if any(t[1] != "other" for t in typed):
        typed = [t for t in typed if t[1] != "other"]
    return sorted(typed, key=lambda t: TYPE_ORDER[t[1]])


def rank_emails(emails: list, site_url: str) -> list:
    return [e for e, _ in rank_emails_typed(emails, site_url)]
