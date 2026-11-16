"""Review check: take a reply export CSV (PlusVibe), look up each business's lowest-rated Google reviews on
DataForSEO, and split the rows into an .xlsx with tabs:
  - "Recent 1-2 Star"      : a 1- or 2-star review posted within the last N days
  - "No Recent 1-2 Star"   : looked up, but no such review
  - "Couldn't Check"       : no way to identify the listing, or DataForSEO found nothing (only if any)
Rows whose Label is in EXCLUDE_LABELS are dropped entirely and never looked up.
"""
import csv
import io
import re
from datetime import datetime, timezone, timedelta

EXCLUDE_LABELS = {"NOT_INTERESTED", "OUT_OF_OFFICE", "AUTOMATIC_REPLY"}
CELL_MAX = 32000  # Excel's per-cell limit is 32,767 characters

ADDED = ["REVIEW CHECK", "LOW REVIEWS IN WINDOW", "NEWEST LOW SCORE", "NEWEST LOW DATE", "NEWEST LOW REVIEWER",
         "NEWEST LOW TEXT", "NEWEST LOW URL", "MATCHED LISTING", "MATCHED RATING", "LOOKED UP BY"]


def parse_csv(raw):
    for enc in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    csv.field_size_limit(10 ** 8)
    rd = csv.reader(io.StringIO(text, newline=""))
    rows = list(rd)
    if not rows:
        return [], []
    header = [h.strip() for h in rows[0]]
    body = [r + [""] * (len(header) - len(r)) for r in rows[1:] if any(c.strip() for c in r)]
    return header, body


def _col(header, *names):
    low = [h.lower() for h in header]
    for n in names:
        if n.lower() in low:
            return low.index(n.lower())
    return None


def _get(row, i):
    return row[i].strip() if i is not None and i < len(row) else ""


CID_RE = re.compile(r"[?&]cid=(\d{5,})")
HEX_RE = re.compile(r"0x[0-9a-f]+:0x([0-9a-f]+)", re.I)
PLACE_RE = re.compile(r"(?:query_place_id|place_id)[=:](ChIJ[\w-]+)")


def identify(header, row):
    """(key, task fields, how) for a row, or (None, None, reason) when it can't be looked up."""
    url = _get(row, _col(header, "Lead: custom_url"))
    m = CID_RE.search(url)
    if m:
        return "cid:" + m.group(1), {"cid": m.group(1)}, "Google listing ID"
    m = HEX_RE.search(url)
    if m:
        cid = str(int(m.group(1), 16))
        return "cid:" + cid, {"cid": cid}, "Google listing ID"
    m = PLACE_RE.search(url)
    if m:
        return "place:" + m.group(1), {"place_id": m.group(1)}, "Google place ID"
    name = _get(row, _col(header, "Lead: custom_name")) or _get(row, _col(header, "Company Name"))
    addr = _get(row, _col(header, "Lead: custom_full_address"))
    if not addr:
        street = _get(row, _col(header, "Lead: custom_street_address", "Address"))
        city = _get(row, _col(header, "City"))
        state = _get(row, _col(header, "Lead: custom_state_code", "State", "Lead: custom_region"))
        addr = " ".join(x for x in (street, city, state) if x)
    if name and addr:
        kw = f"{name} {addr}"
        return "kw:" + kw.lower(), {"keyword": kw[:700]}, "business name + address"
    domain = _get(row, _col(header, "Domain"))
    if not domain:
        site = _get(row, _col(header, "Website"))
        domain = re.sub(r"^https?://(www\.)?", "", site, flags=re.I).split("/")[0]
    if not domain:
        email = _get(row, _col(header, "Lead Email"))
        d = email.split("@")[-1].lower() if "@" in email else ""
        if d and d not in FREE_MAIL:
            domain = d
    if name and domain:
        kw = f"{name} {domain}"
        return "kw:" + kw.lower(), {"keyword": kw[:700]}, "business name + website"
    if domain:
        return "kw:" + domain.lower(), {"keyword": domain[:700]}, "website only (check the matched listing)"
    if name:
        return "kw:" + name.lower(), {"keyword": name[:700]}, "business name only (check the matched listing)"
    return None, None, "no listing link, name, address or website in the row"


FREE_MAIL = {"gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com", "icloud.com", "live.com", "msn.com",
             "comcast.net", "att.net", "sbcglobal.net", "verizon.net", "me.com", "ymail.com", "bellsouth.net"}


def select(header, body, test_limit):
    """Rows to keep (label filter, then the test limit) and the stats."""
    li = _col(header, "Label")
    kept, dropped = [], 0
    for r in body:
        if _get(r, li).upper() in EXCLUDE_LABELS:
            dropped += 1
            continue
        kept.append(r)
    total = len(kept)
    if test_limit:
        kept = kept[:test_limit]
    return kept, {"rows_in": len(body), "dropped_by_label": dropped, "eligible": total, "processing": len(kept)}


def plan(header, rows, depth, sort_by="lowest_rating", language_code="en"):
    """One DataForSEO task per business. Returns (tasks [(body, key)], row_keys [key or None], row_how)."""
    tasks, seen, keys, hows = [], set(), [], []
    for r in rows:
        key, fields, how = identify(header, r)
        keys.append(key)
        hows.append(how)
        if key and key not in seen:
            seen.add(key)
            tasks.append(({**fields, "location_code": 2840, "language_code": language_code, "depth": depth,
                           "sort_by": sort_by, "tag": key[:250]}, key))
    return tasks, keys, hows


def _when(ts):
    for fmt in ("%Y-%m-%d %H:%M:%S %z", "%Y-%m-%d %H:%M:%S"):
        try:
            d = datetime.strptime(ts.strip(), fmt)
            return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
        except (ValueError, AttributeError):
            pass
    return None


def summarize(t, max_stars, days, now=None):
    """DataForSEO reviews task -> what the sheet needs about that business."""
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days)
    res = (t.get("result") or [None])[0] or {}
    items = res.get("items") or []
    rating = (res.get("rating") or {}).get("value")
    low = []
    for it in items:
        try:
            val = float((it.get("rating") or {}).get("value"))
        except (TypeError, ValueError):
            continue
        when = _when(it.get("timestamp") or "")
        if val <= max_stars and when and when >= cutoff:
            low.append((when, val, it))
    low.sort(key=lambda x: x[0], reverse=True)
    out = {"found": bool(res), "title": res.get("title") or "", "rating": rating if rating is not None else "",
           "reviews_seen": len(items), "recent_low": len(low)}
    if low:
        when, val, it = low[0]
        out.update({"score": int(val) if val == int(val) else val, "date": when.strftime("%Y-%m-%d"),
                    "user": it.get("profile_name") or "",
                    "text": (it.get("review_text") or it.get("original_review_text") or "").strip(),
                    "url": it.get("review_url") or ""})
    return out


_ILLEGAL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def _cell(v):
    s = "" if v is None else str(v)
    s = _ILLEGAL.sub("", s)
    if len(s) > CELL_MAX:
        s = s[:CELL_MAX] + " ...[cut]"
    if s.startswith("="):
        s = " " + s  # openpyxl would write it as a formula
    return s


def split(header, rows, keys, hows, results, days):
    """-> {tab name: [row lists]} with the ADDED columns in front."""
    tabs = {"Recent 1-2 Star": [], "No Recent 1-2 Star": [], "Couldn't Check": []}
    for r, key, how in zip(rows, keys, hows):
        s = results.get(key) if key else None
        if not key:
            tabs["Couldn't Check"].append([how] + [""] * (len(ADDED) - 1) + r)
            continue
        if s is None:
            tabs["Couldn't Check"].append(["DataForSEO did not return this lookup"] + [""] * (len(ADDED) - 2) + [how] + r)
            continue
        if not s["found"]:
            tabs["Couldn't Check"].append(["No Google listing found"] + [""] * (len(ADDED) - 2) + [how] + r)
            continue
        if s["recent_low"]:
            lead = [f"Yes - 1-2 star in last {days} days", s["recent_low"], s["score"], s["date"], s["user"],
                    s["text"], s["url"], s["title"], s["rating"], how]
            tabs["Recent 1-2 Star"].append(lead + r)
        else:
            lead = [f"No 1-2 star in last {days} days ({s['reviews_seen']} reviews checked)", 0, "", "", "", "",
                    "", s["title"], s["rating"], how]
            tabs["No Recent 1-2 Star"].append(lead + r)
    return tabs


def to_xlsx(header, tabs, path):
    from openpyxl import Workbook
    wb = Workbook(write_only=True)
    cols = ADDED + header
    for name, rows in tabs.items():
        if name == "Couldn't Check" and not rows:
            continue
        ws = wb.create_sheet(title=name[:31])
        ws.append(cols)
        for r in rows:
            ws.append([_cell(v) for v in r])
    wb.save(path)
