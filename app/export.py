"""Builds the final files for a finished maps job (what n8n's Transform / Merge Reviews / Build MV File /
Apply MV Results / Build Final Files code nodes used to do), so n8n never has to hold the whole list.

Output files and columns match the old n8n output exactly:
  FB no Email, No Email, wReview, LowScoreNoText, NoReview (+ *_NoEmail buckets, which only exist for
  odd inputs). One row per email, reviews in SCORE/TEXT/PICTURES/USER NAME, SCORE1/TEXT1/... columns.
"""
import csv
import io
import re

EMPTY = {"", "nan", "none", "na", "n/a", "null", "undefined"}
BUCKET_ORDER = ["wReview", "LowScoreNoText", "NoReview", "wReview_NoEmail", "LowScoreNoText_NoEmail", "NoReview_NoEmail"]


def _s(v):
    # what the CSV hop used to do to a value: None -> "", everything else str()
    return "" if v is None else str(v)


def clean(v):
    s = _s(v).strip()
    return "" if s.lower() in EMPTY else s


def flat(v):
    return _s(v).replace("\x00", "").replace("\r", " ").replace("\n", " ").strip()


def _title(s):
    # JS: s.toLowerCase().replace(/(^|[^a-z])([a-z])/g, (m, p, c) => p + c.toUpperCase())
    return re.sub(r"(^|[^a-z])([a-z])", lambda m: m.group(1) + m.group(2).upper(), s.lower())


def _num(v):
    s = clean(v)
    if s == "":
        return None
    try:
        f = float(s)
    except ValueError:
        return None
    return int(f) if f == int(f) else f


def _score_str(n):
    return "" if n is None else str(n)


def transform(rows, state_codes, state_names):
    """n8n 'Transform': clean values, SAB, tidy categories, drop out-of-group states, dedupe by CID."""
    cols = list(rows[0].keys()) if rows else []
    out = []
    for raw in rows:
        o = {k: clean(raw.get(k)) for k in cols}
        o.setdefault("EMAIL", "")
        o.setdefault("FACEBOOK", "")
        out.append(o)
    addr_col = next((c for c in ("ADDRESS", "STREET ADDRESS", "FULL ADDRESS") if any(c in r for r in out)), None)
    for r in out:
        r["SAB"] = ("True" if r.get(addr_col, "") == "" else "False") if addr_col else "True"
        if "CATEGORY" in r:
            seen, cats = set(), []
            for c in r["CATEGORY"].split(","):
                t = c.strip()
                if not t or t.lower() in EMPTY:
                    continue
                n = _title(t)
                if n not in seen:
                    seen.add(n)
                    cats.append(n)
            r["CATEGORY"] = ", ".join(cats)
    codes = {c.upper() for c in state_codes}
    names = {n.lower() for n in state_names}
    dropped = 0
    kept = []
    for r in out:
        sc, rg = r.get("STATE CODE", "").upper(), r.get("REGION", "").lower()
        if sc or rg:
            if not (sc in codes or rg in names or rg.upper() in codes):
                dropped += 1
                continue
        kept.append(r)
    seen_cid, deduped = set(), []
    for r in kept:
        cid = r.get("CID", "")
        if cid:
            if cid in seen_cid:
                continue
            seen_cid.add(cid)
        deduped.append(r)
    fb_no = [r for r in deduped if not r["EMAIL"] and r["FACEBOOK"]]
    no = [r for r in deduped if not r["EMAIL"]]
    withe = [r for r in deduped if r["EMAIL"]]
    stats = {"inputCount": len(rows), "droppedOutOfGroup": dropped, "afterDedupe": len(deduped),
             "fbNoEmail": len(fb_no), "noEmail": len(no), "withEmail": len(withe)}
    return fb_no, no, withe, stats


def _sort_key_score_text(r):
    s = r["_score"]
    return (1 if s is None else 0, s if s is not None else 0, -len(r["TEXT"]))


def merge(with_email, reviews, keep):
    """n8n 'Merge Reviews': reviews into columns, one row per email, lowest score first, one row per email
    address, buckets."""
    by_cid = {}
    for rv in reviews:
        score, text = _num(rv.get("SCORE")), flat(rv.get("TEXT"))
        cid = clean(rv.get("CID"))
        if score is None or not cid:
            continue
        by_cid.setdefault(cid, []).append({"SCORE": score, "TEXT": text, "PICTURES": flat(rv.get("PICTURES")),
                                           "USER NAME": flat(rv.get("USER NAME"))})
    for cid, lst in by_cid.items():
        srt = sorted(lst, key=lambda r: (0 if r["TEXT"] else 1, r["SCORE"]))  # stable, like the JS sortBy
        low = srt[0]["SCORE"]
        best = 0
        for i, r in enumerate(srt):
            if r["SCORE"] == low and len(r["TEXT"]) > len(srt[best]["TEXT"]):
                best = i
        b = srt.pop(best)
        by_cid[cid] = ([b] + srt)[:keep]
    max_n = max((len(v) for v in by_cid.values()), default=0)
    slots = max(max_n, 1)

    expanded = []
    for e in with_email:
        o = {k: v for k, v in e.items() if k not in ("SCORE", "TEXT")}
        o["CID_STR"] = o.get("CID", "")
        lst = by_cid.get(o["CID_STR"], [])
        o["REVIEW_COUNT"] = str(len(lst)) if lst else ""
        score0 = None
        for i in range(slots):
            suf = "" if i == 0 else str(i)
            r = lst[i] if i < len(lst) else None
            if i == 0:
                score0 = r["SCORE"] if r else None
                o["SCORE"] = _score_str(score0)
            else:
                o["SCORE" + suf] = _score_str(r["SCORE"]) if r else ""
            o["TEXT" + suf] = r["TEXT"] if r else ""
            o["PICTURES" + suf] = r["PICTURES"] if r else ""
            o["USER NAME" + suf] = r["USER NAME"] if r else ""
        emails = [s.strip() for s in o.get("EMAIL", "").split(",") if s.strip()]
        types = [s.strip() for s in o.get("EMAIL TYPES", "").split(",")]
        base = {k: v for k, v in o.items() if k != "EMAIL TYPES"}
        if emails:
            for i, em in enumerate(emails):
                row = dict(base)
                row["EMAIL"] = em
                row["EMAIL TYPE"] = types[i] if i < len(types) else ""
                row["EMAIL RANK"] = str(i + 1)
                row["_score"] = score0
                expanded.append(row)
        else:
            row = dict(base)
            row.update({"EMAIL": "", "EMAIL TYPE": "", "EMAIL RANK": "", "_score": score0})
            expanded.append(row)

    has = [r for r in expanded if r["EMAIL"].strip()]
    none = [r for r in expanded if not r["EMAIL"].strip()]
    has.sort(key=_sort_key_score_text)  # stable
    seen, uniq = set(), []
    for r in has:
        if r["EMAIL"] in seen:
            continue
        seen.add(r["EMAIL"])
        uniq.append(r)
    out, counts = [], {}
    for r in uniq + none:
        s = r.pop("_score")
        low = s in (1, 2, 3)
        label = "wReview" if low and r["TEXT"].strip() else ("LowScoreNoText" if low else "NoReview")
        r["BUCKET"] = label + ("" if r["EMAIL"].strip() else "_NoEmail")
        counts[r["BUCKET"]] = counts.get(r["BUCKET"], 0) + 1
        out.append(r)
    return out, {"reviewRowsIn": len(reviews), "businessesWithReviews": len(by_cid),
                 "maxReviewsPerBusiness": max_n, "buckets": counts}


def mv_emails(merged):
    """n8n 'Build MV File': only wReview emails get verified."""
    seen, out = set(), []
    for r in merged:
        e = r["EMAIL"].strip()
        if r["BUCKET"] == "wReview" and e and e.lower() not in seen:
            seen.add(e.lower())
            out.append(e)
    return out


def parse_mv_csv(text):
    """MillionVerifier download -> {email(lower): result(lower)}."""
    text = text.lstrip("﻿")
    rd = csv.DictReader(io.StringIO(text))
    keys = rd.fieldnames or []
    ekey = next((k for k in keys if k.strip().lstrip("﻿").lower() == "email"), None) \
        or next((k for k in keys if "email" in k.lower()), keys[0] if keys else None)
    rkey = next((k for k in keys if k.strip().lower() == "result"), None) or next((k for k in keys if "result" in k.lower()), None)
    out = {}
    for row in rd:
        e = (row.get(ekey) or "").strip().lower() if ekey else ""
        if e:
            out[e] = (row.get(rkey) or "").strip().lower() if rkey else "unknown"
    return out


def apply_mv(merged, mv, keep_results, skipped):
    """n8n 'Apply MV Results': wReview rows stay only when MillionVerifier said ok / catch_all."""
    keep = {k.lower() for k in keep_results}
    if not skipped and not mv:
        raise ValueError("MillionVerifier results had no rows")
    rows, counts = [], {}
    for r in merged:
        if not r["EMAIL"]:
            rows.append({**r, "MV RESULT": ""})
            continue
        if r["BUCKET"] != "wReview":
            rows.append({**r, "MV RESULT": "not_verified"})
            continue
        res = mv.get(r["EMAIL"].strip().lower(), "not_returned")
        counts[res] = counts.get(res, 0) + 1
        if res in keep:
            rows.append({**r, "MV RESULT": res})
    return rows, counts


def names_to_clean(rows):
    return list(dict.fromkeys(n for n in (str(r.get("NAME") or "").strip() for r in rows) if n))


def _esc(v):
    s = re.sub(r"\r?\n|\r", " ", _s(v))
    return '"' + s.replace('"', '""') + '"' if ('"' in s or "," in s) else s


def to_csv(rows):
    cols, seen = [], set()
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k)
                cols.append(k)
    lines = [",".join(_esc(c) for c in cols)]
    lines += [",".join(_esc(r.get(c, "")) for c in cols) for r in rows]
    return "﻿" + "\n".join(lines)


def final_files(fb_no, no, rows, name_map, base, ts):
    """n8n 'Build Final Files': [(file name, row count, csv text)]."""
    def with_clean(r):
        o = {}
        for k, v in r.items():
            if k in ("CID_STR", "BUCKET"):
                continue
            o[k] = v
            if k == "NAME":
                o["COMPANY NAME"] = name_map.get(str(v).strip()) or v
        return o
    out = []

    def add(rs, label):
        out.append((f"{base} - {label} {ts}.csv", len(rs), to_csv(rs)))
    if fb_no:
        add(fb_no, "FB no Email")
    if no:
        add(no, "No Email")
    by = {}
    for r in rows:
        by.setdefault(r["BUCKET"], []).append(with_clean(r))
    for b in BUCKET_ORDER:
        if by.get(b):
            add(by[b], b)
    return out
