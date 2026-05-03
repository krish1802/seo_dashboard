#!/usr/bin/env python3
"""
Standalone daily crawl script — multi-site.

Iterates every site in sites_config.SITES and produces, per site:
  seo_reports/<slug>/<DOMAIN>_technical_audit_YYYY-MM-DD.csv
  seo_reports/<slug>/<DOMAIN>_page_keywords_YYYY-MM-DD.csv
  seo_reports/<slug>/<DOMAIN>_keyword_clusters_YYYY-MM-DD.csv

Designed to run via GitHub Actions on a schedule. Auto-commits CSVs back.

CLI:
    python crawl_script.py                     # all sites
    python crawl_script.py --site sanfranciscobriefing.com
    python crawl_script.py --max-pages 60 --max-keyword-pages 20
"""

from __future__ import annotations

import argparse
import os
import re
import time
from collections import defaultdict
from datetime import datetime
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests

from sites_config import Site, SITES, get_site


HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SEO-Audit-Bot/1.0)"}


# ── CRAWL ───────────────────────────────────────────────────────────────

def crawl(start_url: str, max_pages: int = 80) -> list[dict]:
    visited, queue, results = set(), [start_url], []
    parsed = urlparse(start_url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    while queue and len(visited) < max_pages:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)
        print(f"  [{len(visited)}/{max_pages}] {url}")
        try:
            t0 = time.time()
            r = requests.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
            lt = round(time.time() - t0, 2)
        except Exception as e:
            results.append({
                "url": url, "status": "ERROR", "load_time_s": None,
                "title": "", "title_length": 0, "meta_description": "", "meta_desc_length": 0,
                "h1_count": 0, "canonical": "", "noindex": False, "images_missing_alt": 0,
                "has_og_tags": False, "has_schema": False, "issues": str(e),
            })
            continue

        status = r.status_code
        content = r.text if status == 200 else ""

        tm = re.search(r"<title[^>]*>(.*?)</title>", content, re.I | re.S)
        title = re.sub(r"<[^>]+>", "", tm.group(1)).strip() if tm else ""
        mm = (re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']*)', content, re.I) or
              re.search(r'<meta[^>]+content=["\']([^"\']*)["\'][^>]+name=["\']description["\']', content, re.I))
        meta = mm.group(1).strip() if mm else ""
        h1 = len(re.findall(r"<h1[^>]*>", content, re.I))
        cm = re.search(r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']([^"\']*)', content, re.I)
        can = cm.group(1).strip() if cm else ""
        noi = bool(re.search(r'content=["\'][^"\']*noindex', content, re.I))
        img = len(re.findall(r'<img(?![^>]*\balt\s*=)[^>]*/?>', content, re.I))
        og  = bool(re.search(r'property=["\']og:', content, re.I))
        sch = bool(re.search(r'application/ld\+json', content, re.I))

        if status == 200:
            for link in re.findall(r'href=["\']([^"\'#?][^"\']*)["\']', content, re.I):
                full = urljoin(base, link)
                if full.startswith(base) and full not in visited:
                    queue.append(full)

        issues: list[str] = []
        if status >= 400: issues.append(f"HTTP {status}")
        if not title:           issues.append("Missing title")
        elif len(title) < 30:   issues.append(f"Title short ({len(title)})")
        elif len(title) > 65:   issues.append(f"Title long ({len(title)})")
        if not meta:            issues.append("Missing meta desc")
        elif len(meta) < 70:    issues.append(f"Meta short ({len(meta)})")
        elif len(meta) > 160:   issues.append(f"Meta long ({len(meta)})")
        if h1 == 0:    issues.append("No H1")
        elif h1 > 1:   issues.append(f"Multiple H1s ({h1})")
        if lt and lt > 3.0: issues.append(f"Slow ({lt}s)")
        if noi:        issues.append("Noindexed")
        if img > 0:    issues.append(f"{img} img no alt")
        if not og:     issues.append("No OG tags")
        if not sch:    issues.append("No Schema")

        results.append({
            "url": url, "status": status, "load_time_s": lt,
            "title": title, "title_length": len(title),
            "meta_description": meta, "meta_desc_length": len(meta),
            "h1_count": h1, "canonical": can, "noindex": noi,
            "images_missing_alt": img, "has_og_tags": og,
            "has_schema": sch, "issues": " | ".join(issues),
        })
        time.sleep(0.3)
    return results


# ── KEYWORD EXTRACTION ──────────────────────────────────────────────────

_STOP = set(
    "the a an and or but in on at to for of with is are was were be been "
    "have has had do does did will would could should may might shall this "
    "that these those i we you he she it they my our your his her its their "
    "from by up about into than then so if not no more".split()
)


def extract_keywords(url: str) -> dict | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        content = r.text
    except Exception:
        return None

    clean = re.sub(r"<(script|style)[^>]*>.*?</(script|style)>", "", content, flags=re.I | re.S)
    text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", clean)).strip().lower()
    tm = re.search(r"<title[^>]*>(.*?)</title>", content, re.I | re.S)
    title = re.sub(r"<[^>]+>", "", tm.group(1)).strip() if tm else ""
    h1s = [re.sub(r"<[^>]+>", "", h).strip() for h in re.findall(r"<h1[^>]*>(.*?)</h1>", content, re.I | re.S)]
    h2s = [re.sub(r"<[^>]+>", "", h).strip() for h in re.findall(r"<h2[^>]*>(.*?)</h2>", content, re.I | re.S)]

    words = re.findall(r"\b[a-z][a-z\-]{2,}\b", text)
    freq: dict[str, int] = defaultdict(int)
    for w in words:
        if w not in _STOP:
            freq[w] += 1
    top = sorted(freq.items(), key=lambda x: -x[1])[:20]

    bigrams: dict[str, int] = defaultdict(int)
    for i in range(len(words) - 1):
        if words[i] not in _STOP and words[i + 1] not in _STOP:
            bigrams[f"{words[i]} {words[i + 1]}"] += 1
    top_bi = sorted(bigrams.items(), key=lambda x: -x[1])[:15]

    return {
        "url": url, "title": title, "h1": " | ".join(h1s),
        "h2s": " | ".join(h2s[:8]), "top_words": top,
        "top_bigrams": top_bi, "word_count": len(words),
    }


# ── PER-SITE PIPELINE ───────────────────────────────────────────────────

def run_site(
    site: Site,
    max_pages: int = 80,
    max_keyword_pages: int = 30,
    base_output: str = "seo_reports",
) -> dict:
    """Crawl one site end-to-end. Returns a small summary dict."""
    today = datetime.today().strftime("%Y-%m-%d")
    output_dir = site.output_dir(base_output)
    print(f"\n🔍 [{site.domain}] crawl starting on {today}...")

    summary = {
        "domain": site.domain,
        "date": today,
        "audit_csv": None, "audit_pages": 0, "audit_issues": 0,
        "keywords_csv": None, "keyword_pages": 0,
        "clusters_csv": None,
    }

    results = crawl(site.site_url, max_pages=max_pages)
    if not results:
        print(f"❌ [{site.domain}] crawl returned no results")
        return summary

    df = pd.DataFrame(results)
    audit_path = os.path.join(output_dir, f"{site.domain}_technical_audit_{today}.csv")
    df.to_csv(audit_path, index=False)
    n_issues = int((df["issues"].astype(str).str.len() > 0).sum())
    summary.update(audit_csv=audit_path, audit_pages=len(df), audit_issues=n_issues)
    print(f"✅ [{site.domain}] {len(results)} pages, {n_issues} with issues → {audit_path}")

    # Keyword analysis (only 200 OK pages)
    print(f"📝 [{site.domain}] analysing page content...")
    urls = df[df["status"].astype(str) == "200"]["url"].tolist()[:max_keyword_pages]
    kw_rows = []
    for i, url in enumerate(urls):
        print(f"  [{i + 1}/{len(urls)}] {url}")
        kd = extract_keywords(url)
        if kd:
            kw_rows.append({
                "url": kd["url"], "title": kd["title"], "h1": kd["h1"], "h2s": kd["h2s"],
                "top_words":   ", ".join(f"{w}({c})" for w, c in kd["top_words"]),
                "top_bigrams": ", ".join(f"{b}({c})" for b, c in kd["top_bigrams"]),
                "word_count":  kd["word_count"],
            })
        time.sleep(0.4)

    if kw_rows:
        kw_path = os.path.join(output_dir, f"{site.domain}_page_keywords_{today}.csv")
        pd.DataFrame(kw_rows).to_csv(kw_path, index=False)
        summary.update(keywords_csv=kw_path, keyword_pages=len(kw_rows))
        print(f"✅ [{site.domain}] keywords → {kw_path}")

        # Clusters seeded from per-site tracked keywords + observed h2 phrases
        all_kws = list(site.tracked_keywords)
        for row in kw_rows:
            for h in str(row.get("h2s", "")).split(" | "):
                h = h.strip()
                if 10 < len(h) < 80:
                    all_kws.append(h)
        clusters: dict[str, list[str]] = defaultdict(list)
        for kw in all_kws:
            w = kw.lower().split()
            k = " ".join(w[:2]) if len(w) >= 2 else (w[0] if w else "other")
            clusters[k].append(kw)
        crows = [
            {"cluster": k, "keyword_count": len(v), "keywords": " | ".join(v)}
            for k, v in sorted(clusters.items(), key=lambda x: -len(x[1]))
        ]
        cl_path = os.path.join(output_dir, f"{site.domain}_keyword_clusters_{today}.csv")
        pd.DataFrame(crows).to_csv(cl_path, index=False)
        summary["clusters_csv"] = cl_path
        print(f"✅ [{site.domain}] clusters → {cl_path}")

    return summary


def run_all(
    max_pages: int = 80,
    max_keyword_pages: int = 30,
    base_output: str = "seo_reports",
) -> list[dict]:
    summaries: list[dict] = []
    for site in SITES:
        try:
            summaries.append(run_site(
                site,
                max_pages=max_pages,
                max_keyword_pages=max_keyword_pages,
                base_output=base_output,
            ))
        except Exception as e:
            print(f"❌ [{site.domain}] crawler crashed: {e}")
            summaries.append({"domain": site.domain, "error": str(e)})
    return summaries


# ── CLI ─────────────────────────────────────────────────────────────────

def _main() -> None:
    ap = argparse.ArgumentParser(description="Multi-site daily SEO crawler")
    ap.add_argument("--site", help="Run for one domain only")
    ap.add_argument("--max-pages", type=int, default=80)
    ap.add_argument("--max-keyword-pages", type=int, default=30)
    ap.add_argument("--output", default="seo_reports")
    args = ap.parse_args()

    if args.site:
        run_site(
            get_site(args.site),
            max_pages=args.max_pages,
            max_keyword_pages=args.max_keyword_pages,
            base_output=args.output,
        )
    else:
        summaries = run_all(
            max_pages=args.max_pages,
            max_keyword_pages=args.max_keyword_pages,
            base_output=args.output,
        )
        print("\n──────── CRAWL SUMMARY ────────")
        for s in summaries:
            if "error" in s:
                print(f"  {s['domain']:35s}  ERROR: {s['error']}")
            else:
                print(
                    f"  {s['domain']:35s}  pages={s['audit_pages']:4d} "
                    f"issues={s['audit_issues']:4d} kw_pages={s['keyword_pages']:3d}"
                )

    print("\n🎉 Done. Results in seo_reports/<site>/")


if __name__ == "__main__":
    _main()