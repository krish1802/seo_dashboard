#!/usr/bin/env python3
"""
SEO Analytics Dashboard + WordPress SEO Auto-Optimizer
For aifrontierdispatch.com
"""

import os
import re
import time
import csv
import json
from datetime import datetime, timedelta
from collections import defaultdict
from urllib.parse import urljoin, urlparse
from pathlib import Path
from base64 import b64encode

import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from bs4 import BeautifulSoup
import streamlit as st

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest

# Import CSV-driven fixer
from fix_issues import fix_from_audit, latest_audit_csv  # from fix_issues.py

# Optional: dotenv
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ──────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────

GA4_PROPERTY_ID = "532475459"
# GA4_CREDENTIALS_PATH = "ga4_credentials.json"

from google.oauth2 import service_account


def get_ga4_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["ga4"]
    )
    return BetaAnalyticsDataClient(credentials=credentials)
# def get_ga4_client():
#     return BetaAnalyticsDataClient.from_service_account_file(GA4_CREDENTIALS_PATH)

SITE_URL = "https://aifrontierdispatch.com"
DOMAIN = "aifrontierdispatch.com"
OUTPUT_DIR = "seo_reports"
os.makedirs(OUTPUT_DIR, exist_ok=True)

TRACKED_KEYWORDS = [
    "AI news 2025", "artificial intelligence breakthroughs",
    "AI business insights", "machine learning research",
    "AI frontier news", "generative AI tools",
    "AI startup news", "large language models news",
]
COMPETITORS = ["techcrunch.com", "venturebeat.com", "wired.com"]
CRAWL_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SEO-Audit-Bot/1.0)"}

# WordPress credentials from .env
WP_URL = os.getenv("WP_URL", "https://aifrontierdispatch.com").rstrip("/")
WP_USER = os.getenv("WP_USER", "californianartisinal")
WP_APP_PASS = os.getenv("WP_APP_PASSWORD", "")
API_BASE = f"{WP_URL}/wp-json/wp/v2"

SEO_TITLE_MIN = 50
SEO_TITLE_MAX = 60
META_DESC_MIN = 150
META_DESC_MAX = 160

SLUG_STOP_WORDS = {
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "was", "are", "were", "be", "been",
    "has", "have", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "this", "that", "these", "those", "it", "its"
}

REQUEST_DELAY = 1.5

# ──────────────────────────────────────────────────────────────
# WORDPRESS AUTOBOT: SESSION + AUTH
# ──────────────────────────────────────────────────────────────

from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import html as html_lib

def _make_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Connection": "keep-alive",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "application/json, text/plain, */*",
    })
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=2,
        pool_maxsize=5,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

SESSION = _make_session()

def safe_request(method, url, max_attempts=4, **kwargs):
    global SESSION
    kwargs.setdefault("timeout", 30)
    for attempt in range(1, max_attempts + 1):
        try:
            fn = getattr(SESSION, method)
            resp = fn(url, **kwargs)
            return resp
        except (requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError) as e:
            wait = 2 ** attempt
            print(f"  ⚠️  Connection error (attempt {attempt}/{max_attempts}): {e}")
            if attempt < max_attempts:
                print(f"     Retrying in {wait}s with a fresh session...")
                time.sleep(wait)
                SESSION = _make_session()
            else:
                print("  ❌ All retries exhausted.")
                raise

def _auth_header():
    token = b64encode(f"{WP_USER}:{WP_APP_PASS}".encode()).decode()
    return {"Authorization": f"Basic {token}", "Content-Type": "application/json"}

# ──────────────────────────────────────────────────────────────
# WORDPRESS AUTOBOT: PERFORMANCE AUDIT
# ──────────────────────────────────────────────────────────────

def audit_page_performance(url):
    perf = {
        "response_time_s": None,
        "html_size_kb": None,
        "request_count": None,
        "js_unminified": [],
        "css_unminified": [],
        "perf_issues": [],
    }
    try:
        start = time.time()
        r = safe_request("get", url, timeout=15, headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        })
        elapsed = round(time.time() - start, 3)
        perf["response_time_s"] = elapsed
        if elapsed > 0.2:
            perf["perf_issues"].append(
                f"Response time {elapsed}s exceeds 0.2s recommendation"
            )

        html_bytes = len(r.content)
        html_kb = round(html_bytes / 1024, 1)
        perf["html_size_kb"] = html_kb
        if html_kb > 50:
            perf["perf_issues"].append(
                f"HTML document is {html_kb} KB (recommendation: ≤ 50 KB)"
            )

        soup = BeautifulSoup(r.text, "html.parser")
        scripts = soup.find_all("script", src=True)
        stylesheets = soup.find_all("link", rel=lambda v: v and "stylesheet" in v)
        images_tag = soup.find_all("img", src=True)
        iframes = soup.find_all("iframe", src=True)
        request_count = len(scripts) + len(stylesheets) + len(images_tag) + len(iframes)
        perf["request_count"] = request_count
        if request_count > 20:
            perf["perf_issues"].append(
                f"Page makes ~{request_count} requests (recommendation: ≤ 20)"
            )

        for tag in scripts:
            src = tag.get("src", "")
            if src and ".js" in src and ".min.js" not in src and "cdn" not in src.lower():
                perf["js_unminified"].append(src.split("?")[0])
        if perf["js_unminified"]:
            perf["perf_issues"].append(
                f"{len(perf['js_unminified'])} JS file(s) appear unminified"
            )

        for tag in stylesheets:
            href = tag.get("href", "")
            if href and ".css" in href and ".min.css" not in href and "cdn" not in href.lower():
                perf["css_unminified"].append(href.split("?")[0])
        if perf["css_unminified"]:
            perf["perf_issues"].append(
                f"{len(perf['css_unminified'])} CSS file(s) appear unminified"
            )

        missing_expires = []
        for img in images_tag[:5]:
            img_src = img.get("src", "")
            if img_src.startswith("http"):
                try:
                    img_resp = safe_request("get", img_src, timeout=5, max_attempts=1,
                                            headers={"User-Agent": "Mozilla/5.0"})
                    cache_ctrl = img_resp.headers.get("Cache-Control", "")
                    expires = img_resp.headers.get("Expires", "")
                    if not cache_ctrl and not expires:
                        missing_expires.append(img_src)
                except Exception:
                    pass
        if missing_expires:
            perf["perf_issues"].append(
                f"Server is not using expires/cache-control headers for "
                f"{len(missing_expires)} sampled image(s)"
            )
    except Exception as e:
        perf["perf_issues"].append(f"Performance audit error: {e}")

    return perf

# ──────────────────────────────────────────────────────────────
# WORDPRESS AUTOBOT: TEXT HELPERS
# ──────────────────────────────────────────────────────────────

def clean_html_entities(text):
    return html_lib.unescape(text)

def strip_html_tags(text):
    return BeautifulSoup(text, "html.parser").get_text(separator=" ").strip()

def word_count(text):
    return len(strip_html_tags(text).split())

def extract_keywords_from_title(title):
    clean = clean_html_entities(title).lower()
    clean = re.sub(r"[^a-z0-9\s]", " ", clean)
    words = [w for w in clean.split() if w and w not in SLUG_STOP_WORDS and len(w) > 2]
    return words[:5]

def optimize_slug(title):
    clean = clean_html_entities(title).lower()
    clean = re.sub(r"[^a-z0-9\s]", " ", clean)
    words = [w for w in clean.split() if w not in SLUG_STOP_WORDS and len(w) > 1]
    slug = "-".join(words[:8])
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug

def generate_seo_title(raw_title, site_name="AI Frontier Dispatch"):
    clean = clean_html_entities(raw_title).strip()
    full = f"{clean} | {site_name}"
    if SEO_TITLE_MIN <= len(full) <= SEO_TITLE_MAX:
        return full
    if len(full) > SEO_TITLE_MAX:
        max_content = SEO_TITLE_MAX - len(f" | {site_name}")
        trimmed = clean[:max_content].rsplit(" ", 1)[0]
        return f"{trimmed} | {site_name}"
    return full

def generate_meta_description(content_html, title, keywords):
    soup = BeautifulSoup(content_html, "html.parser")
    desc = ""
    for p in soup.find_all("p"):
        text = p.get_text(separator=" ").strip()
        if len(text) > 60:
            desc = text
            break
    if not desc:
        desc = soup.get_text(separator=" ").strip()
    desc = re.sub(r"\s+", " ", desc)
    if len(desc) > META_DESC_MAX:
        desc = desc[:META_DESC_MAX].rsplit(" ", 1)[0]
        if not desc.endswith("."):
            desc += "…"
    if len(desc) < META_DESC_MIN and keywords:
        kw_phrase = f" Learn about {', '.join(keywords[:3])}."
        desc = (desc + kw_phrase)[:META_DESC_MAX]
    return desc.strip()

def add_alt_tags_to_images(content_html, keywords):
    soup = BeautifulSoup(content_html, "html.parser")
    images = soup.find_all("img")
    updated = False
    count = 0
    for i, img in enumerate(images):
        if not img.get("alt") or img.get("alt", "").strip() == "":
            kw_base = " ".join(keywords[:3]) if keywords else "article image"
            alt_text = f"{kw_base} - image {i + 1}" if i > 0 else kw_base
            img["alt"] = alt_text
            updated = True
            count += 1
    return str(soup), updated, count

def clean_duplicate_og_tags(meta):
    cleaned = dict(meta)
    if "_yoast_wpseo_opengraph-title" in meta:
        for key in [
            "rank_math_facebook_title", "rank_math_facebook_description",
            "rank_math_twitter_title", "rank_math_twitter_description"
        ]:
            cleaned.pop(key, None)
    og_title = cleaned.get("_yoast_wpseo_opengraph-title", "")
    og_desc = cleaned.get("_yoast_wpseo_opengraph-description", "")
    if og_title and cleaned.get("_yoast_wpseo_twitter-title") == og_title:
        cleaned.pop("_yoast_wpseo_twitter-title", None)
    if og_desc and cleaned.get("_yoast_wpseo_twitter-description") == og_desc:
        cleaned.pop("_yoast_wpseo_twitter-description", None)
    return cleaned

def seo_score(title_clean, meta_desc, slug, content_html, keywords):
    issues = []
    score = 100
    tlen = len(title_clean)
    if tlen < SEO_TITLE_MIN:
        issues.append(f"SEO title too short ({tlen} chars, min {SEO_TITLE_MIN})")
        score -= 15
    elif tlen > SEO_TITLE_MAX:
        issues.append(f"SEO title too long ({tlen} chars, max {SEO_TITLE_MAX})")
        score -= 10
    mlen = len(meta_desc)
    if mlen < META_DESC_MIN:
        issues.append(f"Meta description too short ({mlen} chars, min {META_DESC_MIN})")
        score -= 20
    elif mlen > META_DESC_MAX:
        issues.append(f"Meta description too long ({mlen} chars, max {META_DESC_MAX})")
        score -= 10
    if len(slug) > 75:
        issues.append(f"Slug too long ({len(slug)} chars)")
        score -= 10
    wc = word_count(content_html)
    if wc < 300:
        issues.append(f"Content too thin ({wc} words, aim for 600+)")
        score -= 20
    elif wc < 600:
        issues.append(f"Content could be longer ({wc} words, aim for 600+)")
        score -= 10
    soup = BeautifulSoup(content_html, "html.parser")
    if not soup.find(["h2", "h3"]):
        issues.append("No subheadings (H2/H3) found in content")
        score -= 10
    plain = soup.get_text().lower()
    kw_found = sum(1 for k in keywords if k in plain)
    if keywords and kw_found == 0:
        issues.append("Focus keywords not found in content")
        score -= 10
    return max(0, score), issues

# ──────────────────────────────────────────────────────────────
# WORDPRESS AUTOBOT: FETCH & APPLY (POST-LEVEL AUTOBOT)
# ──────────────────────────────────────────────────────────────

def get_all_posts(status="publish", per_page=10, max_pages=5):
    all_posts = []
    for page in range(1, max_pages + 1):
        print(f"  📄 Fetching WP posts page {page}...", flush=True)
        r = safe_request(
            "get",
            f"{API_BASE}/posts",
            headers=_auth_header(),
            params={
                "status": status,
                "per_page": per_page,
                "page": page,
                "context": "edit",
                "_fields": "id,title,slug,content,excerpt,meta,link,modified"
            }
        )
        if not r.ok or not r.json():
            print(f"stopped (status {r.status_code})")
            break
        batch = r.json()
        total_pages = int(r.headers.get("X-WP-TotalPages", 1))
        all_posts.extend(batch)
        print(f"got {len(batch)} posts (total so far: {len(all_posts)})")
        if page >= total_pages:
            break
        time.sleep(REQUEST_DELAY)
    return all_posts

def apply_seo_fixes(post, dry_run=True, min_score_to_fix=80):
    pid = post["id"]
    raw_title = post["title"]["rendered"]
    slug = post["slug"]
    content_html = post["content"]["rendered"]
    post_link = post.get("link", WP_URL)

    clean_title = clean_html_entities(raw_title)
    keywords = extract_keywords_from_title(clean_title)

    content_html, alt_updated, alt_count = add_alt_tags_to_images(content_html, keywords)

    meta = post.get("meta", {})
    meta = clean_duplicate_og_tags(meta)

    new_seo_title = generate_seo_title(clean_title)
    new_meta_desc = generate_meta_description(content_html, clean_title, keywords)
    new_slug = optimize_slug(clean_title)

    old_yoast_title = meta.get("_yoast_wpseo_title", "")
    old_yoast_desc = meta.get("_yoast_wpseo_metadesc", "")
    old_rm_title = meta.get("rank_math_title", "")
    old_rm_desc = meta.get("rank_math_description", "")

    score_before, issues = seo_score(
        old_yoast_title or clean_title,
        old_yoast_desc or new_meta_desc,
        slug, content_html, keywords
    )
    score_after, _ = seo_score(
        new_seo_title, new_meta_desc,
        new_slug, content_html, keywords
    )

    perf = audit_page_performance(post_link)

    result = {
        "id": pid,
        "title": clean_title,
        "link": post_link,
        "slug_old": slug,
        "slug_new": new_slug,
        "seo_title_old": old_yoast_title or old_rm_title or "(none)",
        "seo_title_new": new_seo_title,
        "meta_desc_old": old_yoast_desc or old_rm_desc or "(none)",
        "meta_desc_new": new_meta_desc,
        "score_before": score_before,
        "score_after": score_after,
        "issues": issues,
        "keywords": keywords,
        "word_count": word_count(content_html),
        "alt_tags_added": alt_count,
        "performance": perf,
        "changes_made": [],
        "dry_run": dry_run,
    }

    if dry_run or score_before >= min_score_to_fix:
        return result

    changes = {}

    if alt_updated:
        changes["content"] = content_html
        result["changes_made"].append(f"Added ALT tags to {alt_count} image(s)")

    if raw_title != clean_title:
        changes["title"] = clean_title
        result["changes_made"].append("Cleaned HTML entities from title")

    if new_slug and new_slug != slug and len(new_slug) < len(slug):
        changes["slug"] = new_slug
        result["changes_made"].append(f"Slug: {slug} → {new_slug}")

    yoast_meta = {}
    if new_seo_title != old_yoast_title:
        yoast_meta["_yoast_wpseo_title"] = new_seo_title
        yoast_meta["_yoast_wpseo_opengraph-title"] = new_seo_title
        result["changes_made"].append("Updated Yoast SEO title")
    if new_meta_desc != old_yoast_desc:
        yoast_meta["_yoast_wpseo_metadesc"] = new_meta_desc
        yoast_meta["_yoast_wpseo_opengraph-description"] = new_meta_desc
        result["changes_made"].append("Updated Yoast meta description")
    if yoast_meta:
        changes["meta"] = yoast_meta

    rm_meta = {}
    if new_seo_title != old_rm_title:
        rm_meta["rank_math_title"] = new_seo_title
    if new_meta_desc != old_rm_desc:
        rm_meta["rank_math_description"] = new_meta_desc
    if rm_meta:
        changes.setdefault("meta", {}).update(rm_meta)

    if changes:
        time.sleep(REQUEST_DELAY)
        r = safe_request(
            "post",
            f"{API_BASE}/posts/{pid}",
            headers=_auth_header(),
            json=changes
        )
        if r.ok:
            result["changes_made"].append("✅ Saved to WordPress")
        else:
            result["changes_made"].append(
                f"❌ Save failed ({r.status_code}): {r.text[:200]}"
            )

    return result

def run_seo_optimizer(
    status="publish",
    per_page=10,
    max_pages=10,
    dry_run=True,
    min_score_to_fix=80,
    report_file="seo_report.json",
):
    print("=" * 60)
    mode = "DRY RUN (audit only)" if dry_run else "LIVE (applying fixes)"
    print(f"  WordPress SEO Optimizer — {mode}")
    print(f"  Site  : {WP_URL}")
    print(f"  Date  : {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    print(f"\n📥 Fetching {status} posts from WordPress...")
    posts = get_all_posts(status=status, per_page=per_page, max_pages=max_pages)
    print(f"\n✅ Loaded {len(posts)} posts\n")

    if not posts:
        return []

    report = []
    needs_fix = 0
    fixed = 0

    for i, post in enumerate(posts, 1):
        pid = post["id"]
        title = clean_html_entities(post["title"]["rendered"])
        print(f"[{i:>3}/{len(posts)}] ID {pid} — {title[:55]}")

        result = apply_seo_fixes(post, dry_run=dry_run, min_score_to_fix=min_score)
        score = result["score_before"]

        if score < min_score_to_fix:
            needs_fix += 1
        print(f"        Score : {score}/100 | Words: {result['word_count']} | Issues: {len(result['issues'])}")
        for issue in result["issues"]:
            print(f"          • {issue}")

        perf = result.get("performance", {})
        if perf.get("perf_issues"):
            print(f"        ⚡ Performance Issues:")
            for pi in perf["perf_issues"]:
                print(f"          • {pi}")
        else:
            if perf.get("response_time_s") is not None:
                print(f"        ⚡ Perf OK — {perf['response_time_s']}s / {perf['html_size_kb']} KB / {perf['request_count']} requests")

        if result.get("alt_tags_added", 0) > 0:
            print(f"        🖼️  {result['alt_tags_added']} image(s) missing ALT attributes (fixed)")

        if not dry_run and result["changes_made"]:
            fixed += 1
            print(f"        Applied: {', '.join(result['changes_made'])}")

        report.append(result)
        time.sleep(REQUEST_DELAY)
        print()

    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"\n📄 Full report saved to: {report_file}")

    return report

# ──────────────────────────────────────────────────────────────
# ORIGINAL DASHBOARD HELPERS (AUDIT/SERP LOADERS)
# ──────────────────────────────────────────────────────────────

def get_report_dates():
    dates = set()
    if os.path.exists(OUTPUT_DIR):
        for f in os.listdir(OUTPUT_DIR):
            m = re.search(r"(\d{4}-\d{2}-\d{2})", f)
            if m:
                dates.add(m.group(1))
    return sorted(dates, reverse=True)

def load_csv(prefix, date):
    path = f"{OUTPUT_DIR}/{prefix}_{date}.csv"
    return pd.read_csv(path) if os.path.exists(path) else None

def load_audit(date): return load_csv(f"{DOMAIN}_technical_audit", date)
def load_serp(date): return load_csv("serp_tracking", date)
def load_keywords(date): return load_csv(f"{DOMAIN}_page_keywords", date)
def load_clusters(date): return load_csv(f"{DOMAIN}_keyword_clusters", date)

def compute_audit_snapshot(df):
    if df is None or len(df) == 0:
        return None
    total = len(df)
    broken = len(df[df["status"].astype(str).str.match(r"^[45E]")])
    with_issues = len(df[df["issues"].astype(str).str.len() > 0])
    clean = total - with_issues
    avg_load = df["load_time_s"].dropna().mean()
    slow = len(df[df["load_time_s"].dropna() > 3.0]) if "load_time_s" in df else 0
    missing_title = len(df[df["title_length"] == 0]) if "title_length" in df else 0
    missing_meta = len(df[df["meta_desc_length"] == 0]) if "meta_desc_length" in df else 0
    no_schema = len(df[~df["has_schema"].astype(bool)]) if "has_schema" in df else 0
    no_og = len(df[~df["has_og_tags"].astype(bool)]) if "has_og_tags" in df else 0
    return {
        "total_pages": total,
        "clean_pages": clean,
        "pages_with_issues": with_issues,
        "broken_pages": broken,
        "avg_load_time": round(avg_load, 3) if pd.notna(avg_load) else None,
        "slow_pages": slow,
        "missing_title": missing_title,
        "missing_meta": missing_meta,
        "no_schema": no_schema,
        "no_og": no_og,
        "health_score": round((clean / total) * 100, 1) if total else 0,
    }

def compute_serp_snapshot(df):
    if df is None or len(df) == 0:
        return None
    ss = df[df["site"] == DOMAIN] if "site" in df.columns else df
    pos = pd.to_numeric(ss["our_position"], errors="coerce")
    return {
        "top3": int((pos <= 3).sum()),
        "top10": int((pos <= 10).sum()),
        "top20": int((pos <= 20).sum()),
        "not_ranked": int(pos.isna().sum()),
        "avg_position": round(pos.dropna().mean(), 1) if len(pos.dropna()) > 0 else None,
    }

def load_all_snapshots():
    all_dates = get_report_dates()
    audit_rows, serp_rows = [], []
    for d in all_dates:
        snap = compute_audit_snapshot(load_audit(d))
        if snap:
            snap["date"] = d
            audit_rows.append(snap)
        ssnap = compute_serp_snapshot(load_serp(d))
        if ssnap:
            ssnap["date"] = d
            serp_rows.append(ssnap)
    audit_df = pd.DataFrame(audit_rows).sort_values("date") if audit_rows else pd.DataFrame()
    serp_df = pd.DataFrame(serp_rows).sort_values("date") if serp_rows else pd.DataFrame()
    return audit_df, serp_df

def delta_str(new_val, old_val, higher_is_better=True, fmt="{:.0f}", suffix=""):
    if old_val is None or new_val is None:
        return "—", "kpi-delta-neu"
    diff = new_val - old_val
    if diff == 0:
        return "No change", "kpi-delta-neu"
    sign = "+" if diff > 0 else ""
    label = f"{sign}{fmt.format(diff)}{suffix}"
    if higher_is_better:
        cls = "kpi-delta-pos" if diff > 0 else "kpi-delta-neg"
    else:
        cls = "kpi-delta-neg" if diff > 0 else "kpi-delta-pos"
    return label, cls

def trend_icon(new_val, old_val, higher_is_better=True):
    if old_val is None or new_val is None or new_val == old_val:
        return "→", "trend-flat", "FLAT"
    improved = (new_val > old_val) == higher_is_better
    if improved:
        return "↑", "trend-up", "IMPROVED"
    return "↓", "trend-down", "DECLINED"

# ──────────────────────────────────────────────────────────────
# FIX REPORT HELPERS (FOR ✅ Fixed Issues TAB)
# ──────────────────────────────────────────────────────────────

def get_fix_report_dates():
    dates = set()
    if os.path.exists(OUTPUT_DIR):
        for f in os.listdir(OUTPUT_DIR):
            m = re.search(rf"{re.escape(DOMAIN)}_fix_issues_(\d{{4}}-\d{{2}}-\d{{2}})\.csv", f)
            if m:
                dates.add(m.group(1))
    return sorted(dates, reverse=True)

def load_fix_issues(date: str):
    path = f"{OUTPUT_DIR}/{DOMAIN}_fix_issues_{date}.csv"
    return pd.read_csv(path) if os.path.exists(path) else None

# ──────────────────────────────────────────────────────────────
# GA4 HELPERS
# ──────────────────────────────────────────────────────────────

def fetch_ga4_data(days=7):
    try:
        client = get_ga4_client()
        request = RunReportRequest(
            property=f"properties/{GA4_PROPERTY_ID}",
            dimensions=[{"name": "date"}],
            metrics=[
                {"name": "activeUsers"},
                {"name": "sessions"},
                {"name": "screenPageViews"}
            ],
            date_ranges=[{"start_date": f"{days}daysAgo", "end_date": "today"}],
        )
        response = client.run_report(request)
        data = []
        for row in response.rows:
            data.append({
                "date": row.dimension_values[0].value,
                "users": int(row.metric_values[0].value),
                "sessions": int(row.metric_values[1].value),
                "pageviews": int(row.metric_values[2].value),
            })
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"GA4 Error: {e}")
        return None

def fetch_top_pages():
    try:
        client = get_ga4_client()
        request = RunReportRequest(
            property=f"properties/{GA4_PROPERTY_ID}",
            dimensions=[{"name": "pagePath"}],
            metrics=[{"name": "screenPageViews"}],
            date_ranges=[{"start_date": "7daysAgo", "end_date": "today"}],
        )
        response = client.run_report(request)
        rows = []
        for row in response.rows:
            rows.append({
                "page": row.dimension_values[0].value,
                "views": int(row.metric_values[0].value)
            })
        return pd.DataFrame(rows).sort_values("views", ascending=False).head(10)
    except Exception as e:
        st.error(f"Top Pages Error: {e}")
        return None

# ──────────────────────────────────────────────────────────────
# STREAMLIT UI
# ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="SEO Dashboard — AI Frontier Dispatch",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');
    html, body, .stApp { font-family: 'DM Sans', sans-serif; }
    div[data-testid="stSidebar"] { background: #0f1117; }
    div[data-testid="stSidebar"] * { color: #e2e8f0 !important; }
    div[data-testid="stSidebar"] .stRadio label { color: #e2e8f0 !important; }
    div[data-testid="stSidebar"] hr { border-color: #2d3748; }
</style>
""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("## 📊 SEO Dashboard")
    st.markdown(f"**Site:** `{DOMAIN}`")
    st.divider()
    page = st.radio("Navigate", [
        "🏠 Overview",
        "📈 Growth Tracker",
        "🔍 Technical Audit",
        "🏆 SERP Rankings",
        "📊 Traffic Analytics",
        "📝 Content Analysis",
        "🔑 Keywords",
        "⚡ Run New Scan",
        "🛠️ Fix Issues",
        "✅ Fixed Issues",
    ], label_visibility="collapsed")
    st.divider()
    dates = get_report_dates()
    selected_date = st.selectbox("Report Date", dates, index=0) if dates else datetime.today().strftime("%Y-%m-%d")
    if not dates:
        st.info("No reports yet. Run a scan!")
    st.divider()
    st.caption("SEO Automation Toolkit — Free Edition")

# ──────────────────────────────────────────────────────────────
# PAGE ROUTING
# ──────────────────────────────────────────────────────────────


if page == "🔍 Technical Audit":
    st.markdown("# 🔍 Technical SEO Audit")
    st.divider()
    audit_df = load_audit(selected_date)
    if audit_df is not None:
        c1, c2, c3 = st.columns(3)
        with c1:
            filt = st.selectbox("Filter", ["All", "With Issues", "Clean"])
        with c2:
            search = st.text_input("Search URL")
        with c3:
            statuses = st.multiselect("Status", sorted(audit_df["status"].astype(str).unique()))

        df = audit_df.copy()
        if filt == "With Issues":
            df = df[df["issues"].astype(str).str.len() > 0]
        elif filt == "Clean":
            df = df[(df["issues"].isna()) | (df["issues"].astype(str).str.len() == 0)]
        if statuses:
            df = df[df["status"].astype(str).isin(statuses)]
        if search:
            df = df[df["url"].str.contains(search, case=False, na=False)]

        st.markdown(f"**{len(df)} of {len(audit_df)} pages**")
        cols = [c for c in [
            "url", "status", "load_time_s", "title_length", "meta_desc_length",
            "h1_count", "images_missing_alt", "has_og_tags", "has_schema", "issues"
        ] if c in df.columns]
        st.dataframe(df[cols], use_container_width=True, height=500)
        st.download_button("📥 Download CSV", df.to_csv(index=False).encode(),
                           f"audit_{selected_date}.csv", "text/csv")

        st.divider()
        st.markdown("### 🚨 Top Issues")
        il = []
        for _, row in df.iterrows():
            for i in str(row.get("issues", "")).split(" | "):
                if i.strip():
                    il.append(i.strip())
        if il:
            idf = pd.Series(il).value_counts().head(10).reset_index()
            idf.columns = ["Issue", "Count"]
            st.dataframe(idf, use_container_width=True)
    else:
        st.warning("No data. Run a scan!")

elif page == "🛠️ Fix Issues":
    st.markdown("# 🛠️ Fix SEO Issues (WordPress)")
    st.markdown(
        "This connects to your WordPress site via REST using credentials "
        "and auto-fixes SEO issues. You can either:\n\n"
        "- Run the CSV-driven fixer (latest Technical Audit CSV → WP fixes)\n"
        "- Or run the original post-level autobot over all posts."
    )
    st.divider()

    tab1, tab2 = st.tabs(["CSV-driven Fix (Recommended)", "Legacy Autobot"])

    with tab1:
        st.markdown("### CSV-driven Fix from Technical Audit")
        st.write(
            "Uses the latest Technical Audit CSV (`seo_reports/...technical_audit_YYYY-MM-DD.csv`), "
            "maps URLs → WordPress posts, and fixes what it can (SEO title/meta, ALT text, slug)."
        )
        st.warning(
            "Live changes:\n"
            "- SEO titles & meta descriptions (Yoast + Rank Math)\n"
            "- Image ALT attributes\n"
            "- Slug cleanup\n\n"
            "Make sure `WP_URL`, `WP_USER`, `WP_APP_PASSWORD` are set."
        )

        dry_run_csv = st.checkbox("Dry run (simulate only, no changes)", value=True)
        if st.button("🚀 Run Fix Issues from Latest Audit", type="primary"):
            with st.spinner("Running fix_from_audit against WordPress..."):
                try:
                    results = fix_from_audit(dry_run=dry_run_csv)
                    if not results:
                        st.info("No results returned (no audit or no applicable fixes).")
                    else:
                        df = pd.DataFrame(results)
                        st.success(f"Completed. {df['fixed'].sum()} rows marked as fixed.")
                        st.dataframe(df, use_container_width=True, height=500)
                        st.download_button(
                            "📥 Download Fix Report CSV",
                            df.to_csv(index=False).encode(),
                            "fix_issues_from_audit.csv",
                            "text/csv",
                        )
                except Exception as e:
                    st.error(f"Error while running CSV-driven fixer: {e}")

    with tab2:
        st.markdown("### Legacy WordPress SEO Autobot (Post-level)")
        st.warning(
            "**Live changes ahead**:\n\n"
            "- Updates SEO titles and meta descriptions\n"
            "- Cleans slugs\n"
            "- Adds ALT attributes for images\n"
            "- Normalizes Yoast / Rank Math meta\n\n"
            "Make sure your `.env` has `WP_URL`, `WP_USER`, `WP_APP_PASSWORD`."
        )

        dry_run = st.checkbox("Dry run for legacy autobot (no changes)", value=True)
        min_score = st.slider("Minimum SEO score to fix", 0, 100, 80, 5)
        max_pages = st.slider("Max WordPress post pages to fetch", 1, 50, 10, 1)
        per_page = st.slider("Posts per page (WordPress API)", 5, 50, 10, 5)
        report_path = st.text_input("Legacy autobot report file", "seo_report.json")

        only_from_audit = st.checkbox(
            "Only target URLs that have issues in latest Technical Audit (status=200)",
            value=False
        )

        audit_df = load_audit(selected_date) if only_from_audit else None

        if st.button("🚀 Run Legacy WordPress SEO Auto-Optimizer", use_container_width=True):
            with st.spinner("Running legacy WordPress SEO autobot… this may take a few minutes."):
                try:
                    results = []
                    if only_from_audit and audit_df is not None and len(audit_df) > 0:
                        target_urls = audit_df[
                            (audit_df["status"].astype(str) == "200") &
                            (audit_df["issues"].astype(str).str.len() > 0)
                        ]["url"].tolist()
                        st.write(f"Targeting {len(target_urls)} URLs from Technical Audit…")

                        for url in target_urls:
                            slug = url.rstrip("/").split("/")[-1]
                            resp = safe_request(
                                "get",
                                f"{API_BASE}/posts",
                                params={"slug": slug, "context": "edit"},
                                headers=_auth_header(),
                            )
                            if resp.ok and resp.json():
                                post = resp.json()[0]
                                res = apply_seo_fixes(post, dry_run=dry_run, min_score_to_fix=min_score)
                                if res:
                                    results.append(res)
                            time.sleep(REQUEST_DELAY)

                        with open(report_path, "w", encoding="utf-8") as f:
                            json.dump(results, f, indent=2, ensure_ascii=False)
                    else:
                        results = run_seo_optimizer(
                            status="publish",
                            per_page=per_page,
                            max_pages=max_pages,
                            dry_run=dry_run,
                            min_score_to_fix=min_score,
                            report_file=report_path,
                        )

                    if results:
                        df = pd.DataFrame([
                            {
                                "id": r["id"],
                                "title": r["title"],
                                "score_before": r["score_before"],
                                "score_after": r["score_after"],
                                "alt_tags_added": r.get("alt_tags_added", 0),
                                "perf_issues": " | ".join(r.get("performance", {}).get("perf_issues", [])),
                                "changes": ", ".join(r.get("changes_made", [])),
                            }
                            for r in results
                        ])
                        st.success("Completed. See summary below.")
                        st.dataframe(df, use_container_width=True)
                        st.download_button(
                            "📥 Download legacy fix report CSV",
                            df.to_csv(index=False).encode(),
                            "wp_autobot_fix_report.csv",
                            "text/csv"
                        )
                    else:
                        st.info("No posts were processed.")
                except Exception as e:
                    st.error(f"Error while running WordPress autobot: {e}")
elif page == "🏠 Overview":
    st.markdown("# 🏠 SEO Performance Overview")
    st.markdown(f"**{DOMAIN}** — Report for **{selected_date}**")
    st.divider()
    audit_df = load_audit(selected_date)
    serp_df  = load_serp(selected_date)

    if audit_df is not None:
        total       = len(audit_df)
        broken      = len(audit_df[audit_df["status"].astype(str).str.match(r"^[45E]")])
        with_issues = len(audit_df[audit_df["issues"].astype(str).str.len() > 0])
        clean       = total - with_issues
        avg_load    = audit_df["load_time_s"].dropna().mean()

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Pages Crawled", total)
        c2.metric("Clean Pages", clean, delta=f"{clean/max(total,1)*100:.0f}%")
        c3.metric("Issues Found", with_issues, delta=f"-{with_issues}" if with_issues else "0", delta_color="inverse")
        c4.metric("Broken Pages", broken, delta=f"-{broken}" if broken else "0", delta_color="inverse")
        c5.metric("Avg Load Time", f"{avg_load:.2f}s" if pd.notna(avg_load) else "N/A")
        st.divider()

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("### 📊 Issue Distribution")
            if with_issues > 0:
                cats = defaultdict(int)
                for iss_str in audit_df["issues"].dropna():
                    for iss in str(iss_str).split(" | "):
                        iss = iss.strip()
                        if not iss: continue
                        il = iss.lower()
                        if "title" in il: cats["Title Issues"] += 1
                        elif "meta" in il: cats["Meta Description"] += 1
                        elif "h1" in il: cats["H1 Issues"] += 1
                        elif "slow" in il: cats["Slow Pages"] += 1
                        elif "alt" in il: cats["Missing Alt"] += 1
                        elif "og" in il: cats["Missing OG"] += 1
                        elif "schema" in il: cats["Missing Schema"] += 1
                        elif "http" in il: cats["HTTP Errors"] += 1
                        else: cats["Other"] += 1
                fig = px.pie(names=list(cats.keys()), values=list(cats.values()),
                             color_discrete_sequence=px.colors.qualitative.Set2, hole=0.4)
                fig.update_layout(margin=dict(t=20, b=20, l=20, r=20), height=350)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.success("No issues! 🎉")

        with col2:
            st.markdown("### ⏱️ Load Times")
            lt = audit_df["load_time_s"].dropna()
            if len(lt) > 0:
                fig = px.histogram(lt, nbins=20, labels={"value": "Load Time (s)"},
                                   color_discrete_sequence=["#01696f"])
                fig.add_vline(x=3.0, line_dash="dash", line_color="#da7101",
                              annotation_text="3s threshold")
                fig.update_layout(margin=dict(t=20, b=20), height=350, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

        st.markdown("### 🔢 HTTP Status Codes")
        sc = audit_df["status"].astype(str).value_counts().reset_index()
        sc.columns = ["Status", "Count"]
        fig = px.bar(sc, x="Status", y="Count", color_discrete_sequence=["#01696f"])
        fig.update_layout(margin=dict(t=20, b=20), height=250)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No audit data. Run a scan from ⚡ Run New Scan.")

    if serp_df is not None and len(serp_df) > 0:
        st.divider()
        st.markdown("### 🏆 SERP Summary")
        ss = serp_df[serp_df["site"] == DOMAIN] if "site" in serp_df.columns else serp_df
        if len(ss) > 0:
            pos = pd.to_numeric(ss["our_position"], errors="coerce")
            r1, r2, r3 = st.columns(3)
            r1.metric("Top 3", int((pos <= 3).sum()))
            r2.metric("Top 10", int((pos <= 10).sum()))
            r3.metric("Not Ranked", int(pos.isna().sum()))
    st.divider()
    st.markdown("## 📊 Google Analytics (Live Traffic)")

    ga_df = fetch_ga4_data()

    if ga_df is not None and len(ga_df) > 0:
        c1, c2, c3 = st.columns(3)

        st.divider()
        st.markdown("### 🤖 Bot vs Real Traffic")

        total_users = ga_df["users"].sum()

        # Simulated split
        real_users = int(total_users * 0.0476)
        bot_users = total_users - real_users

        c1, c2 = st.columns(2)
        c1.metric("👤 Real Users", real_users)
        c2.metric("🤖 Bot Traffic", bot_users)

        # # Pie chart
        traffic_df = pd.DataFrame({
            "Type": ["Real Users", "Bot Traffic"],
            "Count": [real_users, bot_users]
        })

        fig_bot = px.pie(
            traffic_df,
            names="Type",
            values="Count",
            title="Traffic Composition",
            hole=0.4
        )
        fig_bot.update_traces(
            textinfo="label+value",   # <-- this is the key change
            hovertemplate="%{label}: %{value}"  # clean hover
        )
        st.plotly_chart(fig_bot, use_container_width=True)

        fig = px.line(ga_df, x="date", y=["users", "sessions", "pageviews"],
                    title="Traffic Trend (Last 7 Days)")
        st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    st.markdown("## 🔥 Top Pages (Last 7 Days)")

    top_pages = fetch_top_pages()

    if top_pages is not None and len(top_pages) > 0:
        fig = px.bar(top_pages, x="page", y="views",
                    title="Top Performing Pages",
                    color_discrete_sequence=["#01696f"])
        fig.update_layout(xaxis_tickangle=-45, height=350)
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(top_pages, use_container_width=True)
    else:
        st.info("No GA4 top pages data available.")


# ═══════════════════════════════════════
# PAGE: GROWTH TRACKER  ← NEW
# ═══════════════════════════════════════
elif page == "📈 Growth Tracker":
    st.markdown("# 📈 Growth Tracker")
    st.markdown("Day-over-day and long-term trends across all scan history.")
    st.divider()

    all_dates = get_report_dates()

    if len(all_dates) < 2:
        st.info("📭 You need **at least 2 scans** to track growth. Run another scan from ⚡ Run New Scan!")
        st.stop()

    audit_hist, serp_hist = load_all_snapshots()

    # ── Compare selector ──────────────────────────────────
    col_a, col_b = st.columns(2)
    with col_a:
        date_new = st.selectbox("Compare (newer)", all_dates, index=0, key="d_new")
    with col_b:
        older_options = [d for d in all_dates if d < date_new]
        date_old = st.selectbox("vs (older)", older_options, index=0, key="d_old") if older_options else None

    if date_old is None:
        st.warning("No older scan available for comparison.")
        st.stop()

    snap_new = compute_audit_snapshot(load_audit(date_new))
    snap_old = compute_audit_snapshot(load_audit(date_old))
    ssnap_new = compute_serp_snapshot(load_serp(date_new))
    ssnap_old = compute_serp_snapshot(load_serp(date_old))

    days_apart = (datetime.strptime(date_new, "%Y-%m-%d") - datetime.strptime(date_old, "%Y-%m-%d")).days

    st.markdown(f"#### Comparing **{date_new}** vs **{date_old}** *(gap: {days_apart} day{'s' if days_apart!=1 else ''})*")
    st.divider()

    # ── KPI comparison cards ──────────────────────────────
    if snap_new:
        st.markdown("### 🏥 Site Health")
        k1, k2, k3, k4, k5 = st.columns(5)

        def render_kpi(col, label, new_val, old_val, higher_better=True, fmt="{:.0f}", suffix=""):
            d_str, d_cls = delta_str(new_val, old_val, higher_better, fmt, suffix)
            icon, badge_cls, badge_lbl = trend_icon(new_val, old_val, higher_better)
            display = fmt.format(new_val) + suffix if new_val is not None else "—"
            col.metric(label, display, delta=d_str if d_str != "—" else None,
                       delta_color="normal" if higher_better else "inverse")

        render_kpi(k1, "Total Pages",      snap_new["total_pages"],      snap_old and snap_old["total_pages"],      True,  "{:.0f}")
        render_kpi(k2, "Health Score",     snap_new["health_score"],     snap_old and snap_old["health_score"],     True,  "{:.1f}", "%")
        render_kpi(k3, "Pages w/ Issues",  snap_new["pages_with_issues"],snap_old and snap_old["pages_with_issues"],False, "{:.0f}")
        render_kpi(k4, "Broken Pages",     snap_new["broken_pages"],     snap_old and snap_old["broken_pages"],     False, "{:.0f}")
        render_kpi(k5, "Avg Load Time",    snap_new["avg_load_time"],    snap_old and snap_old["avg_load_time"],    False, "{:.2f}", "s")

        st.divider()

        # detailed issue comparison
        st.markdown("### 🔬 Issue-by-Issue Change")
        issue_fields = [
            ("Missing Title",    "missing_title",    False),
            ("Missing Meta",     "missing_meta",     False),
            ("No Schema",        "no_schema",        False),
            ("No OG Tags",       "no_og",            False),
            ("Slow Pages (>3s)", "slow_pages",       False),
        ]
        cols = st.columns(len(issue_fields))
        for col, (label, field, hb) in zip(cols, issue_fields):
            nv = snap_new.get(field)
            ov = snap_old.get(field) if snap_old else None
            render_kpi(col, label, nv, ov, hb, "{:.0f}")

    if ssnap_new:
        st.divider()
        st.markdown("### 🏆 SERP Rankings Change")
        s1, s2, s3, s4, s5 = st.columns(5)
        render_kpi(s1, "Top 3 Keywords",  ssnap_new["top3"],         ssnap_old and ssnap_old["top3"],         True,  "{:.0f}")
        render_kpi(s2, "Top 10 Keywords", ssnap_new["top10"],        ssnap_old and ssnap_old["top10"],        True,  "{:.0f}")
        render_kpi(s3, "Top 20 Keywords", ssnap_new["top20"],        ssnap_old and ssnap_old["top20"],        True,  "{:.0f}")
        render_kpi(s4, "Not Ranked",      ssnap_new["not_ranked"],   ssnap_old and ssnap_old["not_ranked"],   False, "{:.0f}")
        render_kpi(s5, "Avg Position",    ssnap_new["avg_position"], ssnap_old and ssnap_old["avg_position"], False, "{:.1f}")

    st.divider()

    # ── Historical trend charts ────────────────────────────
    st.markdown("### 📊 Long-Term Trends (All Scans)")

    if not audit_hist.empty:
        tab1, tab2, tab3, tab4 = st.tabs(["Health Score", "Page Count & Issues", "Load Time", "SERP Positions"])

        with tab1:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=audit_hist["date"], y=audit_hist["health_score"],
                mode="lines+markers", name="Health Score %",
                line=dict(color="#48bb78", width=2.5),
                marker=dict(size=7, color="#48bb78"),
                fill="tozeroy", fillcolor="rgba(72,187,120,0.08)"
            ))
            fig.update_layout(
                yaxis=dict(title="Health Score (%)", range=[0, 105]),
                xaxis=dict(title="Scan Date"),
                height=360, margin=dict(t=20, b=40),
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)"
            )
            fig.add_hrect(y0=90, y1=105, fillcolor="rgba(72,187,120,0.05)",
                          line_width=0, annotation_text="Target ≥ 90%", annotation_position="top left")
            st.plotly_chart(fig, use_container_width=True)

        with tab2:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=audit_hist["date"], y=audit_hist["total_pages"],
                name="Total Pages", marker_color="#4299e1", opacity=0.8
            ))
            fig.add_trace(go.Scatter(
                x=audit_hist["date"], y=audit_hist["pages_with_issues"],
                mode="lines+markers", name="Pages w/ Issues",
                line=dict(color="#fc8181", width=2), marker=dict(size=6),
                yaxis="y2"
            ))
            fig.update_layout(
                yaxis=dict(title="Total Pages"),
                yaxis2=dict(title="Issues", overlaying="y", side="right"),
                height=360, margin=dict(t=20, b=40), legend=dict(orientation="h"),
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)"
            )
            st.plotly_chart(fig, use_container_width=True)

        with tab3:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=audit_hist["date"], y=audit_hist["avg_load_time"],
                mode="lines+markers", name="Avg Load Time (s)",
                line=dict(color="#ed8936", width=2.5),
                marker=dict(size=7),
                fill="tozeroy", fillcolor="rgba(237,137,54,0.08)"
            ))
            fig.add_hrect(y0=0, y1=3.0, fillcolor="rgba(72,187,120,0.05)", line_width=0,
                          annotation_text="Under 3s ✓", annotation_position="top left")
            fig.add_hline(y=3.0, line_dash="dash", line_color="#fc8181",
                          annotation_text="3s threshold")
            fig.update_layout(
                yaxis=dict(title="Seconds"),
                height=360, margin=dict(t=20, b=40),
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)"
            )
            st.plotly_chart(fig, use_container_width=True)

        with tab4:
            if not serp_hist.empty:
                fig = go.Figure()
                for col, color, name in [
                    ("top3",  "#48bb78", "Top 3"),
                    ("top10", "#4299e1", "Top 10"),
                    ("top20", "#9f7aea", "Top 20"),
                ]:
                    if col in serp_hist.columns:
                        fig.add_trace(go.Scatter(
                            x=serp_hist["date"], y=serp_hist[col],
                            mode="lines+markers", name=name,
                            line=dict(color=color, width=2),
                            marker=dict(size=6)
                        ))
                fig.update_layout(
                    yaxis=dict(title="Keywords Count"),
                    height=360, margin=dict(t=20, b=40), legend=dict(orientation="h"),
                    plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No SERP data yet.")

    st.divider()

    # ── Per-keyword position history ──────────────────────
    st.markdown("### 🔑 Keyword Position History")
    all_kw_rows = []
    for d in all_dates:
        sdf = load_serp(d)
        if sdf is not None:
            for _, r in sdf.iterrows():
                site_val = r.get("site", DOMAIN)
                if str(site_val) == DOMAIN:
                    p = pd.to_numeric(r.get("our_position"), errors="coerce")
                    if pd.notna(p):
                        all_kw_rows.append({"date": d, "keyword": r["keyword"], "position": p})

    if all_kw_rows:
        kw_df = pd.DataFrame(all_kw_rows)
        selected_kw = st.multiselect(
            "Filter keywords",
            sorted(kw_df["keyword"].unique()),
            default=list(kw_df["keyword"].unique())[:5]
        )
        if selected_kw:
            filtered = kw_df[kw_df["keyword"].isin(selected_kw)]
            fig = px.line(filtered, x="date", y="position", color="keyword",
                          markers=True, color_discrete_sequence=px.colors.qualitative.Set2)
            fig.update_layout(
                yaxis=dict(autorange="reversed", title="Position (lower = better)"),
                height=420, legend=dict(orientation="h"),
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)"
            )
            fig.add_hrect(y0=1, y1=3, fillcolor="rgba(72,187,120,0.08)",
                          line_width=0, annotation_text="Top 3", annotation_position="top right")
            fig.add_hrect(y0=4, y1=10, fillcolor="rgba(66,153,225,0.05)",
                          line_width=0, annotation_text="Top 10", annotation_position="top right")
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No keyword tracking data found yet.")

    st.divider()

    # ── Scan history log ──────────────────────────────────
    st.markdown("### 🗓️ Scan History Log")
    log_rows = []
    for d in all_dates:
        snap = compute_audit_snapshot(load_audit(d))
        ssnap = compute_serp_snapshot(load_serp(d))
        log_rows.append({
            "Date": d,
            "Pages": snap["total_pages"]   if snap else "—",
            "Health %": snap["health_score"]  if snap else "—",
            "Issues": snap["pages_with_issues"] if snap else "—",
            "Broken": snap["broken_pages"]  if snap else "—",
            "Avg Load (s)": snap["avg_load_time"] if snap else "—",
            "Top 10 KWs": ssnap["top10"] if ssnap else "—",
            "Avg SERP Pos": ssnap["avg_position"] if ssnap else "—",
        })
    log_df = pd.DataFrame(log_rows)
    st.dataframe(log_df, use_container_width=True, hide_index=True)
    st.download_button(
        "📥 Export Growth History CSV",
        log_df.to_csv(index=False).encode(),
        "seo_growth_history.csv",
        "text/csv"
    )


# ═══════════════════════════════════════
# PAGE: TECHNICAL AUDIT
# ═══════════════════════════════════════
elif page == "🔍 Technical Audit":
    st.markdown("# 🔍 Technical SEO Audit")
    st.divider()
    audit_df = load_audit(selected_date)
    if audit_df is not None:
        c1, c2, c3 = st.columns(3)
        with c1: filt   = st.selectbox("Filter", ["All", "With Issues", "Clean"])
        with c2: search = st.text_input("Search URL")
        with c3: statuses = st.multiselect("Status", sorted(audit_df["status"].astype(str).unique()))

        df = audit_df.copy()
        if filt == "With Issues":  df = df[df["issues"].astype(str).str.len() > 0]
        elif filt == "Clean":      df = df[(df["issues"].isna()) | (df["issues"].astype(str).str.len() == 0)]
        if statuses: df = df[df["status"].astype(str).isin(statuses)]
        if search:   df = df[df["url"].str.contains(search, case=False, na=False)]

        st.markdown(f"**{len(df)} of {len(audit_df)} pages**")
        cols = [c for c in ["url", "status", "load_time_s", "title_length", "meta_desc_length",
                             "h1_count", "images_missing_alt", "has_og_tags", "has_schema", "issues"]
                if c in df.columns]
        st.dataframe(df[cols], use_container_width=True, height=500)
        st.download_button("📥 Download CSV", df.to_csv(index=False).encode(),
                           f"audit_{selected_date}.csv", "text/csv")

        st.divider()
        st.markdown("### 🚨 Top Issues")
        il = []
        for _, row in df.iterrows():
            for i in str(row.get("issues", "")).split(" | "):
                if i.strip(): il.append(i.strip())
        if il:
            idf = pd.Series(il).value_counts().head(10).reset_index()
            idf.columns = ["Issue", "Count"]
            st.dataframe(idf, use_container_width=True)
    else:
        st.warning("No data. Run a scan!")


# ═══════════════════════════════════════
# PAGE: SERP
# ═══════════════════════════════════════
elif page == "🏆 SERP Rankings":
    st.markdown("# 🏆 SERP Rankings")
    st.divider()
    serp_df = load_serp(selected_date)
    if serp_df is not None and len(serp_df) > 0:
        ss = serp_df[serp_df["site"] == DOMAIN] if "site" in serp_df.columns else serp_df
        cd = ss[["keyword", "our_position"]].copy()
        cd["our_position"] = pd.to_numeric(cd["our_position"], errors="coerce")
        cd = cd.dropna()
        if len(cd) > 0:
            fig = px.bar(cd, x="keyword", y="our_position", color="our_position",
                         color_continuous_scale=["#437a22", "#da7101", "#a12c7b"], range_color=[1, 20])
            fig.update_layout(yaxis=dict(autorange="reversed", title="Position"),
                              xaxis_tickangle=-45, height=400)
            st.plotly_chart(fig, use_container_width=True)
        st.dataframe(ss, use_container_width=True)

        all_dates = get_report_dates()
        if len(all_dates) > 1:
            st.divider()
            st.markdown("### 📈 Trends")
            rows = []
            for d in all_dates[:30]:
                sdf = load_serp(d)
                if sdf is not None:
                    for _, r in sdf.iterrows():
                        p = pd.to_numeric(r.get("our_position"), errors="coerce")
                        if pd.notna(p):
                            rows.append({"date": d, "keyword": r["keyword"], "position": p})
            if rows:
                fig = px.line(pd.DataFrame(rows), x="date", y="position", color="keyword", markers=True)
                fig.update_layout(yaxis=dict(autorange="reversed"), height=400)
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No SERP data.")


# ═══════════════════════════════════════
# PAGE: CONTENT
# ═══════════════════════════════════════
elif page == "📝 Content Analysis":
    st.markdown("# 📝 Content Analysis")
    st.divider()
    kw_df = load_keywords(selected_date)
    if kw_df is not None and len(kw_df) > 0:
        c1, c2, c3 = st.columns(3)
        c1.metric("Pages", len(kw_df))
        c2.metric("Avg Words", f"{kw_df['word_count'].mean():.0f}" if "word_count" in kw_df else "N/A")
        c3.metric("Total Words", f"{kw_df['word_count'].sum():,}" if "word_count" in kw_df else "N/A")
        if "word_count" in kw_df.columns:
            fig = px.bar(kw_df.head(20), x="url", y="word_count", color_discrete_sequence=["#01696f"])
            fig.update_layout(xaxis_tickangle=-45, height=350, margin=dict(b=150))
            st.plotly_chart(fig, use_container_width=True)
        st.dataframe(kw_df, use_container_width=True, height=400)
    else:
        st.warning("No content data.")


# ═══════════════════════════════════════
# PAGE: KEYWORDS
# ═══════════════════════════════════════
elif page == "🔑 Keywords":
    st.markdown("# 🔑 Keyword Clusters")
    st.divider()
    cl = load_clusters(selected_date)
    if cl is not None and len(cl) > 0:
        fig = px.treemap(cl.head(15), path=["cluster"], values="keyword_count",
                         color="keyword_count", color_continuous_scale="Teal")
        fig.update_layout(height=400, margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(cl, use_container_width=True)
    else:
        st.warning("No keyword data.")

elif page == "📊 Traffic Analytics":
    st.markdown("# 📊 Traffic Analytics")
    st.markdown("Live data from Google Analytics (GA4)")
    st.divider()

    ga_df = fetch_ga4_data()

    if ga_df is not None and len(ga_df) > 0:
        # KPIs
        c1, c2, c3 = st.columns(3)
        c1.metric("Users (7d)", ga_df["users"].sum())
        c2.metric("Sessions (7d)", ga_df["sessions"].sum())
        c3.metric("Pageviews (7d)", ga_df["pageviews"].sum())

        st.divider()

        # Traffic trend
        st.markdown("### 📈 Traffic Trend")
        fig = px.line(
            ga_df,
            x="date",
            y=["users", "sessions", "pageviews"],
        )
        st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # Top pages
        st.markdown("### 🔥 Top Pages")
        top_pages = fetch_top_pages()

        if top_pages is not None and len(top_pages) > 0:
            fig2 = px.bar(top_pages, x="page", y="views")
            fig2.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig2, use_container_width=True)

            st.dataframe(top_pages, use_container_width=True)
        else:
            st.info("No top pages data available.")
        st.divider()
        st.markdown("### 🤖 Bot vs Real Traffic")

        total_users = ga_df["users"].sum()

        # Simulated split
        real_users = int(total_users * 0.0476)
        bot_users = total_users - real_users

        c1, c2 = st.columns(2)
        c1.metric("👤 Real Users", real_users)
        c2.metric("🤖 Bot Traffic", bot_users)

        # # Pie chart
        traffic_df = pd.DataFrame({
            "Type": ["Real Users", "Bot Traffic"],
            "Count": [real_users, bot_users]
        })

        fig_bot = px.pie(
            traffic_df,
            names="Type",
            values="Count",
            title="Traffic Composition",
            hole=0.4
        )
        fig_bot.update_traces(
            textinfo="label+value",   # <-- this is the key change
            hovertemplate="%{label}: %{value}"  # clean hover
        )
        st.plotly_chart(fig_bot, use_container_width=True)
    else:
        st.warning("No Google Analytics data found.")

# ═══════════════════════════════════════
# PAGE: RUN SCAN
# ═══════════════════════════════════════
elif page == "⚡ Run New Scan":
    st.markdown("# ⚡ Run New SEO Scan")
    st.markdown(f"Scan **{DOMAIN}** now")
    st.divider()

    c1, c2 = st.columns(2)
    with c1: max_pages = st.slider("Max pages", 10, 300, 50, 10)
    with c2: modules   = st.multiselect("Modules", ["Technical Audit", "Content Analysis"], default=["Technical Audit"])
    st.info("💡 SERP tracking may be rate-limited by Google. Use the CLI script for reliable SERP data.")

    if st.button("🚀 Start Scan", type="primary", use_container_width=True):
        today = datetime.today().strftime("%Y-%m-%d")

        if "Technical Audit" in modules:
            st.markdown("### 🔍 Crawling...")
            prog = st.progress(0)
            stat = st.empty()
            results = crawl_site(SITE_URL, max_pages, prog, stat)
            if results:
                df = pd.DataFrame(results)
                df.to_csv(f"{OUTPUT_DIR}/{DOMAIN}_technical_audit_{today}.csv", index=False)
                os.system(f'cd {OUTPUT_DIR} && git add . && git commit -m "scan {today}" && git push')
                iss_count = len(df[df["issues"].astype(str).str.len() > 0])
                st.success(f"✅ Crawled {len(results)} pages. {iss_count} with issues.")
            else:
                st.error("No results.")

        if "Content Analysis" in modules:
            st.markdown("### 📝 Analyzing content...")
            today = datetime.today().strftime("%Y-%m-%d")
            urls = [SITE_URL]
            ap = f"{OUTPUT_DIR}/{DOMAIN}_technical_audit_{today}.csv"
            if os.path.exists(ap):
                adf = pd.read_csv(ap)
                urls += adf[adf["status"].astype(str) == "200"]["url"].tolist()[:30]
            prog2 = st.progress(0)
            kw_rows = []
            for i, url in enumerate(urls):
                prog2.progress((i + 1) / len(urls))
                kd = extract_page_keywords(url)
                if "error" not in kd:
                    kw_rows.append({
                        "url": kd["url"], "title": kd["title"], "h1": kd["h1"],
                        "h2s": kd["h2s"],
                        "top_words": ", ".join(f"{w}({c})" for w, c in kd.get("top_words", [])),
                        "top_bigrams": ", ".join(f"{b}({c})" for b, c in kd.get("top_bigrams", [])),
                        "word_count": kd.get("word_count", 0)
                    })
                time.sleep(0.5)
            if kw_rows:
                pd.DataFrame(kw_rows).to_csv(f"{OUTPUT_DIR}/{DOMAIN}_page_keywords_{today}.csv", index=False)
                all_kws = list(TRACKED_KEYWORDS)
                for r in kw_rows:
                    for h in str(r.get("h2s", "")).split(" | "):
                        h = h.strip()
                        if 10 < len(h) < 80: all_kws.append(h)
                clusters = defaultdict(list)
                for kw in all_kws:
                    w = kw.lower().split()
                    k = " ".join(w[:2]) if len(w) >= 2 else (w[0] if w else "other")
                    clusters[k].append(kw)
                crows = [{"cluster": k, "keyword_count": len(v), "keywords": " | ".join(v)}
                         for k, v in sorted(clusters.items(), key=lambda x: -len(x[1]))]
                pd.DataFrame(crows).to_csv(f"{OUTPUT_DIR}/{DOMAIN}_keyword_clusters_{today}.csv", index=False)
                st.success(f"✅ Analyzed {len(kw_rows)} pages.")
        st.balloons()
        st.info("Navigate to 📈 Growth Tracker to see trends!")

elif page == "✅ Fixed Issues":
    st.markdown("# ✅ Fixed Issues (Reports)")
    st.divider()

    fix_dates = get_fix_report_dates()
    if not fix_dates:
        st.info("No fix-issues reports found in seo_reports/. Run the CSV fixer or the GitHub Action first.")
    else:
        selected_fix_date = st.selectbox("Fix report date", fix_dates, index=0)
        fix_df = load_fix_issues(selected_fix_date)

        if fix_df is None or len(fix_df) == 0:
            st.warning("Selected fix report is empty.")
        else:
            st.markdown(
                f"Showing **{len(fix_df)}** rows from "
                f"`{DOMAIN}_fix_issues_{selected_fix_date}.csv`"
            )

            col1, col2 = st.columns(2)
            with col1:
                only_fixed = st.checkbox("Show only successfully fixed", value=True)
            with col2:
                url_search = st.text_input("Filter by URL contains")

            df_view = fix_df.copy()

            if "fixed" in df_view.columns and only_fixed:
                df_view = df_view[df_view["fixed"] == True]

            if url_search:
                df_view = df_view[
                    df_view["url"].astype(str).str.contains(url_search, case=False, na=False)
                ]

            if "changes" in df_view.columns:
                df_view["changes"] = df_view["changes"].astype(str)

            st.dataframe(df_view, use_container_width=True, height=500)

            st.download_button(
                "📥 Download Filtered Fixed Issues CSV",
                df_view.to_csv(index=False).encode(),
                f"{DOMAIN}_fixed_issues_view_{selected_fix_date}.csv",
                "text/csv",
            )

            if "issues" in df_view.columns:
                st.divider()
                st.markdown("### 🔍 Most Frequent Audit Issues (for these rows)")
                all_issue_fragments = []
                for v in df_view["issues"].astype(str):
                    for part in v.split(" | "):
                        p = part.strip()
                        if p:
                            all_issue_fragments.append(p)
                if all_issue_fragments:
                    issue_counts = (
                        pd.Series(all_issue_fragments)
                        .value_counts()
                        .head(10)
                        .reset_index()
                    )
                    issue_counts.columns = ["Issue (from audit)", "Count"]
                    st.dataframe(issue_counts, use_container_width=True)

else:
    st.info("Other pages (Growth Tracker, SERP, etc.) can be re-added as needed.")
