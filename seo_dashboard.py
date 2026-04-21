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
# # ──────────────────────────────────────────────────────────────
# # GA4 CLIENT
# # ──────────────────────────────────────────────────────────────

# def get_ga4_client():
#     return BetaAnalyticsDataClient.from_service_account_file(GA4_CREDENTIALS_PATH)

from google.oauth2 import service_account


def get_ga4_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["ga4"]
    )
    return BetaAnalyticsDataClient(credentials=credentials)


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
# WORDPRESS AUTOBOT: FETCH & APPLY
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

        result = apply_seo_fixes(post, dry_run=dry_run, min_score_to_fix=min_score_to_fix)
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
        "🛠️ Fix Issues",    # NEW
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
# (for brevity: only Overview + Technical Audit + Fix Issues shown;
#  you can paste in your other page code as needed)
# ──────────────────────────────────────────────────────────────

if page == "🏠 Overview":
    st.markdown("# 🏠 SEO Performance Overview")
    st.markdown(f"**{DOMAIN}** — Report for **{selected_date}**")
    st.divider()
    audit_df = load_audit(selected_date)
    serp_df = load_serp(selected_date)

    if audit_df is not None:
        total = len(audit_df)
        broken = len(audit_df[audit_df["status"].astype(str).str.match(r"^[45E]")])
        with_issues = len(audit_df[audit_df["issues"].astype(str).str.len() > 0])
        clean = total - with_issues
        avg_load = audit_df["load_time_s"].dropna().mean()

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Pages Crawled", total)
        c2.metric("Clean Pages", clean, delta=f"{clean/max(total,1)*100:.0f}%")
        c3.metric("Issues Found", with_issues, delta=f"-{with_issues}" if with_issues else "0", delta_color="inverse")
        c4.metric("Broken Pages", broken, delta=f"-{broken}" if broken else "0", delta_color="inverse")
        c5.metric("Avg Load Time", f"{avg_load:.2f}s" if pd.notna(avg_load) else "N/A")
        st.divider()

    # (You can bring back the full Overview implementation from your original file here)

elif page == "🔍 Technical Audit":
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
    st.markdown("# 🛠️ Fix SEO Issues on WordPress")
    st.markdown(
        "This connects to your WordPress site via REST using the `.env` credentials "
        "and auto-fixes SEO issues on posts."
    )
    st.divider()

    st.warning(
        "**Live changes ahead**:\n\n"
        "- Updates SEO titles and meta descriptions\n"
        "- Cleans slugs\n"
        "- Adds ALT attributes for images\n"
        "- Normalizes Yoast / Rank Math meta\n\n"
        "Make sure your `.env` has **WP_URL**, **WP_USER**, **WP_APP_PASSWORD`."
    )

    dry_run = st.checkbox("Dry run (no changes, just simulate & report)", value=True)
    min_score = st.slider("Minimum SEO score to fix", 0, 100, 80, 5)
    max_pages = st.slider("Max WordPress post pages to fetch", 1, 50, 10, 1)
    per_page = st.slider("Posts per page (WordPress API)", 5, 50, 10, 5)
    report_path = st.text_input("Report output file", "seo_report.json")

    only_from_audit = st.checkbox(
        "Only target URLs that have issues in latest Technical Audit (status=200)",
        value=False
    )

    audit_df = load_audit(selected_date) if only_from_audit else None

    if st.button("🚀 Run WordPress SEO Auto-Optimizer", type="primary", use_container_width=True):
        with st.spinner("Running WordPress SEO autobot… this may take a few minutes."):
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
                        "📥 Download fix report CSV",
                        df.to_csv(index=False).encode(),
                        "wp_autobot_fix_report.csv",
                        "text/csv"
                    )
                else:
                    st.info("No posts were processed.")
            except Exception as e:
                st.error(f"Error while running WordPress autobot: {e}")

else:
    st.info("Other pages (Growth Tracker, SERP, etc.) can be re-added as needed.")
