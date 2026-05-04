#!/usr/bin/env python3
"""
Centralized multi-site SEO Analytics Dashboard + WordPress SEO Auto-Optimizer.

A single Streamlit dashboard that manages every WordPress site registered in
sites_config.SITES.

  • Sidebar site picker → switches the entire dashboard's active site.
  • New "🌐 All Sites" landing tab → portfolio-wide KPIs across every site.
  • All existing per-site sections preserved (Overview, Growth, Audit, Traffic,
    Content, Keywords, Backlinks, Fixed Issues, Latest Posts).
  • Per-site report folders: seo_reports/<slug>/...
  • Falls back to legacy flat seo_reports/<DOMAIN>_*.csv layout.

Run:
    streamlit run dashboard.py
"""

from __future__ import annotations

import os
import re
import time
import json
import html as html_lib
from datetime import datetime
from collections import defaultdict, Counter
from urllib.parse import urljoin, urlparse
from base64 import b64encode
from difflib import SequenceMatcher

import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from bs4 import BeautifulSoup
import streamlit as st

from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest,
    FilterExpression,
    Filter,
    FilterExpressionList,
)

from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Multi-site registry
from sites_config import SITES, SITES_BY_DOMAIN, get_site, set_active

# Optional CSV-driven fixer module
try:
    from fix_issues import fix_from_audit, latest_audit_csv, fix_all_sites
except Exception:
    fix_from_audit = None
    latest_audit_csv = None
    fix_all_sites = None


# ──────────────────────────────────────────────────────────────────────────
# CORE CONFIG (constants — site-agnostic)
# ──────────────────────────────────────────────────────────────────────────

REPORTS_BASE = "seo_reports"
os.makedirs(REPORTS_BASE, exist_ok=True)

CRAWL_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SEO-Audit-Bot/1.0)"
}

BING_API_KEY = os.getenv("BING_API_KEY", "").strip()

SEO_TITLE_MIN = 50
SEO_TITLE_MAX = 60
META_DESC_MIN = 150
META_DESC_MAX = 160
REQUEST_DELAY = 1.2

SLUG_STOP_WORDS = {
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "was", "are", "were", "be", "been",
    "has", "have", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "this", "that", "these", "those", "it", "its",
}
LLM_STOP_WORDS = SLUG_STOP_WORDS | {
    "news", "briefing", "brief", "today", "update", "updates", "story", "stories",
    "read", "guide", "local", "latest", "new", "post", "posts", "page",
}
LLM_REFERRER_REGEX = r"(chat\.openai\.com|chatgpt\.com|perplexity\.ai|claude\.ai|anthropic\.com)"
LLM_SOURCE_REGEX   = r"(chatgpt|openai|perplexity|claude|anthropic)"
LLM_UTM_REGEX      = r".*utm_source=(chatgpt|perplexity|claude).*"
LLM_BOT_SIGNATURES = {
    "GPTBot": ["gptbot"],
    "ChatGPT-User": ["chatgpt-user"],
    "PerplexityBot": ["perplexitybot"],
    "ClaudeBot": ["claudebot", "claude-web"],
    "CCBot": ["ccbot"],
    "Google-Extended": ["google-extended"],
    "GoogleOther": ["googleother"],
    "Amazonbot": ["amazonbot"],
    "Bytespider": ["bytespider"],
    "Meta-ExternalAgent": ["meta-externalagent"],
    "OAI-SearchBot": ["oai-searchbot"],
    "Applebot-Extended": ["applebot-extended"],
}


# ──────────────────────────────────────────────────────────────────────────
# ACTIVE-SITE GLOBALS — set once per Streamlit run from session_state
# ──────────────────────────────────────────────────────────────────────────
#
# These names match the original single-site script so the unchanged render
# functions below keep working. They are repointed at the start of every run
# based on the user's sidebar selection.

GA4_PROPERTY_ID: str = ""
SITE_URL:        str = ""
DOMAIN:          str = ""
BRAND_NAME:      str = ""
SITE_DESCRIPTION:str = ""
BRAND_LOGO_URL:  str = ""
WP_URL:          str = ""
WP_USER:         str = ""
WP_APP_PASS:     str = ""
API_BASE:        str = ""
TRACKED_KEYWORDS: list[str] = []
COMPETITORS:      list[str] = []
OUTPUT_DIR:       str = REPORTS_BASE  # per-site dir, set in _bind_active_site


def _bind_active_site(domain: str) -> None:
    """Repoint the module-level constants at the chosen site for this run."""
    global GA4_PROPERTY_ID, SITE_URL, DOMAIN, BRAND_NAME, SITE_DESCRIPTION
    global BRAND_LOGO_URL, WP_URL, WP_USER, WP_APP_PASS, API_BASE
    global TRACKED_KEYWORDS, COMPETITORS, OUTPUT_DIR

    site = set_active(domain)
    GA4_PROPERTY_ID  = site.ga4_property_id
    SITE_URL         = site.site_url
    DOMAIN           = site.domain
    BRAND_NAME       = site.brand_name
    SITE_DESCRIPTION = site.site_description
    BRAND_LOGO_URL   = site.brand_logo_url
    WP_URL           = site.wp_url
    WP_USER          = site.wp_user
    WP_APP_PASS      = site.wp_app_pass
    API_BASE         = site.api_base
    TRACKED_KEYWORDS = list(site.tracked_keywords)
    COMPETITORS      = list(site.competitors)
    OUTPUT_DIR       = site.output_dir(REPORTS_BASE)


# ──────────────────────────────────────────────────────────────────────────
# GA4
# ──────────────────────────────────────────────────────────────────────────

def get_ga4_client():
    """Build a GA4 client from Streamlit secrets ([ga4] table)."""
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["ga4"]
    )
    return BetaAnalyticsDataClient(credentials=credentials)


# ──────────────────────────────────────────────────────────────────────────
# HTTP SESSION
# ──────────────────────────────────────────────────────────────────────────

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
        total=5, backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"], raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=2, pool_maxsize=5)
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
            return fn(url, **kwargs)
        except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError) as e:
            wait = 2 ** attempt
            if attempt < max_attempts:
                time.sleep(wait)
                SESSION = _make_session()
            else:
                raise e


def _auth_header():
    if not WP_USER or not WP_APP_PASS:
        raise RuntimeError("Missing WP_USER or WP_APP_PASSWORD for active site.")
    token = b64encode(f"{WP_USER}:{WP_APP_PASS}".encode()).decode()
    return {"Authorization": f"Basic {token}", "Content-Type": "application/json"}


# ──────────────────────────────────────────────────────────────────────────
# TEXT HELPERS
# ──────────────────────────────────────────────────────────────────────────

def clean_html_entities(text):
    return html_lib.unescape(text or "")


def strip_html_tags(text):
    return BeautifulSoup(text or "", "html.parser").get_text(separator=" ").strip()


def html_to_text(text):
    return re.sub(r"\s+", " ", strip_html_tags(text)).strip()


def word_count(text):
    return len(strip_html_tags(text).split())


def clean_text_snippet(text, max_len=220):
    text = re.sub(r"\s+", " ", (text or "").strip())
    if len(text) <= max_len:
        return text
    return text[:max_len].rsplit(" ", 1)[0] + "…"


def normalize_url(url):
    url = (url or "").strip()
    if not url:
        return ""
    parsed = urlparse(url)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip("/")
    return f"{scheme}://{netloc}{path}"


def same_url(a, b):
    return normalize_url(a) == normalize_url(b)


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


def generate_seo_title(raw_title, site_name=None):
    site_name = site_name or BRAND_NAME
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
    soup = BeautifulSoup(content_html or "", "html.parser")
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
    soup = BeautifulSoup(content_html or "", "html.parser")
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
    cleaned = dict(meta or {})
    if "_yoast_wpseo_opengraph-title" in cleaned:
        for key in ["rank_math_facebook_title", "rank_math_facebook_description",
                    "rank_math_twitter_title", "rank_math_twitter_description"]:
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
        issues.append(f"SEO title too short ({tlen} chars, min {SEO_TITLE_MIN})"); score -= 15
    elif tlen > SEO_TITLE_MAX:
        issues.append(f"SEO title too long ({tlen} chars, max {SEO_TITLE_MAX})"); score -= 10
    mlen = len(meta_desc)
    if mlen < META_DESC_MIN:
        issues.append(f"Meta description too short ({mlen} chars, min {META_DESC_MIN})"); score -= 20
    elif mlen > META_DESC_MAX:
        issues.append(f"Meta description too long ({mlen} chars, max {META_DESC_MAX})"); score -= 10
    if len(slug) > 75:
        issues.append(f"Slug too long ({len(slug)} chars)"); score -= 10
    wc = word_count(content_html)
    if wc < 300:
        issues.append(f"Content too thin ({wc} words, aim for 600+)"); score -= 20
    elif wc < 600:
        issues.append(f"Content could be longer ({wc} words, aim for 600+)"); score -= 10
    soup = BeautifulSoup(content_html or "", "html.parser")
    if not soup.find(["h2", "h3"]):
        issues.append("No subheadings (H2/H3) found in content"); score -= 10
    plain = soup.get_text().lower()
    kw_found = sum(1 for k in keywords if k in plain)
    if keywords and kw_found == 0:
        issues.append("Focus keywords not found in content"); score -= 10
    return max(0, score), issues


# ──────────────────────────────────────────────────────────────────────────
# REPORT FILE HELPERS — per-site folder + legacy flat fallback
# ──────────────────────────────────────────────────────────────────────────

def _candidate_dirs(domain: str | None = None) -> list[str]:
    """Where to look for CSVs, in priority order."""
    domain = domain or DOMAIN
    site = SITES_BY_DOMAIN.get(domain) if domain else None
    dirs = []
    if site:
        dirs.append(site.output_dir(REPORTS_BASE))  # per-site
    dirs.append(REPORTS_BASE)                       # legacy flat
    return dirs


def _find_csv(prefix_with_date: str, domain: str | None = None) -> str | None:
    """Find a CSV with a given basename across candidate dirs."""
    fname = f"{prefix_with_date}.csv"
    for d in _candidate_dirs(domain):
        path = os.path.join(d, fname)
        if os.path.exists(path):
            return path
    return None


def load_clickfarm_today(domain: str | None = None):
    today = datetime.today().strftime("%Y-%m-%d")
    path = _find_csv(f"traffic_generated_{today}", domain)
    if path:
        return pd.read_csv(path)
    return None


def get_report_dates(domain: str | None = None):
    """Union of dates across legacy flat dir and the active site's per-site dir."""
    dates = set()
    for d in _candidate_dirs(domain):
        if not os.path.isdir(d):
            continue
        for f in os.listdir(d):
            m = re.search(r"(\d{4}-\d{2}-\d{2})", f)
            if m:
                dates.add(m.group(1))
    return sorted(dates, reverse=True)


def load_csv(prefix, date, domain: str | None = None):
    path = _find_csv(f"{prefix}_{date}", domain)
    return pd.read_csv(path) if path else None


def load_audit(date, domain: str | None = None):
    return load_csv(f"{(domain or DOMAIN)}_technical_audit", date, domain)


def load_serp(date, domain: str | None = None):
    return load_csv("serp_tracking", date, domain)


def load_keywords(date, domain: str | None = None):
    return load_csv(f"{(domain or DOMAIN)}_page_keywords", date, domain)


def load_clusters(date, domain: str | None = None):
    return load_csv(f"{(domain or DOMAIN)}_keyword_clusters", date, domain)


def load_llm_visibility(date, domain: str | None = None):
    return load_csv(f"{(domain or DOMAIN)}_llm_visibility", date, domain)


def get_fix_report_dates(domain: str | None = None):
    domain = domain or DOMAIN
    dates = set()
    for d in _candidate_dirs(domain):
        if not os.path.isdir(d):
            continue
        for f in os.listdir(d):
            m = re.search(rf"{re.escape(domain)}_fix_issues_(\d{{4}}-\d{{2}}-\d{{2}})\.csv", f)
            if m:
                dates.add(m.group(1))
    return sorted(dates, reverse=True)


def load_fix_issues(date, domain: str | None = None):
    return load_csv(f"{(domain or DOMAIN)}_fix_issues", date, domain)


# ──────────────────────────────────────────────────────────────────────────
# PERFORMANCE / WP / SCHEMA / LINKING HELPERS
# (carried over from the single-site script — they read the active-site
# globals (DOMAIN, API_BASE, BRAND_NAME, …) which are repointed each run)
# ──────────────────────────────────────────────────────────────────────────

def audit_page_performance(url):
    perf = {"response_time_s": None, "html_size_kb": None, "request_count": None,
            "js_unminified": [], "css_unminified": [], "perf_issues": []}
    try:
        start = time.time()
        r = safe_request("get", url, timeout=15, headers={"User-Agent": CRAWL_HEADERS["User-Agent"]})
        elapsed = round(time.time() - start, 3)
        perf["response_time_s"] = elapsed
        html_kb = round(len(r.content) / 1024, 1)
        perf["html_size_kb"] = html_kb
        soup = BeautifulSoup(r.text, "html.parser")
        scripts = soup.find_all("script", src=True)
        stylesheets = soup.find_all("link", rel=lambda v: v and "stylesheet" in v)
        images_tag = soup.find_all("img", src=True)
        iframes = soup.find_all("iframe", src=True)
        perf["request_count"] = len(scripts) + len(stylesheets) + len(images_tag) + len(iframes)
        for tag in scripts:
            src = tag.get("src", "")
            if src and ".js" in src and ".min.js" not in src and "cdn" not in src.lower():
                perf["js_unminified"].append(src.split("?")[0])
        for tag in stylesheets:
            href = tag.get("href", "")
            if href and ".css" in href and ".min.css" not in href and "cdn" not in href.lower():
                perf["css_unminified"].append(href.split("?")[0])
        if elapsed > 0.2: perf["perf_issues"].append(f"Response time {elapsed}s exceeds 0.2s recommendation")
        if html_kb > 50: perf["perf_issues"].append(f"HTML document is {html_kb} KB (recommendation: ≤ 50 KB)")
        if perf["request_count"] > 20: perf["perf_issues"].append(f"Page makes ~{perf['request_count']} requests (recommendation: ≤ 20)")
        if perf["js_unminified"]: perf["perf_issues"].append(f"{len(perf['js_unminified'])} JS file(s) appear unminified")
        if perf["css_unminified"]: perf["perf_issues"].append(f"{len(perf['css_unminified'])} CSS file(s) appear unminified")
    except Exception as e:
        perf["perf_issues"].append(f"Performance audit error: {e}")
    return perf


def get_all_posts(status="publish", per_page=10, max_pages=5):
    all_posts = []
    for page in range(1, max_pages + 1):
        r = safe_request("get", f"{API_BASE}/posts", headers=_auth_header(),
                         params={"status": status, "per_page": per_page, "page": page,
                                 "context": "edit",
                                 "_fields": "id,title,slug,content,excerpt,meta,link,modified,modified_gmt,date,date_gmt,author,categories"})
        if not r.ok or not r.json():
            break
        batch = r.json()
        total_pages = int(r.headers.get("X-WP-TotalPages", 1))
        all_posts.extend(batch)
        if page >= total_pages:
            break
        time.sleep(REQUEST_DELAY)
    return all_posts


def fetch_latest_posts(n=30):
    posts = []
    remaining = n
    page = 1
    per_page = min(n, 100)
    while remaining > 0 and page <= 10:
        r = safe_request("get", f"{API_BASE}/posts",
                         params={"status": "publish", "per_page": min(per_page, remaining),
                                 "page": page, "_embed": "author,wp:featuredmedia,wp:term",
                                 "orderby": "date", "order": "desc"})
        if not r.ok or not r.json():
            break
        batch = r.json()
        posts.extend(batch)
        remaining -= len(batch)
        total_pages = int(r.headers.get("X-WP-TotalPages", 1))
        if page >= total_pages:
            break
        page += 1
        time.sleep(REQUEST_DELAY)
    return posts[:n]


def summarize_latest_posts(posts):
    rows = []
    for p in posts:
        title = clean_html_entities((p.get("title") or {}).get("rendered", ""))
        link = p.get("link", "")
        date_str = (p.get("date_gmt") or p.get("date") or "")[:10]
        modified_str = (p.get("modified_gmt") or p.get("modified") or "")[:10]
        embedded = p.get("_embedded") or {}
        authors = embedded.get("author") or []
        author = authors[0].get("name", "") if authors and isinstance(authors, list) else ""
        cats = []
        for term_group in (embedded.get("wp:term") or []):
            for term in term_group:
                if term.get("taxonomy") == "category":
                    cats.append(term.get("name", ""))
        category_str = ", ".join(c for c in cats if c)
        raw_html = ((p.get("content") or {}).get("rendered")) or ""
        text_only = re.sub(r"<[^>]+>", " ", raw_html)
        text_only = html_lib.unescape(text_only)
        wc = len(re.findall(r"\w+", text_only))
        featured = ""
        media = embedded.get("wp:featuredmedia") or []
        if media and isinstance(media, list):
            featured = media[0].get("source_url", "") or ""
        excerpt_html = ((p.get("excerpt") or {}).get("rendered")) or ""
        excerpt = clean_html_entities(re.sub(r"<[^>]+>", " ", excerpt_html)).strip()
        if len(excerpt) > 220:
            excerpt = excerpt[:217].rstrip() + "…"
        rows.append({"title": title, "url": link, "author": author, "category": category_str,
                     "published": date_str, "modified": modified_str, "word_count": wc,
                     "excerpt": excerpt, "featured_image": featured})
    return rows


def get_post_by_slug(slug):
    r = safe_request("get", f"{API_BASE}/posts", headers=_auth_header(),
                     params={"slug": slug, "context": "edit"})
    if r.ok and r.json():
        return r.json()[0]
    return None


def get_wp_author_name(author_id):
    if not author_id:
        return BRAND_NAME
    try:
        r = safe_request("get", f"{API_BASE}/users/{author_id}", headers=_auth_header(), timeout=20)
        if r.ok:
            data = r.json()
            return data.get("name") or data.get("slug") or BRAND_NAME
    except Exception:
        pass
    return BRAND_NAME


def get_wp_categories_map():
    categories = {}
    page = 1
    while True:
        try:
            r = safe_request("get", f"{API_BASE}/categories", headers=_auth_header(),
                             params={"per_page": 100, "page": page, "_fields": "id,name,slug"},
                             timeout=20)
            if not r.ok:
                break
            rows = r.json() or []
            if not rows:
                break
            for row in rows:
                categories[row.get("id")] = row.get("name") or row.get("slug") or ""
            total_pages = int(r.headers.get("X-WP-TotalPages", 1))
            if page >= total_pages:
                break
            page += 1
            time.sleep(REQUEST_DELAY)
        except Exception:
            break
    return categories


# ── Schema / linking / fixers (unchanged from single-site) ──────────────

def build_newsarticle_schema(post, meta_desc="", author_name="", categories_map=None):
    categories_map = categories_map or {}
    title = clean_html_entities(post.get("title", {}).get("rendered", ""))
    content_html = post.get("content", {}).get("rendered", "")
    excerpt_html = post.get("excerpt", {}).get("rendered", "")
    canonical = post.get("link") or f"{SITE_URL}/{post.get('slug','').strip('/')}/"
    soup = BeautifulSoup(content_html or "", "html.parser")
    first_img = soup.find("img", src=True)
    image_url = first_img.get("src", "") if first_img else ""
    if not meta_desc:
        meta_desc = generate_meta_description(content_html or excerpt_html, title, extract_keywords_from_title(title))
    date_published = post.get("date_gmt") or post.get("date") or datetime.utcnow().isoformat()
    date_modified = post.get("modified_gmt") or post.get("modified") or date_published
    author_name = author_name or get_wp_author_name(post.get("author"))
    article_section = [categories_map.get(cid, "") for cid in (post.get("categories", []) or []) if categories_map.get(cid)]
    schema = {
        "@context": "https://schema.org", "@type": "NewsArticle",
        "mainEntityOfPage": {"@type": "WebPage", "@id": canonical},
        "headline": clean_text_snippet(title, 110),
        "description": clean_text_snippet(html_to_text(excerpt_html) or meta_desc or SITE_DESCRIPTION, 220),
        "url": canonical, "datePublished": date_published, "dateModified": date_modified,
        "author": {"@type": "Person" if author_name != BRAND_NAME else "Organization", "name": author_name},
        "publisher": {"@type": "Organization", "name": BRAND_NAME,
                      "logo": {"@type": "ImageObject", "url": BRAND_LOGO_URL}},
    }
    if image_url:
        schema["image"] = [image_url]
    if article_section:
        schema["articleSection"] = article_section
    keywords = extract_keywords_from_title(title)
    if keywords:
        schema["keywords"] = keywords
    return schema


def upsert_json_ld_schema(content_html, schema_obj):
    soup = BeautifulSoup(content_html or "", "html.parser")
    schema_json = json.dumps(schema_obj, ensure_ascii=False, separators=(",", ":"))
    existing = None
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = (tag.string or tag.get_text(" ", strip=True) or "").strip()
        try:
            parsed = json.loads(raw)
        except Exception:
            continue
        parsed_list = parsed if isinstance(parsed, list) else [parsed]
        if any(str(item.get("@type", "")).lower() in {"newsarticle", "article"}
               for item in parsed_list if isinstance(item, dict)):
            existing = tag
            break
    if existing is not None:
        existing.string = schema_json
    else:
        script_tag = soup.new_tag("script", type="application/ld+json")
        script_tag.string = schema_json
        soup.append(script_tag)
    return str(soup)


def tokenize_for_linking(text):
    text = re.sub(r"[^a-z0-9\s-]", " ", (text or "").lower())
    return [w for w in text.split() if w and w not in LLM_STOP_WORDS and len(w) > 2]


def post_to_link_record(post, categories_map=None):
    categories_map = categories_map or {}
    title = clean_html_entities(post.get("title", {}).get("rendered", ""))
    excerpt = html_to_text(post.get("excerpt", {}).get("rendered", ""))
    link = post.get("link") or f"{SITE_URL}/{post.get('slug','').strip('/')}/"
    slug = post.get("slug", "")
    cat_names = [categories_map.get(cid, "") for cid in (post.get("categories", []) or []) if categories_map.get(cid)]
    source_text = " ".join([title, excerpt, slug.replace("-", " "), " ".join(cat_names)])
    return {"id": post.get("id"), "url": link, "slug": slug, "title": title,
            "keywords": extract_keywords_from_title(title), "tokens": tokenize_for_linking(source_text),
            "categories": cat_names, "content_html": post.get("content", {}).get("rendered", "")}


def suggest_internal_links_for_post(post, all_posts, categories_map=None, max_suggestions=5):
    categories_map = categories_map or {}
    current = post_to_link_record(post, categories_map)
    current_tokens = set(current["tokens"])
    suggestions = []
    for candidate_post in all_posts:
        if candidate_post.get("id") == post.get("id"):
            continue
        candidate = post_to_link_record(candidate_post, categories_map)
        overlap = len(current_tokens.intersection(set(candidate["tokens"])))
        title_sim = SequenceMatcher(None, current["title"].lower(), candidate["title"].lower()).ratio()
        cat_overlap = len(set(current["categories"]).intersection(set(candidate["categories"])))
        score = overlap * 3 + int(title_sim * 20) + cat_overlap * 5
        anchor = " ".join(candidate["keywords"][:3]).strip() or candidate["title"]
        if score > 6:
            suggestions.append({"target_id": candidate["id"], "target_title": candidate["title"],
                                "target_url": candidate["url"], "anchor_text": anchor[:80], "score": score})
    suggestions = sorted(suggestions, key=lambda x: x["score"], reverse=True)
    deduped, seen_urls = [], set()
    for s in suggestions:
        if s["target_url"] not in seen_urls:
            deduped.append(s); seen_urls.add(s["target_url"])
        if len(deduped) >= max_suggestions:
            break
    return deduped


def insert_internal_links(content_html, suggestions, max_links=3):
    soup = BeautifulSoup(content_html or "", "html.parser")
    paragraphs = soup.find_all("p")
    inserted = 0
    for suggestion in suggestions:
        if inserted >= max_links:
            break
        anchor = suggestion["anchor_text"].strip()
        target_url = suggestion["target_url"]
        if not anchor or not target_url:
            continue
        for p in paragraphs:
            p_html = str(p)
            p_text = p.get_text(" ", strip=True)
            if len(p_text) < 60 or target_url in p_html:
                continue
            anchor_regex = re.compile(rf"\b({re.escape(anchor)})\b", re.I)
            if anchor_regex.search(p_text):
                new_html = anchor_regex.sub(rf'<a href="{target_url}">\1</a>',
                                            p.decode_contents(), count=1)
                p.clear()
                p.append(BeautifulSoup(new_html, "html.parser"))
                inserted += 1
                break
    return str(soup), inserted


def find_unlinked_mentions(query_brand=None, domain=None, count=20):
    query_brand = query_brand or BRAND_NAME
    domain = domain or DOMAIN
    if not BING_API_KEY:
        return []
    endpoint = "https://api.bing.microsoft.com/v7.0/search"
    query = f'"{query_brand}" -site:{domain}'
    try:
        r = safe_request("get", endpoint,
                         headers={"Ocp-Apim-Subscription-Key": BING_API_KEY},
                         params={"q": query, "count": count, "textDecorations": False, "textFormat": "Raw"},
                         timeout=20)
        if not r.ok:
            return []
        data = r.json()
        values = data.get("webPages", {}).get("value", [])
        rows = []
        for item in values:
            rows.append({
                "name": item.get("name", ""), "url": item.get("url", ""),
                "snippet": item.get("snippet", ""),
                "domain": urlparse(item.get("url", "")).netloc,
                "brand_mentioned": query_brand.lower() in item.get("snippet", "").lower()
                                   or query_brand.lower() in item.get("name", "").lower(),
            })
        return rows
    except Exception:
        return []


def score_backlink_targets(rows):
    scored = []
    for row in rows:
        domain = row.get("domain", "")
        snippet = (row.get("snippet", "") or "").lower()
        url = row.get("url", "")
        score = 0
        if any(c in domain for c in [".org", ".edu", ".gov"]):
            score += 25
        if any(word in snippet for word in ["news", "report", "guide", "resource", "analysis", "coverage"]):
            score += 20
        if any(word in url.lower() for word in ["resources", "news", "blog", "local", "media"]):
            score += 15
        if row.get("brand_mentioned"):
            score += 30
        if domain and DOMAIN not in domain:
            score += 10
        row = dict(row)
        row["pitch_score"] = min(score, 100)
        scored.append(row)
    return sorted(scored, key=lambda x: x["pitch_score"], reverse=True)


# ── Snapshot helpers ────────────────────────────────────────────────────

def fetch_traffic_by_source(days=30):
    SOURCES_OF_INTEREST = ["google", "bing", "yahoo", "duckduckgo", "baidu",
                           "chatgpt", "openai", "perplexity", "claude", "anthropic",
                           "gemini", "copilot", "you.com"]
    try:
        client = get_ga4_client()
        request = RunReportRequest(
            property=f"properties/{GA4_PROPERTY_ID}",
            dimensions=[{"name": "sessionSource"}, {"name": "sessionMedium"},
                        {"name": "sessionDefaultChannelGroup"}],
            metrics=[{"name": "sessions"}, {"name": "activeUsers"}, {"name": "screenPageViews"},
                     {"name": "bounceRate"}, {"name": "averageSessionDuration"}],
            date_ranges=[{"start_date": f"{days}daysAgo", "end_date": "today"}],
            limit=500,
        )
        response = client.run_report(request)
        rows = []
        for row in response.rows:
            source = row.dimension_values[0].value.lower()
            medium = row.dimension_values[1].value.lower()
            channel = row.dimension_values[2].value
            sessions = int(row.metric_values[0].value)
            users = int(row.metric_values[1].value)
            views = int(row.metric_values[2].value)
            bounce = round(float(row.metric_values[3].value) * 100, 1)
            avg_dur = round(float(row.metric_values[4].value), 1)
            if any(s in source for s in ["chatgpt", "openai", "perplexity", "claude", "anthropic", "gemini", "copilot"]):
                source_type = "AI / LLM"
            elif medium in ["organic", "cpc", "paid"]:
                source_type = "Search Engine"
            elif medium in ["referral", "social"]:
                source_type = "Referral / Social"
            elif medium == "(none)" and source == "(direct)":
                source_type = "Direct"
            else:
                source_type = "Other"
            rows.append({"source": row.dimension_values[0].value,
                         "medium": row.dimension_values[1].value, "channel": channel,
                         "source_type": source_type, "sessions": sessions, "users": users,
                         "pageviews": views, "bounce_rate_pct": bounce,
                         "avg_session_duration_s": avg_dur})
        df = pd.DataFrame(rows)
        if len(df) == 0:
            return df
        mask = (df["source"].str.lower().apply(lambda s: any(x in s for x in SOURCES_OF_INTEREST))
                | df["medium"].str.lower().isin(["organic", "cpc"])
                | (df["source_type"] == "AI / LLM"))
        return df[mask].sort_values("sessions", ascending=False)
    except Exception as e:
        st.error(f"Referral source fetch error: {e}")
        return None


def compute_audit_snapshot(df):
    if df is None or len(df) == 0:
        return None
    total = len(df)
    broken = len(df[df["status"].astype(str).str.match(r"^[45E]")])
    with_issues = len(df[df["issues"].astype(str).str.len() > 0])
    clean = total - with_issues
    avg_load = df["load_time_s"].dropna().mean() if "load_time_s" in df else None
    slow = len(df[df["load_time_s"].dropna() > 3.0]) if "load_time_s" in df else 0
    missing_title = len(df[df["title_length"] == 0]) if "title_length" in df else 0
    missing_meta = len(df[df["meta_desc_length"] == 0]) if "meta_desc_length" in df else 0
    no_schema = len(df[~df["has_schema"].astype(bool)]) if "has_schema" in df else 0
    no_og = len(df[~df["has_og_tags"].astype(bool)]) if "has_og_tags" in df else 0
    return {"total_pages": total, "clean_pages": clean, "pages_with_issues": with_issues,
            "broken_pages": broken,
            "avg_load_time": round(avg_load, 3) if pd.notna(avg_load) else None,
            "slow_pages": slow, "missing_title": missing_title, "missing_meta": missing_meta,
            "no_schema": no_schema, "no_og": no_og,
            "health_score": round((clean / total) * 100, 1) if total else 0}


def compute_serp_snapshot(df):
    if df is None or len(df) == 0:
        return None
    ss = df[df["site"] == DOMAIN] if "site" in df.columns else df
    pos = pd.to_numeric(ss["our_position"], errors="coerce")
    return {"top3": int((pos <= 3).sum()), "top10": int((pos <= 10).sum()),
            "top20": int((pos <= 20).sum()), "not_ranked": int(pos.isna().sum()),
            "avg_position": round(pos.dropna().mean(), 1) if len(pos.dropna()) > 0 else None}


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


# ── GA4 helpers ─────────────────────────────────────────────────────────

def fetch_ga4_data(days=7):
    if not GA4_PROPERTY_ID:
        return None
    try:
        client = get_ga4_client()
        request = RunReportRequest(
            property=f"properties/{GA4_PROPERTY_ID}",
            dimensions=[{"name": "date"}],
            metrics=[{"name": "activeUsers"}, {"name": "sessions"}, {"name": "screenPageViews"}],
            date_ranges=[{"start_date": f"{days}daysAgo", "end_date": "today"}],
        )
        response = client.run_report(request)
        data = []
        for row in response.rows:
            raw_date = row.dimension_values[0].value
            formatted_date = datetime.strptime(raw_date, "%Y%m%d").strftime("%d/%m/%Y")
            data.append({"date": formatted_date,
                         "users": int(row.metric_values[0].value),
                         "sessions": int(row.metric_values[1].value),
                         "pageviews": int(row.metric_values[2].value)})
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"GA4 Error: {e}")
        return None


def fetch_top_pages():
    if not GA4_PROPERTY_ID:
        return None
    try:
        client = get_ga4_client()
        request = RunReportRequest(
            property=f"properties/{GA4_PROPERTY_ID}",
            dimensions=[{"name": "pagePath"}],
            metrics=[{"name": "screenPageViews"}],
            date_ranges=[{"start_date": "7daysAgo", "end_date": "today"}],
        )
        response = client.run_report(request)
        rows = [{"page": r.dimension_values[0].value, "views": int(r.metric_values[0].value)}
                for r in response.rows]
        return pd.DataFrame(rows).sort_values("views", ascending=False).head(10)
    except Exception as e:
        st.error(f"Top Pages Error: {e}")
        return None


# ── Cross-site GA4 (used by All Sites view) ─────────────────────────────

@st.cache_data(ttl=600, show_spinner=False)
def fetch_ga4_users_for_property(ga4_id: str, days: int = 7) -> int | None:
    """Return total active users for a GA4 property. None on error."""
    if not ga4_id:
        return None
    try:
        client = get_ga4_client()
        request = RunReportRequest(
            property=f"properties/{ga4_id}",
            dimensions=[{"name": "date"}],
            metrics=[{"name": "activeUsers"}],
            date_ranges=[{"start_date": f"{days}daysAgo", "end_date": "today"}],
        )
        response = client.run_report(request)
        return sum(int(r.metric_values[0].value) for r in response.rows)
    except Exception:
        return None


def latest_audit_for_domain(domain: str) -> pd.DataFrame | None:
    """Find the newest technical-audit CSV for a given domain (any date)."""
    site = SITES_BY_DOMAIN.get(domain)
    candidates: list[str] = []
    for d in _candidate_dirs(domain):
        if not os.path.isdir(d):
            continue
        for f in os.listdir(d):
            if re.fullmatch(rf"{re.escape(domain)}_technical_audit_\d{{4}}-\d{{2}}-\d{{2}}\.csv", f):
                candidates.append(os.path.join(d, f))
    if not candidates:
        return None
    latest = sorted(candidates)[-1]
    try:
        return pd.read_csv(latest)
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────────────────
# STREAMLIT UI
# ──────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Centralized SEO Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');
    html, body, .stApp { font-family: 'DM Sans', sans-serif; }
    .seo-section-anchor { scroll-margin-top: 80px; padding-top: 1.5rem; }
    .site-pill {
        display: inline-block; padding: 0.15rem 0.55rem; border-radius: 999px;
        background: #eef5f5; color: #01696f; font-size: 0.75rem; font-weight: 500;
        margin-left: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)


# ─── Sidebar: site picker + view picker ────────────────────────────────

with st.sidebar:
    st.markdown("## 🌐 Sites")
    domain_options = [s.domain for s in SITES]
    default_idx = 0
    prev = st.session_state.get("active_site")
    if prev in domain_options:
        default_idx = domain_options.index(prev)

    selected_domain = st.selectbox(
        "Active site",
        domain_options,
        index=default_idx,
        format_func=lambda d: f"{SITES_BY_DOMAIN[d].brand_name} — {d}",
        key="active_site",
    )

    view = st.radio(
        "View",
        ["🌐 All Sites (portfolio)", "🔎 Single-site dashboard"],
        index=0,
        key="view_mode",
    )

    st.divider()
    st.caption(f"Managing **{len(SITES)}** WordPress sites.")
    st.caption("Reports root: `seo_reports/<site>/`")


# Bind active-site globals up front so any helper running below sees them.
_bind_active_site(selected_domain)
ACTIVE_SITE = SITES_BY_DOMAIN[selected_domain]


# ──────────────────────────────────────────────────────────────────────────
# 🌐 ALL SITES — portfolio view
# ──────────────────────────────────────────────────────────────────────────

def _render_all_sites():
    st.markdown("# 🌐 All Sites — Portfolio Overview")
    st.caption(f"Managing **{len(SITES)}** WordPress sites from one dashboard.")
    st.divider()

    days = st.selectbox(
        "GA4 window",
        [7, 14, 30],
        index=0,
        format_func=lambda d: f"Last {d} days",
        key="all_sites_days",
    )

    # Build cross-site rollup
    rollup_rows = []
    progress = st.progress(0.0, text="Aggregating per-site KPIs…")
    for i, site in enumerate(SITES):
        audit_df = latest_audit_for_domain(site.domain)
        snap = compute_audit_snapshot(audit_df) if audit_df is not None else None
        cf_df = load_clickfarm_today(site.domain)
        clickfarm_total = int(cf_df["clicks"].sum()) if cf_df is not None and "clicks" in cf_df else 0

        users = fetch_ga4_users_for_property(site.ga4_property_id, days=days) if site.ga4_property_id else None

        rollup_rows.append({
            "Site": site.brand_name,
            "Domain": site.domain,
            "Pages crawled": (snap or {}).get("total_pages", 0),
            "Health %": (snap or {}).get("health_score", 0),
            "Issues": (snap or {}).get("pages_with_issues", 0),
            "Broken": (snap or {}).get("broken_pages", 0),
            "Avg load (s)": (snap or {}).get("avg_load_time"),
            f"Users ({days}d)": users if users is not None else 0,
            "Bot clicks (today)": clickfarm_total,
            "GA4 OK": "✅" if users is not None else "—",
        })
        progress.progress((i + 1) / len(SITES), text=f"{site.domain} done")
    progress.empty()

    df = pd.DataFrame(rollup_rows)

    # Top KPIs
    total_users = int(df[f"Users ({days}d)"].fillna(0).sum())
    total_clicks = int(df["Bot clicks (today)"].fillna(0).sum())
    healthy_sites = int((df["Health %"].fillna(0) >= 90).sum())

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Sites managed", len(SITES))
    k2.metric(f"Total users ({days}d)", f"{total_users:,}")
    k3.metric("Total bot clicks (today)", f"{total_clicks:,}")
    k4.metric("Pages crawled (total)", int(df["Pages crawled"].fillna(0).sum()))
    k5.metric("Issues (total)", int(df["Issues"].fillna(0).sum()))
    k6.metric("Sites ≥ 90% health", f"{healthy_sites}/{len(SITES)}")

    st.divider()

    # Comparison bar — Users
    cols = st.columns(2)
    with cols[0]:
        st.markdown("### 👥 Users by site")
        plot_df = df[["Site", f"Users ({days}d)"]].rename(columns={f"Users ({days}d)": "Users"})
        if plot_df["Users"].sum() > 0:
            fig = px.bar(plot_df.sort_values("Users", ascending=False),
                         x="Site", y="Users", color="Site",
                         title=f"GA4 active users — last {days} days")
            fig.update_layout(showlegend=False, xaxis_tickangle=-30)
            st.plotly_chart(fig, use_container_width=True, key="all_sites_users")
        else:
            st.info("No GA4 data available across sites (check `[ga4]` secret and property IDs).")

    with cols[1]:
        st.markdown("### 🤖 Bot clicks by site (today)")
        cf_plot = df[["Site", "Bot clicks (today)"]].rename(columns={"Bot clicks (today)": "Clicks"})
        if cf_plot["Clicks"].sum() > 0:
            fig_cf = px.bar(cf_plot.sort_values("Clicks", ascending=False),
                            x="Site", y="Clicks", color="Site",
                            title="Click-farm clicks generated today")
            fig_cf.update_layout(showlegend=False, xaxis_tickangle=-30)
            st.plotly_chart(fig_cf, use_container_width=True, key="all_sites_clicks")
        else:
            st.info("No bot clicks logged today. Run `python bypass.py` to populate.")

    st.markdown("### 🩺 Site health")
    if df["Pages crawled"].sum() > 0:
        fig = px.bar(df.sort_values("Health %", ascending=False),
                     x="Site", y="Health %", color="Health %",
                     color_continuous_scale="RdYlGn", range_color=[0, 100])
        fig.update_layout(xaxis_tickangle=-30)
        st.plotly_chart(fig, use_container_width=True, key="all_sites_health")
    else:
        st.info("No audits found yet. Run `python crawl_script.py` to populate.")

    st.divider()
    st.markdown("### 📋 Comparison table")
    st.dataframe(df, use_container_width=True, height=420, key="all_sites_table")

    st.download_button(
        "📥 Download portfolio rollup CSV",
        df.to_csv(index=False).encode(),
        "portfolio_rollup.csv",
        "text/csv",
        key="all_sites_download",
    )

    st.divider()
    st.markdown("### ⚙️ Bulk actions")
    a1, a2 = st.columns(2)
    with a1:
        if st.button("🔧 Run fixer (dry-run) on all sites", use_container_width=True,
                     disabled=fix_all_sites is None):
            if fix_all_sites is None:
                st.error("`fix_issues.fix_all_sites` not available.")
            else:
                with st.spinner("Running fixer in dry-run across every site…"):
                    summary = fix_all_sites(dry_run=True)
                rows = [{"Site": d, "Attempts": len(rs),
                         "Would fix": sum(1 for r in rs if r.get("changes"))}
                        for d, rs in summary.items()]
                st.success("Dry-run complete.")
                st.dataframe(pd.DataFrame(rows), use_container_width=True)
    with a2:
        st.info("Use the per-site **Fixed Issues** tab (single-site view) to apply changes for one domain at a time.")


# ──────────────────────────────────────────────────────────────────────────
# Single-site dashboard wrapper
# (renders the original sections, all keyed off the active-site globals)
# ──────────────────────────────────────────────────────────────────────────

def _render_single_site():
    st.markdown(f"# 📊 SEO Dashboard <span class='site-pill'>{ACTIVE_SITE.brand_name}</span>", unsafe_allow_html=True)
    st.markdown(f"**Site:** [`{DOMAIN}`]({SITE_URL})  ·  **WP user:** `{WP_USER}`  ·  **GA4:** `{GA4_PROPERTY_ID or '—'}`")

    dates = get_report_dates()
    header_cols = st.columns([3, 1])
    with header_cols[1]:
        selected_date = st.selectbox("Report date", dates, index=0,
                                     key=f"date_{ACTIVE_SITE.slug}") if dates else datetime.today().strftime("%Y-%m-%d")
    if not dates:
        st.info(f"No reports yet for {DOMAIN}. Run `python crawl_script.py --site {DOMAIN}`.")

    st.caption("SEO Automation Toolkit — Streamlit Edition")
    st.divider()

    st.markdown(f"""
<div style="margin-bottom: 1rem; font-size: 0.95rem;">
<strong>Jump to:</strong>
<a href="#overview">🏠 Overview</a> &nbsp;·&nbsp;
<a href="#growth">📈 Growth</a> &nbsp;·&nbsp;
<a href="#audit">🔍 Audit</a> &nbsp;·&nbsp;
<a href="#traffic">📊 Traffic</a> &nbsp;·&nbsp;
<a href="#content">📝 Content</a> &nbsp;·&nbsp;
<a href="#keywords">🔑 Keywords</a> &nbsp;·&nbsp;
<a href="#backlinks">🔗 Backlinks</a> &nbsp;·&nbsp;
<a href="#fixed">✅ Fixed</a> &nbsp;·&nbsp;
<a href="#latest_posts">📰 Latest Posts</a>
</div>
""", unsafe_allow_html=True)
    st.divider()

    # ── 🏠 OVERVIEW ────────────────────────────────────────────────────
    st.markdown('<div id="overview" class="seo-section-anchor"></div>', unsafe_allow_html=True)
    st.markdown("## 🏠 SEO Performance Overview")
    st.markdown(f"**{DOMAIN}** — Report for **{selected_date}**")

    audit_df = load_audit(selected_date)
    serp_df = load_serp(selected_date)

    if audit_df is not None:
        total = len(audit_df)
        broken = len(audit_df[audit_df["status"].astype(str).str.match(r"^[45E]")])
        with_issues = len(audit_df[audit_df["issues"].astype(str).str.len() > 0])
        clean = total - with_issues
        avg_load = audit_df["load_time_s"].dropna().mean() if "load_time_s" in audit_df else None

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Pages Crawled", total)
        c2.metric("Clean Pages", clean, delta=f"{clean/max(total,1)*100:.0f}%")
        c3.metric("Issues Found", with_issues)
        c4.metric("Broken Pages", broken)
        c5.metric("Avg Load Time", f"{avg_load:.2f}s" if pd.notna(avg_load) else "N/A")

        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("### 📊 Issue Distribution")
            issue_labels = []
            for iss_str in audit_df["issues"].fillna(""):
                for iss in str(iss_str).split(" | "):
                    if iss.strip():
                        issue_labels.append(iss.strip())
            if issue_labels:
                issue_counts = pd.Series(issue_labels).value_counts().head(10).reset_index()
                issue_counts.columns = ["Issue", "Count"]
                fig = px.pie(issue_counts, names="Issue", values="Count", hole=0.45)
                st.plotly_chart(fig, use_container_width=True, key="overview_pie")
            else:
                st.success("No issues found.")
        with col2:
            st.markdown("### ⏱️ Load Times")
            lt = audit_df["load_time_s"].dropna() if "load_time_s" in audit_df else pd.Series(dtype=float)
            if len(lt) > 0:
                fig = px.histogram(lt, nbins=20, labels={"value": "Load Time (s)"},
                                   color_discrete_sequence=["#01696f"])
                fig.add_vline(x=3.0, line_dash="dash", line_color="#da7101")
                st.plotly_chart(fig, use_container_width=True, key="overview_load")

    if serp_df is not None and len(serp_df) > 0:
        st.divider()
        st.markdown("### 🏆 SERP Summary")
        ss = serp_df[serp_df["site"] == DOMAIN] if "site" in serp_df.columns else serp_df
        pos = pd.to_numeric(ss["our_position"], errors="coerce")
        r1, r2, r3 = st.columns(3)
        r1.metric("Top 3", int((pos <= 3).sum()))
        r2.metric("Top 10", int((pos <= 10).sum()))
        r3.metric("Not Ranked", int(pos.isna().sum()))

    st.divider()
    st.markdown("## 📊 Google Analytics (live)")
    ga_df = fetch_ga4_data()
    if ga_df is not None and len(ga_df) > 0:
        fig = px.line(ga_df, x="date", y="users", title=f"Traffic Trend — last 7 days ({DOMAIN})")
        st.plotly_chart(fig, use_container_width=True, key="overview_ga4")

        top_pages = fetch_top_pages()

        st.divider()
        st.markdown("## 🧪 Click farm results, today")
        cf_df = load_clickfarm_today()
        if cf_df is None or len(cf_df) == 0:
            st.info(f"No click-farm CSV found for today in `seo_reports/{ACTIVE_SITE.slug}/`.")
        else:
            cf_df["engine"] = cf_df["engine"].astype(str)
            cf_df["clicks"] = pd.to_numeric(cf_df["clicks"], errors="coerce").fillna(0).astype(int)
            total_clicks = int(cf_df["clicks"].sum())
            top_engine = cf_df.sort_values("clicks", ascending=False).iloc[0]
            c1, c2, c3 = st.columns(3)
            c1.metric("Total bot clicks today", f"{total_clicks:,}")
            c2.metric("Engines tested", f"{len(cf_df):,}")
            c3.metric("Top engine", f"{top_engine['engine']} ({top_engine['clicks']} clicks)")
            fig_cf = px.bar(cf_df, x="engine", y="clicks", text="clicks",
                            labels={"engine": "Engine", "clicks": "Clicks"},
                            title="Click farm results, today", color="engine")
            fig_cf.update_traces(textposition="outside")
            st.plotly_chart(fig_cf, use_container_width=True, key="overview_clickfarm")
            st.dataframe(cf_df.reset_index(drop=True), use_container_width=True, height=250,
                         key="overview_cf_table")

        st.markdown("## 🔥 Top Pages (last 7 days)")
        if top_pages is not None and len(top_pages) > 0:
            fig = px.bar(top_pages, x="page", y="views", color_discrete_sequence=["#01696f"])
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True, key="overview_top_pages")
            st.dataframe(top_pages, use_container_width=True, key="overview_top_pages_tbl")
        else:
            st.info("No GA4 top pages data available.")

    st.divider()

    # ── 📈 GROWTH ───────────────────────────────────────────────────────
    st.markdown('<div id="growth" class="seo-section-anchor"></div>', unsafe_allow_html=True)
    st.markdown("## 📈 Growth Tracker")
    all_dates = get_report_dates()
    if len(all_dates) < 2:
        st.info("Need at least 2 scans to track growth.")
    else:
        audit_hist, serp_hist = load_all_snapshots()
        col_a, col_b = st.columns(2)
        with col_a:
            date_new = st.selectbox("Compare (newer)", all_dates, index=0, key="d_new")
        with col_b:
            older_options = [d for d in all_dates if d < date_new]
            date_old = st.selectbox("vs (older)", older_options, index=0, key="d_old") if older_options else None
        if date_old is None:
            st.warning("No older scan available.")
        else:
            snap_new = compute_audit_snapshot(load_audit(date_new))
            snap_old = compute_audit_snapshot(load_audit(date_old))
            if snap_new and snap_old:
                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric("Health Score", snap_new["health_score"], delta=snap_new["health_score"] - snap_old["health_score"])
                c2.metric("Pages", snap_new["total_pages"], delta=snap_new["total_pages"] - snap_old["total_pages"])
                c3.metric("Issues", snap_new["pages_with_issues"], delta=snap_new["pages_with_issues"] - snap_old["pages_with_issues"])
                c4.metric("Broken", snap_new["broken_pages"], delta=snap_new["broken_pages"] - snap_old["broken_pages"])
                c5.metric("Avg Load", snap_new["avg_load_time"],
                          delta=(snap_new["avg_load_time"] or 0) - (snap_old["avg_load_time"] or 0))
            if not audit_hist.empty:
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=audit_hist["date"], y=audit_hist["health_score"],
                                         mode="lines+markers", name="Health Score"))
                st.plotly_chart(fig, use_container_width=True, key="growth_health")
            if not serp_hist.empty:
                fig = go.Figure()
                for col in ["top3", "top10", "top20"]:
                    if col in serp_hist.columns:
                        fig.add_trace(go.Scatter(x=serp_hist["date"], y=serp_hist[col],
                                                 mode="lines+markers", name=col))
                st.plotly_chart(fig, use_container_width=True, key="growth_serp")

    st.divider()

    # ── 🔍 AUDIT ────────────────────────────────────────────────────────
    st.markdown('<div id="audit" class="seo-section-anchor"></div>', unsafe_allow_html=True)
    st.markdown("## 🔍 Technical SEO Audit")
    if audit_df is not None:
        c1, c2, c3 = st.columns(3)
        with c1:
            filt = st.selectbox("Filter", ["All", "With Issues", "Clean"], key="audit_filt")
        with c2:
            search = st.text_input("Search URL", key="audit_search")
        with c3:
            statuses = st.multiselect("Status", sorted(audit_df["status"].astype(str).unique()), key="audit_statuses")
        df_view = audit_df.copy()
        if filt == "With Issues":
            df_view = df_view[df_view["issues"].astype(str).str.len() > 0]
        elif filt == "Clean":
            df_view = df_view[(df_view["issues"].isna()) | (df_view["issues"].astype(str).str.len() == 0)]
        if statuses:
            df_view = df_view[df_view["status"].astype(str).isin(statuses)]
        if search:
            df_view = df_view[df_view["url"].str.contains(search, case=False, na=False)]
        st.markdown(f"**{len(df_view)} of {len(audit_df)} pages**")
        cols = [c for c in ["url", "status", "load_time_s", "title_length", "meta_desc_length",
                             "h1_count", "images_missing_alt", "has_og_tags", "has_schema", "issues"]
                if c in df_view.columns]
        st.dataframe(df_view[cols], use_container_width=True, height=500, key="audit_tbl")
        st.download_button("📥 Download CSV",
                           df_view.to_csv(index=False).encode(),
                           f"{DOMAIN}_audit_{selected_date}.csv", "text/csv",
                           key="audit_dl")
    else:
        st.warning("No technical audit data found.")

    st.divider()

    # ── 📊 TRAFFIC ──────────────────────────────────────────────────────
    st.markdown('<div id="traffic" class="seo-section-anchor"></div>', unsafe_allow_html=True)
    st.markdown("## 📊 Traffic Analytics")
    days_choice = st.selectbox("Date range", [7, 14, 30, 60, 90], index=2,
                               format_func=lambda d: f"Last {d} days", key="traffic_days")
    ga_long = fetch_ga4_data(days=days_choice)
    if ga_long is not None and len(ga_long) > 0:
        fig = px.line(ga_long, x="date", y="users", title=f"Traffic trend — last {days_choice} days")
        st.plotly_chart(fig, use_container_width=True, key="traffic_line")

    src_df = fetch_traffic_by_source(days=days_choice)
    wanted_sources = ["google", "bing", "yahoo", "chatgpt", "claude", "anthropic"]
    base_df = pd.DataFrame({"source": wanted_sources})
    if src_df is not None and len(src_df) > 0:
        filtered_df = src_df[src_df["source"].astype(str).str.lower().isin(wanted_sources)].copy()
        if len(filtered_df) > 0:
            filtered_df["source"] = filtered_df["source"].astype(str).str.lower()
            filtered_df = (filtered_df.groupby("source", as_index=False)[["users", "sessions", "pageviews"]].sum())
        else:
            filtered_df = pd.DataFrame(columns=["source", "users", "sessions", "pageviews"])
        sel_df = base_df.merge(filtered_df, on="source", how="left").fillna(0)
    else:
        sel_df = base_df.copy()
        sel_df[["users", "sessions", "pageviews"]] = 0
    for col in ["users", "sessions", "pageviews"]:
        sel_df[col] = sel_df[col].astype(int)

    c1, c2, c3 = st.columns(3)
    c1.metric("Selected Source Users", f"{int(sel_df['users'].sum()):,}")
    c2.metric("Selected Source Sessions", f"{int(sel_df['sessions'].sum()):,}")
    c3.metric("Selected Source Pageviews", f"{int(sel_df['pageviews'].sum()):,}")

    fig_sources = px.bar(sel_df, x="source", y="users", text="users", color="source",
                         title="Users by source",
                         color_discrete_map={"google": "#4285F4", "bing": "#008373", "yahoo": "#6001D2",
                                             "chatgpt": "#10A37F", "claude": "#D97706", "anthropic": "#A16207"})
    fig_sources.update_traces(textposition="outside")
    st.plotly_chart(fig_sources, use_container_width=True, key="traffic_sources")
    st.dataframe(sel_df.reset_index(drop=True), use_container_width=True, height=280, key="traffic_sel_tbl")

    st.divider()

    # ── 📝 CONTENT ──────────────────────────────────────────────────────
    st.markdown('<div id="content" class="seo-section-anchor"></div>', unsafe_allow_html=True)
    st.markdown("## 📝 Content Analysis")
    kw_df = load_keywords(selected_date)
    if kw_df is not None and len(kw_df) > 0:
        c1, c2, c3 = st.columns(3)
        c1.metric("Pages", len(kw_df))
        c2.metric("Avg Words", f"{kw_df['word_count'].mean():.0f}" if "word_count" in kw_df else "N/A")
        c3.metric("Total Words", f"{kw_df['word_count'].sum():,}" if "word_count" in kw_df else "N/A")
        st.dataframe(kw_df, use_container_width=True, height=450, key="content_tbl")
    else:
        st.warning("No content analysis data found.")

    st.divider()

    # ── 🔑 KEYWORDS ─────────────────────────────────────────────────────
    st.markdown('<div id="keywords" class="seo-section-anchor"></div>', unsafe_allow_html=True)
    st.markdown("## 🔑 Keyword Clusters")
    cl = load_clusters(selected_date)
    if cl is not None and len(cl) > 0:
        fig = px.treemap(cl.head(15), path=["cluster"], values="keyword_count", color="keyword_count")
        st.plotly_chart(fig, use_container_width=True, key="kw_tree")
        st.dataframe(cl, use_container_width=True, key="kw_tbl")
    else:
        st.warning("No keyword cluster data found.")

    st.divider()

    # ── 🔗 BACKLINKS ────────────────────────────────────────────────────
    st.markdown('<div id="backlinks" class="seo-section-anchor"></div>', unsafe_allow_html=True)
    st.markdown("## 🔗 Backlink Tools")
    tab1, tab2, tab3 = st.tabs(["Unlinked Mentions", "Backlink Targets", "Internal Linking"])
    with tab1:
        brand_query = st.text_input("Brand to search", BRAND_NAME, key="bl_brand")
        if st.button("Search Mentions", use_container_width=True, key="bl_search"):
            rows = find_unlinked_mentions(query_brand=brand_query, domain=DOMAIN, count=25)
            if rows:
                st.dataframe(pd.DataFrame(rows), use_container_width=True, height=450, key="bl_mentions")
            else:
                st.info("No mention results found, or Bing API key is missing.")
    with tab2:
        if st.button("Score Targets", use_container_width=True, key="bl_score_btn"):
            rows = find_unlinked_mentions(query_brand=BRAND_NAME, domain=DOMAIN, count=25)
            scored = score_backlink_targets(rows)
            if scored:
                st.dataframe(pd.DataFrame(scored), use_container_width=True, height=450, key="bl_scored")
            else:
                st.info("No targets scored.")
    with tab3:
        if st.button("Generate Suggestions From Latest Posts", use_container_width=True, key="bl_int_btn"):
            try:
                posts = get_all_posts(status="publish", per_page=20, max_pages=2)
                categories_map = get_wp_categories_map()
                all_rows = []
                for post in posts[:10]:
                    suggestions = suggest_internal_links_for_post(post, posts, categories_map=categories_map, max_suggestions=5)
                    for s in suggestions:
                        all_rows.append({"source_post": clean_html_entities(post["title"]["rendered"]),
                                         "target_title": s["target_title"], "anchor_text": s["anchor_text"],
                                         "target_url": s["target_url"], "score": s["score"]})
                if all_rows:
                    st.dataframe(pd.DataFrame(all_rows), use_container_width=True, height=450, key="bl_int_tbl")
                else:
                    st.info("No internal link suggestions found.")
            except Exception as e:
                st.error(f"Error generating suggestions: {e}")

    st.divider()

    # ── ✅ FIXED ISSUES ─────────────────────────────────────────────────
    st.markdown('<div id="fixed" class="seo-section-anchor"></div>', unsafe_allow_html=True)
    st.markdown("## ✅ Fixed Issues (Reports)")

    fix_dates = get_fix_report_dates()
    if not fix_dates:
        st.info(f"No fix reports found for {DOMAIN}.")
    else:
        selected_fix_date = st.selectbox("Fix report date", fix_dates, index=0, key="fix_date")
        fix_df = load_fix_issues(selected_fix_date)
        if fix_df is None or len(fix_df) == 0:
            st.warning("Selected fix report is empty.")
        else:
            st.markdown(f"Showing **{len(fix_df)}** rows from `{DOMAIN}_fix_issues_{selected_fix_date}.csv`")
            col1, col2 = st.columns(2)
            with col1:
                only_fixed = st.checkbox("Show only successfully fixed", value=True, key="fix_only")
            with col2:
                url_search = st.text_input("Filter by URL contains", key="fix_search")
            df_view = fix_df.copy()
            if "fixed" in df_view.columns and only_fixed:
                df_view = df_view[df_view["fixed"] == True]
            if url_search:
                df_view = df_view[df_view["url"].astype(str).str.contains(url_search, case=False, na=False)]
            st.dataframe(df_view, use_container_width=True, height=500, key="fix_tbl")
            st.download_button("📥 Download filtered fixed-issues CSV",
                               df_view.to_csv(index=False).encode(),
                               f"{DOMAIN}_fixed_issues_view_{selected_fix_date}.csv",
                               "text/csv", key="fix_dl")

    # Run-fixer-now button
    st.markdown("### Run fixer for this site")
    rf1, rf2 = st.columns([1, 1])
    with rf1:
        run_dry = st.button("🧪 Dry-run fixer (no WP writes)", use_container_width=True,
                            disabled=fix_from_audit is None, key="fix_run_dry")
    with rf2:
        run_live = st.button("⚡ Apply fixes to WordPress", use_container_width=True,
                             disabled=fix_from_audit is None, key="fix_run_live")
    if (run_dry or run_live) and fix_from_audit is not None:
        with st.spinner(f"Running fixer for {DOMAIN}…"):
            try:
                results = fix_from_audit(ACTIVE_SITE, dry_run=run_dry, base_output=REPORTS_BASE)
                ok = sum(1 for r in results if r.get("fixed"))
                st.success(f"{'Dry-run' if run_dry else 'Live run'} complete: {ok}/{len(results)} posts updated.")
                if results:
                    st.dataframe(pd.DataFrame(results), use_container_width=True, key="fix_run_results")
            except Exception as e:
                st.error(f"Fixer error: {e}")

    st.divider()

    # ── 📰 LATEST POSTS ─────────────────────────────────────────────────
    st.markdown('<div id="latest_posts" class="seo-section-anchor"></div>', unsafe_allow_html=True)
    st.markdown("## 📰 Latest Posts")
    st.caption(f"Most recent published posts pulled live from `{DOMAIN}`.")
    c1, c2 = st.columns([1, 1])
    with c1:
        n_posts = st.slider("How many posts to fetch", 5, 60, 30, 5, key="lp_count")
    with c2:
        view_mode = st.radio("View mode", ["Cards", "Table"], horizontal=True, key="lp_view")
    try:
        with st.spinner(f"Fetching latest {n_posts} posts from {DOMAIN}…"):
            raw_posts = fetch_latest_posts(n=n_posts)
            rows = summarize_latest_posts(raw_posts)
    except Exception as e:
        st.error(f"Could not fetch posts from WordPress: {e}")
        return

    if not rows:
        st.info("No posts returned. Check that the site is publicly reachable and the WP REST API is enabled.")
        return

    posts_df = pd.DataFrame(rows)
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Posts fetched", len(posts_df))
    m2.metric("Avg word count", f"{posts_df['word_count'].mean():.0f}" if len(posts_df) else "0")
    m3.metric("Total words", f"{int(posts_df['word_count'].sum()):,}")
    m4.metric("Newest post", posts_df["published"].max() if posts_df["published"].notna().any() else "N/A")

    if view_mode == "Cards":
        cols_per_row = 3
        for i in range(0, len(posts_df), cols_per_row):
            cols = st.columns(cols_per_row)
            for j, col in enumerate(cols):
                if i + j >= len(posts_df):
                    break
                row = posts_df.iloc[i + j]
                with col:
                    if row["featured_image"]:
                        try:
                            st.image(row["featured_image"], use_container_width=True)
                        except Exception:
                            pass
                    st.markdown(f"**[{row['title']}]({row['url']})**")
                    bits = []
                    if row["published"]: bits.append(f"📅 {row['published']}")
                    if row["author"]:    bits.append(f"✍️ {row['author']}")
                    if row["word_count"]: bits.append(f"📝 {row['word_count']} words")
                    if bits: st.caption(" · ".join(bits))
                    if row["category"]: st.caption(f"🏷️ {row['category']}")
                    if row["excerpt"]: st.write(row["excerpt"])
    else:
        display_df = posts_df[["published", "title", "author", "category", "word_count", "url"]].copy()
        display_df.columns = ["Published", "Title", "Author", "Category", "Words", "URL"]
        st.dataframe(display_df, use_container_width=True, height=520,
                     column_config={"URL": st.column_config.LinkColumn("URL", display_text="Open ↗")},
                     key="lp_tbl")

    st.download_button("📥 Download latest posts CSV",
                       posts_df.to_csv(index=False).encode(),
                       f"{DOMAIN}_latest_posts.csv", "text/csv", key="lp_dl")


# ──────────────────────────────────────────────────────────────────────────
# DISPATCH
# ──────────────────────────────────────────────────────────────────────────

if view.startswith("🌐"):
    _render_all_sites()
else:
    _render_single_site()
