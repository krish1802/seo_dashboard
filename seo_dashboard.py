#!/usr/bin/env python3
"""
SEO Analytics Dashboard + WordPress SEO Auto-Optimizer
For sanfranciscobriefing.com

Features
- Technical SEO audit crawler
- WordPress SEO fixer
- JSON-LD NewsArticle schema generation/injection
- Internal linking suggestions and insertion
- LLM visibility audit
- GA4 traffic reporting
- Unlinked mention discovery
- Backlink target scoring
- Streamlit dashboard
"""

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

try:
    from fix_issues import fix_from_audit, latest_audit_csv
except Exception:
    fix_from_audit = None
    latest_audit_csv = None


# ──────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────

GA4_PROPERTY_ID = "534913592"
SITE_URL = "https://sanfranciscobriefing.com"
DOMAIN = "sanfranciscobriefing.com"
OUTPUT_DIR = "seo_reports"
os.makedirs(OUTPUT_DIR, exist_ok=True)

BRAND_NAME = "San Francisco Briefing"
SITE_DESCRIPTION = "Local San Francisco news, politics, business, neighborhoods, and events coverage."
BRAND_LOGO_URL = os.getenv("BRAND_LOGO_URL", f"{SITE_URL}/wp-content/uploads/logo.png")

TRACKED_KEYWORDS = [
    "San Francisco news",
    "San Francisco briefing",
    "SF local news",
    "Bay Area news",
    "San Francisco politics",
    "San Francisco business news",
    "San Francisco events",
    "San Francisco neighborhood news",
]

COMPETITORS = [
    "sfstandard.com",
    "sfgate.com",
    "kqed.org",
]

CRAWL_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SEO-Audit-Bot/1.0)"
}

WP_URL = os.getenv("WP_URL", SITE_URL).rstrip("/")
WP_USER = os.getenv("WP_USER", "testing")
WP_APP_PASS = os.getenv("WP_APP_PASSWORD", "").strip()
API_BASE = f"{WP_URL}/wp-json/wp/v2"

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
    "should", "may", "might", "this", "that", "these", "those", "it", "its"
}

LLM_STOP_WORDS = SLUG_STOP_WORDS | {
    "news", "briefing", "brief", "today", "update", "updates", "story", "stories",
    "read", "guide", "local", "latest", "new", "post", "posts", "page"
}

LLM_REFERRER_REGEX = r"(chat\.openai\.com|chatgpt\.com|perplexity\.ai|claude\.ai|anthropic\.com)"
LLM_SOURCE_REGEX = r"(chatgpt|openai|perplexity|claude|anthropic)"
LLM_UTM_REGEX = r".*utm_source=(chatgpt|perplexity|claude).*"

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


# ──────────────────────────────────────────────────────────────
# GA4
# ──────────────────────────────────────────────────────────────

def get_ga4_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["ga4"]
    )
    return BetaAnalyticsDataClient(credentials=credentials)


# ──────────────────────────────────────────────────────────────
# HTTP SESSION
# ──────────────────────────────────────────────────────────────

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
        raise RuntimeError("Missing WP_USER or WP_APP_PASSWORD in environment.")
    token = b64encode(f"{WP_USER}:{WP_APP_PASS}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json"
    }


# ──────────────────────────────────────────────────────────────
# TEXT HELPERS
# ──────────────────────────────────────────────────────────────

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


def generate_seo_title(raw_title, site_name=BRAND_NAME):
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
        for key in [
            "rank_math_facebook_title",
            "rank_math_facebook_description",
            "rank_math_twitter_title",
            "rank_math_twitter_description",
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

    soup = BeautifulSoup(content_html or "", "html.parser")
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
# PERFORMANCE AUDIT
# ──────────────────────────────────────────────────────────────
def load_clickfarm_today():
    today = datetime.today().strftime("%Y-%m-%d")
    path = os.path.join(OUTPUT_DIR, f"traffic_generated_{today}.csv")
    if os.path.exists(path):
        return pd.read_csv(path)
    return None

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
        r = safe_request("get", url, timeout=15, headers={"User-Agent": CRAWL_HEADERS["User-Agent"]})
        elapsed = round(time.time() - start, 3)
        perf["response_time_s"] = elapsed

        html_bytes = len(r.content)
        html_kb = round(html_bytes / 1024, 1)
        perf["html_size_kb"] = html_kb

        soup = BeautifulSoup(r.text, "html.parser")
        scripts = soup.find_all("script", src=True)
        stylesheets = soup.find_all("link", rel=lambda v: v and "stylesheet" in v)
        images_tag = soup.find_all("img", src=True)
        iframes = soup.find_all("iframe", src=True)
        request_count = len(scripts) + len(stylesheets) + len(images_tag) + len(iframes)
        perf["request_count"] = request_count

        for tag in scripts:
            src = tag.get("src", "")
            if src and ".js" in src and ".min.js" not in src and "cdn" not in src.lower():
                perf["js_unminified"].append(src.split("?")[0])

        for tag in stylesheets:
            href = tag.get("href", "")
            if href and ".css" in href and ".min.css" not in href and "cdn" not in href.lower():
                perf["css_unminified"].append(href.split("?")[0])

        if elapsed > 0.2:
            perf["perf_issues"].append(f"Response time {elapsed}s exceeds 0.2s recommendation")
        if html_kb > 50:
            perf["perf_issues"].append(f"HTML document is {html_kb} KB (recommendation: ≤ 50 KB)")
        if request_count > 20:
            perf["perf_issues"].append(f"Page makes ~{request_count} requests (recommendation: ≤ 20)")
        if perf["js_unminified"]:
            perf["perf_issues"].append(f"{len(perf['js_unminified'])} JS file(s) appear unminified")
        if perf["css_unminified"]:
            perf["perf_issues"].append(f"{len(perf['css_unminified'])} CSS file(s) appear unminified")

    except Exception as e:
        perf["perf_issues"].append(f"Performance audit error: {e}")

    return perf


# ──────────────────────────────────────────────────────────────
# WORDPRESS FETCHERS
# ──────────────────────────────────────────────────────────────

def get_all_posts(status="publish", per_page=10, max_pages=5):
    all_posts = []
    for page in range(1, max_pages + 1):
        r = safe_request(
            "get",
            f"{API_BASE}/posts",
            headers=_auth_header(),
            params={
                "status": status,
                "per_page": per_page,
                "page": page,
                "context": "edit",
                "_fields": "id,title,slug,content,excerpt,meta,link,modified,modified_gmt,date,date_gmt,author,categories"
            }
        )
        if not r.ok or not r.json():
            break
        batch = r.json()
        total_pages = int(r.headers.get("X-WP-TotalPages", 1))
        all_posts.extend(batch)
        if page >= total_pages:
            break
        time.sleep(REQUEST_DELAY)
    return all_posts


def get_post_by_slug(slug):
    r = safe_request(
        "get",
        f"{API_BASE}/posts",
        headers=_auth_header(),
        params={"slug": slug, "context": "edit"}
    )
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
            r = safe_request(
                "get",
                f"{API_BASE}/categories",
                headers=_auth_header(),
                params={"per_page": 100, "page": page, "_fields": "id,name,slug"},
                timeout=20,
            )
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


# ──────────────────────────────────────────────────────────────
# JSON-LD SCHEMA
# ──────────────────────────────────────────────────────────────

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

    article_section = [
        categories_map.get(cid, "")
        for cid in (post.get("categories", []) or [])
        if categories_map.get(cid)
    ]

    schema = {
        "@context": "https://schema.org",
        "@type": "NewsArticle",
        "mainEntityOfPage": {"@type": "WebPage", "@id": canonical},
        "headline": clean_text_snippet(title, 110),
        "description": clean_text_snippet(html_to_text(excerpt_html) or meta_desc or SITE_DESCRIPTION, 220),
        "url": canonical,
        "datePublished": date_published,
        "dateModified": date_modified,
        "author": {
            "@type": "Person" if author_name != BRAND_NAME else "Organization",
            "name": author_name
        },
        "publisher": {
            "@type": "Organization",
            "name": BRAND_NAME,
            "logo": {
                "@type": "ImageObject",
                "url": BRAND_LOGO_URL
            }
        }
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
        if any(
            str(item.get("@type", "")).lower() in {"newsarticle", "article"}
            for item in parsed_list if isinstance(item, dict)
        ):
            existing = tag
            break

    if existing is not None:
        existing.string = schema_json
    else:
        script_tag = soup.new_tag("script", type="application/ld+json")
        script_tag.string = schema_json
        soup.append(script_tag)

    return str(soup)


# ──────────────────────────────────────────────────────────────
# INTERNAL LINKING
# ──────────────────────────────────────────────────────────────

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
    return {
        "id": post.get("id"),
        "url": link,
        "slug": slug,
        "title": title,
        "keywords": extract_keywords_from_title(title),
        "tokens": tokenize_for_linking(source_text),
        "categories": cat_names,
        "content_html": post.get("content", {}).get("rendered", "")
    }


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
            suggestions.append({
                "target_id": candidate["id"],
                "target_title": candidate["title"],
                "target_url": candidate["url"],
                "anchor_text": anchor[:80],
                "score": score
            })

    suggestions = sorted(suggestions, key=lambda x: x["score"], reverse=True)
    deduped = []
    seen_urls = set()
    for s in suggestions:
        if s["target_url"] not in seen_urls:
            deduped.append(s)
            seen_urls.add(s["target_url"])
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
            if len(p_text) < 60:
                continue
            if target_url in p_html:
                continue

            anchor_regex = re.compile(rf"\b({re.escape(anchor)})\b", re.I)
            if anchor_regex.search(p_text):
                new_html = anchor_regex.sub(
                    rf'<a href="{target_url}">\1</a>',
                    p.decode_contents(),
                    count=1
                )
                p.clear()
                p.append(BeautifulSoup(new_html, "html.parser"))
                inserted += 1
                break

    return str(soup), inserted


# ──────────────────────────────────────────────────────────────
# BACKLINK DISCOVERY
# ──────────────────────────────────────────────────────────────

def find_unlinked_mentions(query_brand=BRAND_NAME, domain=DOMAIN, count=20):
    if not BING_API_KEY:
        return []

    endpoint = "https://api.bing.microsoft.com/v7.0/search"
    query = f'"{query_brand}" -site:{domain}'
    try:
        r = safe_request(
            "get",
            endpoint,
            headers={"Ocp-Apim-Subscription-Key": BING_API_KEY},
            params={"q": query, "count": count, "textDecorations": False, "textFormat": "Raw"},
            timeout=20,
        )
        if not r.ok:
            return []

        data = r.json()
        values = data.get("webPages", {}).get("value", [])
        rows = []
        for item in values:
            rows.append({
                "name": item.get("name", ""),
                "url": item.get("url", ""),
                "snippet": item.get("snippet", ""),
                "domain": urlparse(item.get("url", "")).netloc,
                "brand_mentioned": query_brand.lower() in item.get("snippet", "").lower() or query_brand.lower() in item.get("name", "").lower(),
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


# ──────────────────────────────────────────────────────────────
# WORDPRESS FIXER
# ──────────────────────────────────────────────────────────────

def apply_seo_fixes(
    post,
    all_posts=None,
    categories_map=None,
    dry_run=True,
    min_score_to_fix=80,
    apply_schema=True,
    apply_internal_links=False
):
    pid = post["id"]
    raw_title = post["title"]["rendered"]
    slug = post["slug"]
    content_html = post["content"]["rendered"]
    post_link = post.get("link", WP_URL)

    clean_title = clean_html_entities(raw_title)
    keywords = extract_keywords_from_title(clean_title)

    content_html, alt_updated, alt_count = add_alt_tags_to_images(content_html, keywords)

    meta = post.get("meta", {}) or {}
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
    internal_suggestions = []
    internal_links_inserted = 0

    if all_posts:
        internal_suggestions = suggest_internal_links_for_post(post, all_posts, categories_map=categories_map, max_suggestions=5)

    schema_obj = build_newsarticle_schema(
        post,
        meta_desc=new_meta_desc,
        author_name=get_wp_author_name(post.get("author")),
        categories_map=categories_map or {}
    )

    if apply_schema:
        content_html = upsert_json_ld_schema(content_html, schema_obj)

    if apply_internal_links and internal_suggestions:
        content_html, internal_links_inserted = insert_internal_links(content_html, internal_suggestions, max_links=3)

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
        "internal_link_suggestions": internal_suggestions,
        "internal_links_inserted": internal_links_inserted,
        "schema_generated": bool(schema_obj),
    }

    if dry_run or score_before >= min_score_to_fix:
        return result

    changes = {}

    if alt_updated or apply_schema or internal_links_inserted > 0:
        changes["content"] = content_html
        if alt_updated:
            result["changes_made"].append(f"Added ALT tags to {alt_count} image(s)")
        if apply_schema:
            result["changes_made"].append("Inserted/updated NewsArticle JSON-LD schema")
        if internal_links_inserted > 0:
            result["changes_made"].append(f"Inserted {internal_links_inserted} internal link(s)")

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
            result["changes_made"].append("Saved to WordPress")
        else:
            result["changes_made"].append(f"Save failed ({r.status_code}): {r.text[:200]}")

    return result


def run_seo_optimizer(
    status="publish",
    per_page=10,
    max_pages=10,
    dry_run=True,
    min_score_to_fix=80,
    report_file="seo_report.json",
    apply_schema=True,
    apply_internal_links=False,
):
    posts = get_all_posts(status=status, per_page=per_page, max_pages=max_pages)
    if not posts:
        return []

    categories_map = get_wp_categories_map()
    report = []

    for post in posts:
        result = apply_seo_fixes(
            post,
            all_posts=posts,
            categories_map=categories_map,
            dry_run=dry_run,
            min_score_to_fix=min_score_to_fix,
            apply_schema=apply_schema,
            apply_internal_links=apply_internal_links,
        )
        report.append(result)
        time.sleep(REQUEST_DELAY)

    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    return report


# ──────────────────────────────────────────────────────────────
# REPORT FILE HELPERS
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
def load_llm_visibility(date):
    path = f"{OUTPUT_DIR}/{DOMAIN}_llm_visibility_{date}.csv"
    return pd.read_csv(path) if os.path.exists(path) else None


def get_fix_report_dates():
    dates = set()
    if os.path.exists(OUTPUT_DIR):
        for f in os.listdir(OUTPUT_DIR):
            m = re.search(rf"{re.escape(DOMAIN)}_fix_issues_(\d{{4}}-\d{{2}}-\d{{2}})\.csv", f)
            if m:
                dates.add(m.group(1))
    return sorted(dates, reverse=True)


def load_fix_issues(date):
    path = f"{OUTPUT_DIR}/{DOMAIN}_fix_issues_{date}.csv"
    return pd.read_csv(path) if os.path.exists(path) else None


# ──────────────────────────────────────────────────────────────
# SNAPSHOT / GROWTH HELPERS
# ──────────────────────────────────────────────────────────────
def fetch_traffic_by_source(days=30):
    """Fetch sessions grouped by sessionSource + sessionMedium for search engines and LLMs."""
    SOURCES_OF_INTEREST = [
        "google", "bing", "yahoo", "duckduckgo", "baidu",
        "chatgpt", "openai", "perplexity", "claude", "anthropic",
        "gemini", "copilot", "you.com", "cloud"
    ]
    try:
        client = get_ga4_client()
        request = RunReportRequest(
            property=f"properties/{GA4_PROPERTY_ID}",
            dimensions=[
                {"name": "sessionSource"},
                {"name": "sessionMedium"},
                {"name": "sessionDefaultChannelGroup"},
            ],
            metrics=[
                {"name": "sessions"},
                {"name": "activeUsers"},
                {"name": "screenPageViews"},
                {"name": "bounceRate"},
                {"name": "averageSessionDuration"},
            ],
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

            # Classify source type
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

            rows.append({
                "source": row.dimension_values[0].value,
                "medium": row.dimension_values[1].value,
                "channel": channel,
                "source_type": source_type,
                "sessions": sessions,
                "users": users,
                "pageviews": views,
                "bounce_rate_pct": bounce,
                "avg_session_duration_s": avg_dur,
            })

        df = pd.DataFrame(rows)
        if len(df) == 0:
            return df

        # Filter to sources of interest + any organic/LLM traffic
        mask = (
            df["source"].str.lower().apply(lambda s: any(x in s for x in SOURCES_OF_INTEREST))
            | df["medium"].str.lower().isin(["organic", "cpc"])
            | (df["source_type"] == "AI / LLM")
        )
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


# ──────────────────────────────────────────────────────────────
# SITE CRAWLER
# ──────────────────────────────────────────────────────────────

def crawl_site(start_url, max_pages=100, progress_bar=None, status_text=None):
    visited, queue, results = set(), [start_url], []
    parsed = urlparse(start_url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    while queue and len(visited) < max_pages:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)

        if progress_bar:
            progress_bar.progress(min(len(visited) / max_pages, 1.0))
        if status_text:
            status_text.text(f"Crawling ({len(visited)}/{max_pages}): {url[:90]}")

        try:
            t0 = time.time()
            resp = requests.get(url, headers=CRAWL_HEADERS, timeout=15, allow_redirects=True)
            load_time = round(time.time() - t0, 2)
        except Exception as e:
            results.append({
                "url": url, "status": "ERROR", "load_time_s": None, "title": "", "title_length": 0,
                "meta_description": "", "meta_desc_length": 0, "h1_count": 0, "canonical": "",
                "noindex": False, "images_missing_alt": 0, "has_og_tags": False, "has_schema": False,
                "issues": f"Connection error: {e}"
            })
            continue

        status = resp.status_code
        content = resp.text if status == 200 else ""

        title_m = re.search(r"<title[^>]*>(.*?)</title>", content, re.I | re.S)
        title_text = re.sub(r"<[^>]+>", "", title_m.group(1)).strip() if title_m else ""
        meta_m = (
            re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']*)', content, re.I)
            or re.search(r'<meta[^>]+content=["\']([^"\']*)["\'][^>]+name=["\']description["\']', content, re.I)
        )
        meta_text = meta_m.group(1).strip() if meta_m else ""
        h1_count = len(re.findall(r"<h1[^>]*>", content, re.I))
        canonical_m = re.search(r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']([^"\']*)', content, re.I)
        canonical_u = canonical_m.group(1).strip() if canonical_m else ""
        noindex = bool(re.search(r'content=["\'][^"\']*noindex', content, re.I))
        img_missing = len(re.findall(r'<img(?![^>]*\balt\s*=)[^>]*/?>', content, re.I))
        has_og = bool(re.search(r'property=["\']og:', content, re.I))
        has_schema = bool(re.search(r'application/ld\+json', content, re.I))

        if status == 200:
            for link in re.findall(r"""href=["']([^"'#?][^"']*)["']""", content, re.I):
                full = urljoin(base, link)
                if full.startswith(base) and full not in visited and full not in queue:
                    queue.append(full)

        issues = []
        if status >= 400:
            issues.append(f"HTTP {status}")
        if not title_text:
            issues.append("Missing title")
        elif len(title_text) < 30:
            issues.append(f"Title short ({len(title_text)})")
        elif len(title_text) > 65:
            issues.append(f"Title long ({len(title_text)})")
        if not meta_text:
            issues.append("Missing meta desc")
        elif len(meta_text) < 70:
            issues.append(f"Meta short ({len(meta_text)})")
        elif len(meta_text) > 160:
            issues.append(f"Meta long ({len(meta_text)})")
        if h1_count == 0:
            issues.append("No H1")
        elif h1_count > 1:
            issues.append(f"Multiple H1s ({h1_count})")
        if load_time and load_time > 3.0:
            issues.append(f"Slow ({load_time}s)")
        if noindex:
            issues.append("Noindexed")
        if img_missing > 0:
            issues.append(f"{img_missing} img no alt")
        if not has_og:
            issues.append("No OG tags")
        if not has_schema:
            issues.append("No Schema")

        results.append({
            "url": url, "status": status, "load_time_s": load_time,
            "title": title_text, "title_length": len(title_text),
            "meta_description": meta_text, "meta_desc_length": len(meta_text),
            "h1_count": h1_count, "canonical": canonical_u, "noindex": noindex,
            "images_missing_alt": img_missing, "has_og_tags": has_og,
            "has_schema": has_schema, "issues": " | ".join(issues)
        })
        time.sleep(0.25)

    return results


# ──────────────────────────────────────────────────────────────
# CONTENT ANALYSIS
# ──────────────────────────────────────────────────────────────

def extract_page_keywords(url):
    try:
        r = safe_request("get", url, headers=CRAWL_HEADERS, timeout=15)
        if not r.ok:
            return {"url": url, "error": f"HTTP {r.status_code}"}

        soup = BeautifulSoup(r.text, "html.parser")
        title = clean_html_entities(soup.title.get_text(" ", strip=True)) if soup.title else ""
        h1 = soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else ""
        h2s = " | ".join([h.get_text(" ", strip=True) for h in soup.find_all("h2")[:10]])

        text = soup.get_text(" ", strip=True).lower()
        text = re.sub(r"[^a-z0-9\s]", " ", text)
        tokens = [w for w in text.split() if len(w) > 2 and w not in LLM_STOP_WORDS]

        counts = Counter(tokens)
        bigrams = Counter(
            f"{tokens[i]} {tokens[i+1]}"
            for i in range(len(tokens) - 1)
            if tokens[i] != tokens[i+1]
        )

        return {
            "url": url,
            "title": title,
            "h1": h1,
            "h2s": h2s,
            "top_words": counts.most_common(15),
            "top_bigrams": bigrams.most_common(10),
            "word_count": len(tokens),
        }
    except Exception as e:
        return {"url": url, "error": str(e)}


# ──────────────────────────────────────────────────────────────
# LLM VISIBILITY
# ──────────────────────────────────────────────────────────────

def classify_llm_source(source="", medium="", page_location="", referrer=""):
    raw = " | ".join([str(source or ""), str(medium or ""), str(page_location or ""), str(referrer or "")]).lower()
    if any(x in raw for x in ["perplexity.ai", "utm_source=perplexity", "source=perplexity", "perplexity"]):
        return "Perplexity"
    if any(x in raw for x in ["chat.openai.com", "chatgpt.com", "utm_source=chatgpt", "source=chatgpt", "openai", "chatgpt"]):
        return "ChatGPT"
    if any(x in raw for x in ["claude.ai", "anthropic.com", "utm_source=claude", "source=claude", "anthropic", "claude"]):
        return "Claude"
    return "Other LLM"


def extract_page_text_features(html, url=""):
    soup = BeautifulSoup(html or "", "html.parser")
    title = clean_html_entities(soup.title.get_text(" ", strip=True)) if soup.title else ""
    meta_tag = soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
    meta_desc = meta_tag.get("content", "").strip() if meta_tag else ""
    h1s = [h.get_text(" ", strip=True) for h in soup.find_all("h1")][:3]
    h2s = [h.get_text(" ", strip=True) for h in soup.find_all("h2")][:5]
    paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p") if p.get_text(" ", strip=True)]
    first_paragraph = paragraphs[0] if paragraphs else ""
    slug = url.rstrip("/").split("/")[-1] if url else ""
    return {
        "title": title,
        "meta_description": meta_desc,
        "h1": " | ".join(h1s),
        "h2": " | ".join(h2s),
        "first_paragraph": first_paragraph,
        "slug": slug,
        "body_text": " ".join(([title, meta_desc] + h1s + h2s + paragraphs[:6])).strip(),
    }


def infer_candidate_queries_from_text(text, max_queries=15):
    clean = clean_html_entities((text or "").lower())
    clean = re.sub(r"[^a-z0-9\s]", " ", clean)
    words = [w for w in clean.split() if w and w not in LLM_STOP_WORDS and len(w) > 2]
    if not words:
        return []

    freq = defaultdict(int)
    for w in words:
        freq[w] += 1

    bigrams = []
    for i in range(len(words) - 1):
        a, b = words[i], words[i + 1]
        if a != b:
            bigrams.append(f"{a} {b}")

    seen = set()
    ranked = []

    for phrase in sorted(freq, key=lambda x: (-freq[x], len(x))):
        if phrase not in seen:
            ranked.append(phrase)
            seen.add(phrase)
        if len(ranked) >= 5:
            break

    for phrase in bigrams:
        if phrase not in seen:
            ranked.append(phrase)
            seen.add(phrase)
        if len(ranked) >= 10:
            break

    prompts = []
    for phrase in ranked:
        prompts.extend([phrase, f"what is {phrase}", f"{phrase} explained"])
        if len(prompts) >= max_queries:
            break

    deduped = []
    seen = set()
    for p in prompts:
        p = p.strip()
        if p and p not in seen:
            deduped.append(p)
            seen.add(p)
        if len(deduped) >= max_queries:
            break
    return deduped


def score_llm_visibility_signal(record):
    score = 0
    if record.get("llms_txt_present"):
        score += 15
    if record.get("has_schema"):
        score += 20
    if record.get("has_og_tags"):
        score += 10
    if record.get("has_canonical"):
        score += 10
    if record.get("meta_description"):
        score += 10
    if record.get("h1"):
        score += 10
    if record.get("first_paragraph"):
        score += 10
    if len(record.get("candidate_queries", [])) >= 5:
        score += 10
    if record.get("title") and 30 <= len(record.get("title", "")) <= 65:
        score += 5
    if len(record.get("issues", [])) == 0:
        score += 10
    return min(100, score)


def audit_llms_txt(site_url=SITE_URL):
    target = urljoin(site_url.rstrip("/") + "/", "llms.txt")
    try:
        r = safe_request("get", target, timeout=10, headers=CRAWL_HEADERS, max_attempts=1)
        return {
            "url": target,
            "present": bool(r.ok and r.text and r.text.strip()),
            "status": r.status_code,
            "preview": (r.text[:300].strip() if r.ok else "")
        }
    except Exception as e:
        return {"url": target, "present": False, "status": "ERROR", "preview": str(e)}


def audit_llm_visibility(start_url, max_pages=25, progress_bar=None, status_text=None):
    visited, queue, results = set(), [start_url], []
    parsed = urlparse(start_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    llms_txt = audit_llms_txt(start_url)

    while queue and len(visited) < max_pages:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)

        if progress_bar:
            progress_bar.progress(min(len(visited) / max_pages, 1.0))
        if status_text:
            status_text.text(f"LLM audit ({len(visited)}/{max_pages}): {url[:90]}")

        try:
            t0 = time.time()
            resp = safe_request("get", url, timeout=15, headers=CRAWL_HEADERS)
            load_time = round(time.time() - t0, 2)
            html = resp.text if resp.ok else ""
        except Exception as e:
            results.append({
                "url": url,
                "status": "ERROR",
                "load_time_s": None,
                "llms_txt_present": llms_txt["present"],
                "title": "",
                "meta_description": "",
                "h1": "",
                "h2": "",
                "first_paragraph": "",
                "slug": url.rstrip("/").split("/")[-1],
                "has_schema": False,
                "has_og_tags": False,
                "has_canonical": False,
                "candidate_queries": "",
                "primary_keyword": "",
                "issues": f"Connection error: {e}",
                "llm_visibility_score": 0,
            })
            continue

        features = extract_page_text_features(html, url)
        has_schema = bool(re.search(r'application/ld\+json', html, re.I))
        has_og = bool(re.search(r'property=["\']og:', html, re.I))
        has_canonical = bool(re.search(r'rel=["\']canonical["\']', html, re.I))
        candidate_queries = infer_candidate_queries_from_text(
            " ".join([
                features.get("title", ""),
                features.get("h1", ""),
                features.get("h2", ""),
                features.get("meta_description", ""),
                features.get("first_paragraph", ""),
                features.get("slug", "").replace("-", " "),
            ])
        )

        issues = []
        if not llms_txt["present"]:
            issues.append("Missing llms.txt")
        if not has_schema:
            issues.append("Missing schema")
        if not has_og:
            issues.append("Missing OG tags")
        if not has_canonical:
            issues.append("Missing canonical")
        if not features.get("meta_description"):
            issues.append("Missing meta description")
        if not features.get("h1"):
            issues.append("Missing H1")
        if not features.get("first_paragraph"):
            issues.append("Missing intro paragraph")
        if len(candidate_queries) < 3:
            issues.append("Weak keyword/query signals")

        row = {
            "url": url,
            "status": resp.status_code,
            "load_time_s": load_time,
            "llms_txt_present": llms_txt["present"],
            "title": features.get("title", ""),
            "meta_description": features.get("meta_description", ""),
            "h1": features.get("h1", ""),
            "h2": features.get("h2", ""),
            "first_paragraph": features.get("first_paragraph", ""),
            "slug": features.get("slug", ""),
            "has_schema": has_schema,
            "has_og_tags": has_og,
            "has_canonical": has_canonical,
            "candidate_queries": " | ".join(candidate_queries),
            "primary_keyword": candidate_queries[0] if candidate_queries else "",
            "issues": " | ".join(issues),
        }
        row["llm_visibility_score"] = score_llm_visibility_signal({
            **row,
            "candidate_queries": candidate_queries,
            "issues": issues,
        })
        results.append(row)

        if resp.status_code == 200:
            for link in re.findall(r"""href=["']([^"'#?][^"']*)["']""", html, re.I):
                full = urljoin(base, link)
                if full.startswith(base) and full not in visited and full not in queue:
                    queue.append(full)

        time.sleep(0.25)

    return results


def detect_llm_bots_from_logs(log_text):
    rows = []
    if not log_text:
        return pd.DataFrame(rows)

    lines = [line.strip() for line in str(log_text).splitlines() if line.strip()]
    for line in lines:
        lower = line.lower()
        matched = None
        for bot_name, signatures in LLM_BOT_SIGNATURES.items():
            if any(sig in lower for sig in signatures):
                matched = bot_name
                break
        if matched:
            rows.append({"bot": matched, "line": line[:500]})
    return pd.DataFrame(rows)


def save_llm_visibility_report(rows, report_date=None):
    report_date = report_date or datetime.today().strftime("%Y-%m-%d")
    path = f"{OUTPUT_DIR}/{DOMAIN}_llm_visibility_{report_date}.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


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
            raw_date = row.dimension_values[0].value  # format: YYYYMMDD
            formatted_date = datetime.strptime(raw_date, "%Y%m%d").strftime("%d/%m/%Y")
            data.append({
                "date": formatted_date,
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


def _build_llm_ga4_filter():
    source_filter = FilterExpression(
        filter=Filter(
            field_name="sessionSource",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.FULL_REGEXP,
                value=LLM_SOURCE_REGEX,
            ),
        )
    )
    referrer_filter = FilterExpression(
        filter=Filter(
            field_name="pageReferrer",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.FULL_REGEXP,
                value=r".*" + LLM_REFERRER_REGEX + r".*",
            ),
        )
    )
    utm_filter = FilterExpression(
        filter=Filter(
            field_name="fullPageUrl",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.FULL_REGEXP,
                value=LLM_UTM_REGEX,
            ),
        )
    )
    return FilterExpression(or_group=FilterExpressionList(expressions=[source_filter, referrer_filter, utm_filter]))


def fetch_llm_traffic(days=7):
    try:
        client = get_ga4_client()
        request = RunReportRequest(
            property=f"properties/{GA4_PROPERTY_ID}",
            dimensions=[
                {"name": "sessionSource"},
                {"name": "sessionMedium"},
                {"name": "pagePath"},
                {"name": "pageReferrer"},
                {"name": "fullPageUrl"},
            ],
            metrics=[
                {"name": "sessions"},
                {"name": "activeUsers"},
                {"name": "screenPageViews"},
            ],
            date_ranges=[{"start_date": f"{days}daysAgo", "end_date": "today"}],
            dimension_filter=_build_llm_ga4_filter(),
            limit=1000,
        )
        response = client.run_report(request)
        rows = []
        for row in response.rows:
            source = row.dimension_values[0].value
            medium = row.dimension_values[1].value
            page_path = row.dimension_values[2].value
            referrer = row.dimension_values[3].value
            full_url = row.dimension_values[4].value
            rows.append({
                "llm": classify_llm_source(source, medium, full_url, referrer),
                "source": source,
                "medium": medium,
                "page": page_path,
                "referrer": referrer,
                "landing_url": full_url,
                "sessions": int(row.metric_values[0].value),
                "users": int(row.metric_values[1].value),
                "views": int(row.metric_values[2].value),
            })
        return pd.DataFrame(rows)
    except Exception as e:
        st.error(f"LLM Traffic Error: {e}")
        return None


def summarize_llm_traffic(df):
    if df is None or len(df) == 0:
        return None, None
    summary = (
        df.groupby("llm", as_index=False)[["sessions", "users", "views"]]
          .sum()
          .sort_values(["sessions", "views"], ascending=False)
    )
    pages = (
        df.groupby(["llm", "page"], as_index=False)[["sessions", "users", "views"]]
          .sum()
          .sort_values(["sessions", "views"], ascending=False)
    )
    return summary, pages

# ──────────────────────────────────────────────────────────────
# STREAMLIT UI — ONE PAGE REPORT
# ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="SEO One-Page Report | San Francisco Briefing",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');
    html, body, .stApp { font-family: 'DM Sans', sans-serif; }
    .main .block-container { padding-top: 1.5rem; max-width: 1320px; }
    .report-date {
        color: #64748b;
        font-size: 0.95rem;
        font-weight: 600;
        margin-bottom: 0.3rem;
    }
    .hero {
        padding: 1.1rem 1.25rem;
        border: 1px solid #e2e8f0;
        border-radius: 18px;
        background: linear-gradient(135deg, #f8fafc 0%, #eefdfa 100%);
        margin-bottom: 1.2rem;
    }
    .hero h1 { margin: 0; line-height: 1.15; }
    .hero p { color: #475569; margin-top: 0.5rem; margin-bottom: 0; }
    .section-card {
        border: 1px solid #e2e8f0;
        border-radius: 16px;
        padding: 1rem;
        background: #ffffff;
        margin-bottom: 1rem;
    }
    .small-note {
        color: #64748b;
        font-size: 0.9rem;
    }
    div[data-testid="stMetric"] {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        padding: 0.8rem;
        border-radius: 14px;
    }
</style>
""", unsafe_allow_html=True)


def _safe_df(df):
    return df is not None and hasattr(df, "__len__") and len(df) > 0


def _show_download(df, label, filename):
    if _safe_df(df):
        st.download_button(
            label,
            df.to_csv(index=False).encode(),
            filename,
            "text/csv",
            use_container_width=True,
        )


def build_required_source_traffic(source_df):
    """Always return Yahoo, Bing, ChatGPT, Anthropic, and Cloud traffic rows.

    If GA4 has no matching rows for a source, that source is shown with zero traffic.
    """
    required_sources = [
        {
            "traffic_source": "Yahoo",
            "match_terms": ["yahoo"],
        },
        {
            "traffic_source": "Bing",
            "match_terms": ["bing"],
        },
        {
            "traffic_source": "ChatGPT",
            "match_terms": ["chatgpt", "openai"],
        },
        {
            "traffic_source": "Anthropic",
            "match_terms": ["anthropic", "claude"],
        },
        {
            "traffic_source": "Cloud",
            "match_terms": ["cloud"],
        },
    ]

    rows = []
    if source_df is None or len(source_df) == 0:
        source_df = pd.DataFrame(columns=["source", "sessions", "users", "pageviews"])

    source_copy = source_df.copy()
    if "source" not in source_copy.columns:
        source_copy["source"] = ""
    for metric in ["sessions", "users", "pageviews"]:
        if metric not in source_copy.columns:
            source_copy[metric] = 0
        source_copy[metric] = pd.to_numeric(source_copy[metric], errors="coerce").fillna(0).astype(int)

    source_copy["_source_match"] = source_copy["source"].astype(str).str.lower()

    for item in required_sources:
        mask = source_copy["_source_match"].apply(
            lambda value: any(term in value for term in item["match_terms"])
        )
        matched = source_copy[mask]
        rows.append({
            "Traffic Source": item["traffic_source"],
            "Sessions": int(matched["sessions"].sum()) if len(matched) else 0,
            "Users": int(matched["users"].sum()) if len(matched) else 0,
            "Pageviews": int(matched["pageviews"].sum()) if len(matched) else 0,
        })

    return pd.DataFrame(rows)


dates = get_report_dates()
selected_date = dates[0] if dates else datetime.today().strftime("%Y-%m-%d")

with st.sidebar:
    st.markdown("## Report Controls")
    st.markdown(f"**Site:** `{DOMAIN}`")
    if dates:
        selected_date = st.selectbox("Report Date", dates, index=0)
    else:
        st.info("No saved reports found. Use Run New Scan below to generate reports.")
    st.divider()
    st.caption("One-page SEO dashboard")

st.markdown(f'<div class="report-date">Report Date: {selected_date}</div>', unsafe_allow_html=True)
st.markdown(f"""
<div class="hero">
    <h1>SEO One-Page Report</h1>
    <p><strong>{DOMAIN}</strong> — technical health, rankings, traffic, content, LLM visibility, backlinks, and fixes in one continuous dashboard.</p>
</div>
""", unsafe_allow_html=True)

audit_df = load_audit(selected_date)
serp_df = load_serp(selected_date)
keywords_df = load_keywords(selected_date)
clusters_df = load_clusters(selected_date)
llm_df = load_llm_visibility(selected_date)

st.markdown("## Executive Snapshot")

if _safe_df(audit_df):
    total_pages = len(audit_df)
    broken_pages = len(audit_df[audit_df["status"].astype(str).str.match(r"^[45E]")]) if "status" in audit_df else 0
    pages_with_issues = len(audit_df[audit_df["issues"].astype(str).str.len() > 0]) if "issues" in audit_df else 0
    clean_pages = total_pages - pages_with_issues
    avg_load = audit_df["load_time_s"].dropna().mean() if "load_time_s" in audit_df else None
else:
    total_pages = broken_pages = pages_with_issues = clean_pages = 0
    avg_load = None

if _safe_df(serp_df) and "our_position" in serp_df:
    serp_site_df = serp_df[serp_df["site"] == DOMAIN] if "site" in serp_df else serp_df
    pos = pd.to_numeric(serp_site_df["our_position"], errors="coerce")
    top_3 = int((pos <= 3).sum())
    top_10 = int((pos <= 10).sum())
    not_ranked = int(pos.isna().sum())
else:
    top_3 = top_10 = not_ranked = 0

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Pages Crawled", f"{total_pages:,}")
c2.metric("Clean Pages", f"{clean_pages:,}")
c3.metric("Issues Found", f"{pages_with_issues:,}")
c4.metric("Broken Pages", f"{broken_pages:,}")
c5.metric("Avg Load Time", f"{avg_load:.2f}s" if pd.notna(avg_load) else "N/A")
c6.metric("Top 10 Keywords", f"{top_10:,}")

st.divider()

left, right = st.columns(2)

with left:
    st.markdown("## Technical SEO")
    if _safe_df(audit_df):
        if "issues" in audit_df:
            issue_labels = []
            for issue_string in audit_df["issues"].fillna(""):
                for issue in str(issue_string).split(" | "):
                    if issue.strip():
                        issue_labels.append(issue.strip())

            if issue_labels:
                issue_counts = pd.Series(issue_labels).value_counts().head(10).reset_index()
                issue_counts.columns = ["Issue", "Count"]
                fig = px.pie(issue_counts, names="Issue", values="Count", hole=0.45, title="Top Issue Distribution")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.success("No technical issues found in the selected audit.")

        if "load_time_s" in audit_df:
            load_times = audit_df["load_time_s"].dropna()
            if len(load_times) > 0:
                fig = px.histogram(
                    load_times,
                    nbins=20,
                    labels={"value": "Load Time (s)"},
                    color_discrete_sequence=["#01696f"],
                    title="Page Load Time Distribution",
                )
                fig.add_vline(x=3.0, line_dash="dash", line_color="#da7101")
                st.plotly_chart(fig, use_container_width=True)

        st.dataframe(audit_df.head(25), use_container_width=True, height=320)
        _show_download(audit_df, "Download Technical Audit CSV", f"{DOMAIN}_technical_audit_{selected_date}.csv")
    else:
        st.info("No technical audit data is available for this report date.")

with right:
    st.markdown("## SERP Rankings")
    if _safe_df(serp_df):
        c1, c2, c3 = st.columns(3)
        c1.metric("Top 3", f"{top_3:,}")
        c2.metric("Top 10", f"{top_10:,}")
        c3.metric("Not Ranked", f"{not_ranked:,}")

        if "keyword" in serp_df and "our_position" in serp_df:
            chart_df = serp_df.copy()
            chart_df["our_position"] = pd.to_numeric(chart_df["our_position"], errors="coerce")
            chart_df = chart_df.dropna(subset=["our_position"]).sort_values("our_position").head(20)
            if len(chart_df) > 0:
                fig = px.bar(
                    chart_df,
                    x="keyword",
                    y="our_position",
                    color_discrete_sequence=["#01696f"],
                    title="Best Ranking Keywords",
                )
                fig.update_layout(xaxis_tickangle=-45, yaxis_autorange="reversed")
                st.plotly_chart(fig, use_container_width=True)

        st.dataframe(serp_df.head(25), use_container_width=True, height=320)
        _show_download(serp_df, "Download SERP CSV", f"serp_tracking_{selected_date}.csv")
    else:
        st.info("No SERP ranking data is available for this report date.")

st.divider()

st.markdown("## Traffic Analytics")
ga_df = fetch_ga4_data()
traffic_col, top_col = st.columns(2)

with traffic_col:
    if _safe_df(ga_df):
        fig = px.line(ga_df, x="date", y=["users", "sessions", "pageviews"], title="Traffic Trend, Last 7 Days")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(ga_df, use_container_width=True, height=260)
    else:
        st.info("No GA4 traffic data is available. Check GA4 credentials and property access.")

with top_col:
    top_pages = fetch_top_pages()
    if _safe_df(top_pages):
        fig = px.bar(top_pages, x="page", y="views", color_discrete_sequence=["#01696f"], title="Top Pages, Last 7 Days")
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(top_pages, use_container_width=True, height=260)
    else:
        st.info("No GA4 top-page data is available.")

st.markdown("### Required Source Traffic")
source_df = fetch_traffic_by_source(days=30)
required_source_df = build_required_source_traffic(source_df)
src_cols = st.columns(5)
for idx, row in required_source_df.iterrows():
    src_cols[idx].metric(
        row["Traffic Source"],
        f'{int(row["Sessions"]):,} sessions',
        delta=f'{int(row["Users"]):,} users',
    )

st.dataframe(required_source_df, use_container_width=True, height=220)
st.caption("Yahoo, Bing, ChatGPT, Anthropic, and Cloud are always shown here. Missing GA4 traffic is displayed as zero.")

st.divider()

st.markdown("## Click Farm Results Today")
clickfarm_df = load_clickfarm_today()

if clickfarm_df is None or len(clickfarm_df) == 0:
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Bot Clicks Today", "0")
    c2.metric("Engines Tested", "0")
    c3.metric("Top Engine", "N/A")
    st.info("No click farm CSV found for today in seo_reports/.")
else:
    clickfarm_df = clickfarm_df.copy()
    if "engine" not in clickfarm_df.columns:
        clickfarm_df["engine"] = "Unknown"
    if "clicks" not in clickfarm_df.columns:
        clickfarm_df["clicks"] = 0

    clickfarm_df["engine"] = clickfarm_df["engine"].astype(str)
    clickfarm_df["clicks"] = pd.to_numeric(clickfarm_df["clicks"], errors="coerce").fillna(0).astype(int)

    total_clicks = int(clickfarm_df["clicks"].sum())
    engines_tested = int(len(clickfarm_df))
    top_engine = clickfarm_df.sort_values("clicks", ascending=False).iloc[0]

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Bot Clicks Today", f"{total_clicks:,}")
    c2.metric("Engines Tested", f"{engines_tested:,}")
    c3.metric("Top Engine", f"{top_engine['engine']} ({int(top_engine['clicks']):,} clicks)")

    fig_cf = px.bar(
        clickfarm_df,
        x="engine",
        y="clicks",
        text="clicks",
        labels={"engine": "Engine", "clicks": "Clicks"},
        title="Click Farm Results Today",
        color="engine",
    )
    fig_cf.update_traces(textposition="outside")
    fig_cf.update_layout(xaxis_title="Engine", yaxis_title="Clicks")
    st.plotly_chart(fig_cf, use_container_width=True)

    st.dataframe(clickfarm_df.reset_index(drop=True), use_container_width=True, height=260)
    _show_download(clickfarm_df, "Download Click Farm CSV", f"{DOMAIN}_clickfarm_today_{selected_date}.csv")

st.divider()

st.markdown("## Content and Keywords")
content_col, keyword_col = st.columns(2)

with content_col:
    if _safe_df(keywords_df):
        st.markdown("### Page Keywords")
        st.dataframe(keywords_df.head(30), use_container_width=True, height=360)
        _show_download(keywords_df, "Download Page Keywords CSV", f"{DOMAIN}_page_keywords_{selected_date}.csv")
    else:
        st.info("No page keyword data is available for this report date.")

with keyword_col:
    if _safe_df(clusters_df):
        st.markdown("### Keyword Clusters")
        st.dataframe(clusters_df.head(30), use_container_width=True, height=360)
        _show_download(clusters_df, "Download Keyword Clusters CSV", f"{DOMAIN}_keyword_clusters_{selected_date}.csv")
    else:
        st.info("No keyword cluster data is available for this report date.")

st.divider()

st.markdown("## LLM Visibility")
llm_left, llm_right = st.columns([2, 1])

with llm_left:
    if _safe_df(llm_df):
        numeric_cols = [col for col in ["llm_visibility_score", "answerability_score", "entity_score"] if col in llm_df.columns]
        if numeric_cols:
            score_df = llm_df.copy()
            for col in numeric_cols:
                score_df[col] = pd.to_numeric(score_df[col], errors="coerce")
            plot_df = score_df.head(20)
            fig = px.bar(plot_df, x=plot_df.index.astype(str), y=numeric_cols, title="LLM Visibility Signals")
            st.plotly_chart(fig, use_container_width=True)

        st.dataframe(llm_df.head(30), use_container_width=True, height=360)
        _show_download(llm_df, "Download LLM Visibility CSV", f"{DOMAIN}_llm_visibility_{selected_date}.csv")
    else:
        st.info("No LLM visibility report is available for this report date.")

with llm_right:
    st.markdown("### Live LLM Traffic")
    llm_traffic = fetch_llm_traffic()
    llm_summary = summarize_llm_traffic(llm_traffic)
    if llm_summary:
        st.metric("LLM Users", f"{llm_summary.get('users', 0):,}")
        st.metric("LLM Sessions", f"{llm_summary.get('sessions', 0):,}")
        st.metric("LLM Pageviews", f"{llm_summary.get('pageviews', 0):,}")
    else:
        st.info("No LLM referral traffic detected or GA4 data unavailable.")

st.divider()

st.markdown("## Backlink Opportunities")
backlink_query = st.text_input("Brand mention search query", value=BRAND_NAME)
if st.button("Find Unlinked Mentions", use_container_width=True):
    with st.spinner("Searching for unlinked mentions..."):
        mentions = find_unlinked_mentions(backlink_query, DOMAIN)
        if mentions:
            backlink_df = pd.DataFrame(score_backlink_targets(mentions))
            st.dataframe(backlink_df, use_container_width=True, height=360)
            _show_download(backlink_df, "Download Backlink Targets CSV", f"{DOMAIN}_backlink_targets_{selected_date}.csv")
        else:
            st.info("No unlinked mentions found or Bing API key is missing.")

st.divider()

st.markdown("## Run New Scan")
scan_col1, scan_col2, scan_col3 = st.columns(3)
with scan_col1:
    scan_pages = st.slider("Max pages to crawl", 10, 500, 100, 10)
with scan_col2:
    scan_llm_pages = st.slider("Max pages for LLM audit", 5, 100, 25, 5)
with scan_col3:
    run_llm_scan = st.checkbox("Include LLM visibility scan", value=True)

if st.button("Run One-Page SEO Scan", type="primary", use_container_width=True):
    progress = st.progress(0)
    status = st.empty()
    report_date = datetime.today().strftime("%Y-%m-%d")

    with st.spinner("Running technical audit..."):
        audit_rows = crawl_site(SITE_URL, max_pages=scan_pages, progress_bar=progress, status_text=status)
        if audit_rows:
            audit_out = pd.DataFrame(audit_rows)
            audit_path = os.path.join(OUTPUT_DIR, f"{DOMAIN}_technical_audit_{report_date}.csv")
            audit_out.to_csv(audit_path, index=False)
            st.success(f"Technical audit saved: {audit_path}")
            st.dataframe(audit_out, use_container_width=True, height=300)

    if run_llm_scan:
        with st.spinner("Running LLM visibility audit..."):
            llm_rows = audit_llm_visibility(SITE_URL, max_pages=scan_llm_pages, progress_bar=progress, status_text=status)
            if llm_rows:
                llm_path = save_llm_visibility_report(llm_rows, report_date)
                st.success(f"LLM visibility report saved: {llm_path}")
                st.dataframe(pd.DataFrame(llm_rows), use_container_width=True, height=300)

    progress.progress(100)
    status.success("Scan complete. Refresh the app to select the new report date.")

st.divider()

st.markdown("## Fix Issues")
fix_tab1, fix_tab2, fix_tab3 = st.tabs(["CSV-driven Fix", "Full WordPress Optimizer", "Fixed Issues"])

with fix_tab1:
    st.markdown("### CSV-driven Fix from Latest Technical Audit")
    dry_run_csv = st.checkbox("Dry run for CSV-driven fix", value=True)
    if st.button("Run Fix Issues from Latest Audit", use_container_width=True):
        if fix_from_audit is None:
            st.error("fix_issues.py is not available.")
        elif not WP_APP_PASS:
            st.error("WP_APP_PASSWORD is not set. Add it as an environment variable before running write actions.")
        else:
            with st.spinner("Running fix_from_audit..."):
                try:
                    results = fix_from_audit(dry_run=dry_run_csv)
                    if results:
                        fix_df = pd.DataFrame(results)
                        st.dataframe(fix_df, use_container_width=True, height=420)
                        _show_download(fix_df, "Download Fix Report CSV", "fix_issues_from_audit.csv")
                    else:
                        st.info("No results returned.")
                except Exception as exc:
                    st.error(f"Error while running CSV-driven fixer: {exc}")

with fix_tab2:
    st.markdown("### Full WordPress Optimizer")
    dry_run = st.checkbox("Dry run for full optimizer", value=True)
    min_score = st.slider("Minimum SEO score to fix", 0, 100, 80, 5)
    max_pages = st.slider("Max WordPress post pages to fetch", 1, 50, 10, 1)
    per_page = st.slider("Posts per page", 5, 50, 10, 5)
    apply_schema = st.checkbox("Insert/update JSON-LD schema", value=True)
    apply_internal_links = st.checkbox("Insert internal links", value=False)
    report_path = st.text_input("Optimizer report file", "seo_report.json")

    if st.button("Run Full SEO Optimizer", use_container_width=True):
        if not WP_APP_PASS:
            st.error("WP_APP_PASSWORD is not set. Add it as an environment variable before running WordPress optimizer actions.")
        else:
            with st.spinner("Running WordPress SEO optimizer..."):
                try:
                    results = run_seo_optimizer(
                        status="publish",
                        per_page=per_page,
                        max_pages=max_pages,
                        dry_run=dry_run,
                        min_score_to_fix=min_score,
                        report_file=report_path,
                        apply_schema=apply_schema,
                        apply_internal_links=apply_internal_links,
                    )

                    if results:
                        optimizer_df = pd.DataFrame([{
                            "id": row["id"],
                            "title": row["title"],
                            "score_before": row["score_before"],
                            "score_after": row["score_after"],
                            "alt_tags_added": row.get("alt_tags_added", 0),
                            "internal_links_inserted": row.get("internal_links_inserted", 0),
                            "schema_generated": row.get("schema_generated", False),
                            "changes": ", ".join(row.get("changes_made", [])),
                        } for row in results])

                        st.success("Optimizer completed.")
                        st.dataframe(optimizer_df, use_container_width=True, height=420)
                        _show_download(optimizer_df, "Download Optimizer Report CSV", "wp_optimizer_report.csv")
                    else:
                        st.info("No posts were processed.")
                except Exception as exc:
                    st.error(f"Error while running optimizer: {exc}")

with fix_tab3:
    st.markdown("### Fixed Issue Reports")
    fix_dates = get_fix_report_dates()
    if not fix_dates:
        st.info("No fix reports found in seo_reports/.")
    else:
        selected_fix_date = st.selectbox("Fix report date", fix_dates, index=0)
        fix_df = load_fix_issues(selected_fix_date)

        if not _safe_df(fix_df):
            st.warning("Selected fix report is empty.")
        else:
            only_fixed = st.checkbox("Show only successfully fixed", value=True)
            url_search = st.text_input("Filter fixed report by URL contains")

            df_view = fix_df.copy()
            if "fixed" in df_view.columns and only_fixed:
                df_view = df_view[df_view["fixed"] == True]
            if url_search:
                df_view = df_view[df_view["url"].astype(str).str.contains(url_search, case=False, na=False)]

            st.dataframe(df_view, use_container_width=True, height=420)
            _show_download(df_view, "Download Filtered Fixed Issues CSV", f"{DOMAIN}_fixed_issues_view_{selected_fix_date}.csv")
