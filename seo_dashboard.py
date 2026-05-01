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
WP_APP_PASS = os.getenv("WP_APP_PASSWORD", "sTz9 HbAF ROBO prvo SrI2 gJb7")
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
    SOURCES_OF_INTEREST = [
        "google", "bing", "yahoo", "duckduckgo", "baidu",
        "chatgpt", "openai", "perplexity", "claude", "anthropic",
        "gemini", "copilot", "you.com"
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
                "url": url, "status": "ERROR", "load_time_s": None, "title": "",
                "title_length": 0, "meta_description": "", "meta_desc_length": 0,
                "h1_count": 0, "canonical": "", "noindex": False,
                "images_missing_alt": 0, "has_og_tags": False, "has_schema": False,
                "issues": f"Connection error: {e}"
            })
            continue

        status = resp.status_code
        content = resp.text if status == 200 else ""

        title_m = re.search(r"<title>(.*?)</title>", content, re.I | re.S)
        title_text = re.sub(r"\s+", " ", title_m.group(1).strip()) if title_m else ""

        meta_m = re.search(r'<meta\s+name="description"\s+content="(.*?)"', content, re.I) or \
                 re.search(r'<meta\s+content="(.*?)"\s+name="description"', content, re.I)
        meta_text = meta_m.group(1).strip() if meta_m else ""

        h1_count = len(re.findall(r"<h1\b", content, re.I))
        canonical_m = re.search(r'<link\s+rel="canonical"\s+href="(.*?)"', content, re.I)
        canonical_u = canonical_m.group(1).strip() if canonical_m else ""
        noindex = bool(re.search(r'content="noindex"', content, re.I))
        img_missing = len(re.findall(r"<img(?![^>]*alt=)", content, re.I))
        has_og = bool(re.search(r'property="og:', content, re.I))
        has_schema = bool(re.search(r'application/ld\+json', content, re.I))

        if status == 200:
            for link in re.findall(r'href="(.*?)"', content, re.I):
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
            issues.append(f"Slow load ({load_time}s)")
        if noindex:
            issues.append("Noindexed")
        if img_missing > 0:
            issues.append(f"{img_missing} img missing alt")
        if not has_og:
            issues.append("No OG tags")
        if not has_schema:
            issues.append("No Schema")

        results.append({
            "url": url,
            "status": status,
            "load_time_s": load_time,
            "title": title_text,
            "title_length": len(title_text),
            "meta_description": meta_text,
            "meta_desc_length": len(meta_text),
            "h1_count": h1_count,
            "canonical": canonical_u,
            "noindex": noindex,
            "images_missing_alt": img_missing,
            "has_og_tags": has_og,
            "has_schema": has_schema,
            "issues": "; ".join(issues)
        })
        time.sleep(0.25)

    return results


def extract_page_keywords(url):
    try:
        r = safe_request("get", url, headers=CRAWL_HEADERS, timeout=15)
        if not r.ok:
            return {"url": url, "error": f"HTTP {r.status_code}"}
        soup = BeautifulSoup(r.text, "html.parser")
        title = clean_html_entities(soup.title.get_text(" ", strip=True)) if soup.title else ""
        h1 = soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else ""
        h2s = " | ".join(h.get_text(" ", strip=True) for h in soup.find_all("h2")[:10])
        text = soup.get_text(" ", strip=True).lower()
        text = re.sub(r"[^a-z0-9\s]", " ", text)
        tokens = [w for w in text.split() if len(w) > 2 and w not in LLM_STOP_WORDS]
        counts = Counter(tokens)
        bigrams = Counter(f"{tokens[i]} {tokens[i+1]}" for i in range(len(tokens) - 1) if tokens[i] != tokens[i+1])
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


def classify_llm_source(source, medium, page_location, referrer):
    raw = " ".join([str(source or ""), str(medium or ""), str(page_location or ""), str(referrer or "")]).lower()
    if any(x in raw for x in ["perplexity.ai", "utm_source=perplexity", "source=perplexity", "perplexity"]):
        return "Perplexity"
    if any(x in raw for x in ["chat.openai.com", "chatgpt.com", "utm_source=chatgpt", "source=chatgpt", "openai", "chatgpt"]):
        return "ChatGPT"
    if any(x in raw for x in ["claude.ai", "anthropic.com", "utm_source=claude", "source=claude", "anthropic", "claude"]):
        return "Claude"
    return "Other LLM"


def extract_page_text_features(html, url):
    soup = BeautifulSoup(html or "", "html.parser")
    title = clean_html_entities(soup.title.get_text(" ", strip=True)) if soup.title else ""
    meta_tag = soup.find("meta", attrs={"name": re.compile("description", re.I)})
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
        "body_text": " ".join([title, meta_desc] + h1s + h2s + paragraphs[:6]).strip(),
    }


def infer_candidate_queries_from_text(text, max_queries=15):
    clean = clean_html_entities(text or "").lower()
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
            "preview": r.text[:300].strip() if r.ok else ""
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
        has_og = bool(re.search(r'property="og:', html, re.I))
        has_canonical = bool(re.search(r'rel="canonical"', html, re.I))
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
            "candidate_queries": ", ".join(candidate_queries),
            "primary_keyword": candidate_queries[0] if candidate_queries else "",
            "issues": "; ".join(issues),
        }
        row["llm_visibility_score"] = score_llm_visibility_signal({
            **row,
            "candidate_queries": candidate_queries,
            "issues": issues,
        })
        results.append(row)

        if resp.status_code == 200:
            for link in re.findall(r'href="(.*?)"', html, re.I):
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
            raw_date = row.dimension_values[0].value
            formatted_date = datetime.strptime(raw_date, "%Y%m%d").strftime("%d-%m-%Y")
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


def build_llm_ga4_filter():
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
    return FilterExpression(
        or_group=FilterExpressionList(expressions=[source_filter, referrer_filter, utm_filter])
    )


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
            dimension_filter=build_llm_ga4_filter(),
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
    summary = df.groupby("llm", as_index=False)[["sessions", "users", "views"]].sum().sort_values(
        ["sessions", "views"], ascending=False
    )
    pages = df.groupby(["llm", "page"], as_index=False)[["sessions", "users", "views"]].sum().sort_values(
        ["sessions", "views"], ascending=False
    )
    return summary, pages


# ──────────────────────────────────────────────────────────────
# SINGLE PAGE UI
# ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="SEO Dashboard | San Francisco Briefing",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');
html, body, .stApp { font-family: 'DM Sans', sans-serif; }
section.main > div { padding-top: 1.25rem; }
.block-container { padding-top: 1.5rem; padding-bottom: 3rem; max-width: 1400px; }
.section-card {
    border: 1px solid rgba(1, 105, 111, 0.12);
    border-radius: 16px;
    padding: 1rem 1rem 0.5rem 1rem;
    background: rgba(1, 105, 111, 0.03);
    margin-bottom: 1.25rem;
}
</style>
""", unsafe_allow_html=True)

st.title("📊 SEO Dashboard")
st.caption("SEO Automation Toolkit — Single Page Edition")
st.markdown(f"**Site:** `{DOMAIN}`")

dates = get_report_dates()
selected_date = st.selectbox(
    "Report Date",
    dates,
    index=0,
    key="global_selected_date",
) if dates else datetime.today().strftime("%Y-%m-%d")

if not dates:
    st.info("No reports yet. Run a scan!")

st.divider()


def section_open(title):
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.header(title)


def section_close():
    st.markdown("</div>", unsafe_allow_html=True)
    st.divider()


# Overview
section_open("Overview")
try:
    audit_df = load_audit(selected_date)
    serp_df = load_serp(selected_date)
    llm_df = load_llm_visibility(selected_date)

    snap = compute_audit_snapshot(audit_df) if audit_df is not None else None
    serp_snap = compute_serp_snapshot(serp_df) if serp_df is not None else None

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Pages Audited", snap["total_pages"] if snap else 0)
    c2.metric("Health Score", f"{snap['health_score']}%" if snap else "—")
    c3.metric("Top 10 Keywords", serp_snap["top10"] if serp_snap else 0)
    c4.metric("LLM Pages", len(llm_df) if llm_df is not None else 0)

    if audit_df is not None and len(audit_df) > 0:
        issue_counts = audit_df["issues"].fillna("").apply(lambda x: 0 if x == "" else len(str(x).split(";")))
        fig = px.histogram(
            x=issue_counts,
            nbins=min(20, max(5, issue_counts.nunique())),
            labels={"x": "Issues per page", "y": "Count"},
            title="Issue Distribution"
        )
        st.plotly_chart(fig, use_container_width=True)
except Exception as e:
    st.error(f"Overview section error: {e}")
section_close()


# Growth Tracker
section_open("Growth Tracker")
try:
    all_dates = get_report_dates()
    if len(all_dates) < 2:
        st.info("You need at least 2 scans to track growth.")
    else:
        d_new = st.selectbox("Newer scan", all_dates, index=0, key="growth_dnew")
        d_old = st.selectbox("Older scan", all_dates, index=min(1, len(all_dates)-1), key="growth_dold")

        audit_new = compute_audit_snapshot(load_audit(d_new))
        audit_old = compute_audit_snapshot(load_audit(d_old))

        if audit_new and audit_old:
            c1, c2, c3 = st.columns(3)
            c1.metric("Health Score", audit_new["health_score"], audit_new["health_score"] - audit_old["health_score"])
            c2.metric("Broken Pages", audit_new["broken_pages"], audit_old["broken_pages"] - audit_new["broken_pages"])
            c3.metric("Pages With Issues", audit_new["pages_with_issues"], audit_old["pages_with_issues"] - audit_new["pages_with_issues"])

        audit_hist, serp_hist = load_all_snapshots()
        if audit_hist is not None and len(audit_hist) > 0:
            fig = px.line(audit_hist, x="date", y="health_score", markers=True, title="SEO Health Score Over Time")
            st.plotly_chart(fig, use_container_width=True)
except Exception as e:
    st.error(f"Growth Tracker section error: {e}")
section_close()


# Technical Audit
section_open("Technical Audit")
try:
    audit_df = load_audit(selected_date)
    if audit_df is not None and len(audit_df) > 0:
        st.dataframe(audit_df, use_container_width=True)

        if "issues" in audit_df.columns:
            issues = []
            for cell in audit_df["issues"].fillna(""):
                if str(cell).strip():
                    issues.extend([x.strip() for x in str(cell).split(";") if x.strip()])
            if issues:
                issue_counts = pd.Series(issues).value_counts().head(15).reset_index()
                issue_counts.columns = ["issue", "count"]
                fig = px.bar(issue_counts, x="count", y="issue", orientation="h", title="Top Technical Issues")
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No audit data found.")
except Exception as e:
    st.error(f"Technical Audit section error: {e}")
section_close()


# SERP Rankings
section_open("SERP Rankings")
try:
    serp_df = load_serp(selected_date)
    if serp_df is not None and len(serp_df) > 0:
        ss = serp_df[serp_df["site"] == DOMAIN] if "site" in serp_df.columns else serp_df
        cd = ss[["keyword", "our_position"]].copy()
        cd["our_position"] = pd.to_numeric(cd["our_position"], errors="coerce")
        cd = cd.dropna()

        if len(cd) > 0:
            fig = px.bar(cd, x="keyword", y="our_position", color="our_position", range_color=[1, 20], title="Current Rankings")
            fig.update_layout(yaxis=dict(autorange="reversed"), xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

        st.dataframe(ss, use_container_width=True)
    else:
        st.warning("No SERP data found.")
except Exception as e:
    st.error(f"SERP Rankings section error: {e}")
section_close()


# Traffic Analytics
section_open("Traffic Analytics")
try:
    days_choice = st.selectbox("Traffic date range", [7, 14, 30, 60, 90], index=2, format_func=lambda d: f"Last {d} days")
    ga_df = fetch_ga4_data(days=days_choice)
    if ga_df is not None and len(ga_df) > 0:
        c1, c2, c3 = st.columns(3)
        c1.metric("Users", f"{ga_df['users'].sum():,}")
        c2.metric("Sessions", f"{ga_df['sessions'].sum():,}")
        c3.metric("Pageviews", f"{ga_df['pageviews'].sum():,}")

        fig = px.line(ga_df, x="date", y=["users", "sessions", "pageviews"], markers=True, title="Traffic Trend")
        st.plotly_chart(fig, use_container_width=True)

        top_pages = fetch_top_pages()
        if top_pages is not None and len(top_pages) > 0:
            st.subheader("Top Pages")
            st.dataframe(top_pages, use_container_width=True)

        source_df = fetch_traffic_by_source(days_choice)
        if source_df is not None and len(source_df) > 0:
            st.subheader("Sources")
            st.dataframe(source_df, use_container_width=True)
    else:
        st.warning("No GA4 data found.")
except Exception as e:
    st.error(f"Traffic Analytics section error: {e}")
section_close()


# Content Analysis
section_open("Content Analysis")
try:
    audit_df = load_audit(selected_date)
    if audit_df is not None and len(audit_df) > 0 and "url" in audit_df.columns:
        target_url = st.selectbox("Select page", audit_df["url"].dropna().tolist(), key="content_analysis_url")
        if target_url:
            page_kw = extract_page_keywords(target_url)
            if "error" in page_kw:
                st.warning(page_kw["error"])
            else:
                c1, c2 = st.columns(2)
                c1.metric("Word Count", page_kw.get("word_count", 0))
                c2.metric("Top Words", len(page_kw.get("top_words", [])))

                if page_kw.get("top_words"):
                    words_df = pd.DataFrame(page_kw["top_words"], columns=["word", "count"])
                    fig = px.bar(words_df, x="word", y="count", title="Top Words")
                    st.plotly_chart(fig, use_container_width=True)

                if page_kw.get("top_bigrams"):
                    st.subheader("Top Bigrams")
                    st.dataframe(pd.DataFrame(page_kw["top_bigrams"], columns=["bigram", "count"]), use_container_width=True)
    else:
        st.warning("No audit data available for content analysis.")
except Exception as e:
    st.error(f"Content Analysis section error: {e}")
section_close()


# Keywords
section_open("Keywords")
try:
    kw_df = load_keywords(selected_date)
    cl_df = load_clusters(selected_date)

    if kw_df is not None and len(kw_df) > 0:
        st.subheader("Page Keywords")
        st.dataframe(kw_df, use_container_width=True)
    else:
        st.info("No keyword page data found.")

    if cl_df is not None and len(cl_df) > 0:
        st.subheader("Keyword Clusters")
        st.dataframe(cl_df, use_container_width=True)
    else:
        st.info("No keyword cluster data found.")
except Exception as e:
    st.error(f"Keywords section error: {e}")
section_close()


# LLM Visibility
section_open("LLM Visibility")
try:
    llm_df = load_llm_visibility(selected_date)
    if llm_df is not None and len(llm_df) > 0:
        c1, c2 = st.columns(2)
        c1.metric("Pages Evaluated", len(llm_df))
        c2.metric("Average Visibility", round(pd.to_numeric(llm_df["llm_visibility_score"], errors="coerce").fillna(0).mean(), 1))

        fig = px.bar(
            llm_df.sort_values("llm_visibility_score", ascending=False).head(20),
            x="url",
            y="llm_visibility_score",
            title="Top LLM Visibility Pages"
        )
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(llm_df, use_container_width=True)
    else:
        st.info("No LLM visibility report found.")

    llm_days = st.selectbox("LLM traffic date range", [7, 14, 30, 60, 90], index=2)
    llm_traffic = fetch_llm_traffic(llm_days)
    if llm_traffic is not None and len(llm_traffic) > 0:
        summary, pages = summarize_llm_traffic(llm_traffic)
        if summary is not None:
            st.subheader("LLM Traffic Summary")
            st.dataframe(summary, use_container_width=True)
        if pages is not None:
            st.subheader("LLM Landing Pages")
            st.dataframe(pages.head(50), use_container_width=True)

    st.subheader("Parse LLM Bots From Logs")
    log_text = st.text_area("Paste server logs", height=150, key="llm_log_text")
    if st.button("Parse LLM Bots From Logs", use_container_width=True, key="parse_llm_logs_btn"):
        bots_df = detect_llm_bots_from_logs(log_text)
        if len(bots_df) > 0:
            st.dataframe(bots_df, use_container_width=True)
        else:
            st.info("No known LLM bot signatures found.")
except Exception as e:
    st.error(f"LLM Visibility section error: {e}")
section_close()


# Backlink Tools
section_open("Backlink Tools")
try:
    st.subheader("Unlinked Mentions")
    if st.button("Search Mentions", use_container_width=True, key="search_mentions_btn"):
        mentions = find_unlinked_mentions()
        if mentions:
            mentions_df = pd.DataFrame(mentions)
            st.dataframe(mentions_df, use_container_width=True)
            st.session_state["mentions_df"] = mentions_df
        else:
            st.info("No mentions found or Bing API key is missing.")

    if st.button("Score Targets", use_container_width=True, key="score_targets_btn"):
        mentions_df = st.session_state.get("mentions_df")
        if mentions_df is not None and len(mentions_df) > 0:
            scored = score_backlink_targets(mentions_df.to_dict("records"))
            st.dataframe(pd.DataFrame(scored), use_container_width=True)
        else:
            st.info("Run mention search first.")
except Exception as e:
    st.error(f"Backlink Tools section error: {e}")
section_close()


# Run New Scan
section_open("Run New Scan")
try:
    max_pages = st.slider("Max pages to crawl", 10, 300, 50, step=10, key="run_scan_max_pages")
    run_llm = st.checkbox("Also run LLM visibility scan", value=True, key="run_scan_llm")
    run_keywords = st.checkbox("Also extract keyword data", value=False, key="run_scan_keywords")

    if st.button("Start Scan", type="primary", use_container_width=True, key="run_scan_btn"):
        with st.spinner("Running technical crawl..."):
            progress = st.progress(0)
            status = st.empty()
            rows = crawl_site(SITE_URL, max_pages=max_pages, progress_bar=progress, status_text=status)
            report_date = datetime.today().strftime("%Y-%m-%d")
            audit_path = f"{OUTPUT_DIR}/{DOMAIN}_technical_audit_{report_date}.csv"
            pd.DataFrame(rows).to_csv(audit_path, index=False)
            st.success(f"Technical audit saved: {audit_path}")

        if run_keywords:
            with st.spinner("Extracting page keyword summaries..."):
                kw_rows = []
                for url in pd.DataFrame(rows)["url"].dropna().tolist():
                    kw = extract_page_keywords(url)
                    kw_rows.append({
                        "url": kw.get("url", ""),
                        "title": kw.get("title", ""),
                        "h1": kw.get("h1", ""),
                        "h2s": kw.get("h2s", ""),
                        "top_words": json.dumps(kw.get("top_words", [])),
                        "top_bigrams": json.dumps(kw.get("top_bigrams", [])),
                        "word_count": kw.get("word_count", 0),
                        "error": kw.get("error", ""),
                    })
                kw_path = f"{OUTPUT_DIR}/{DOMAIN}_page_keywords_{report_date}.csv"
                pd.DataFrame(kw_rows).to_csv(kw_path, index=False)
                st.success(f"Keyword data saved: {kw_path}")

        if run_llm:
            with st.spinner("Running LLM visibility audit..."):
                progress = st.progress(0)
                status = st.empty()
                llm_rows = audit_llm_visibility(SITE_URL, max_pages=min(max_pages, 50), progress_bar=progress, status_text=status)
                llm_path = save_llm_visibility_report(llm_rows, report_date=report_date)
                st.success(f"LLM visibility report saved: {llm_path}")
except Exception as e:
    st.error(f"Run New Scan section error: {e}")
section_close()


# Fix Issues
section_open("Fix Issues")
try:
    st.subheader("Run full SEO optimizer")
    dry_run = st.checkbox("Dry run only", value=True, key="fix_dry_run")
    min_score_to_fix = st.slider("Skip posts already above score", 0, 100, 80, key="fix_min_score")
    per_page = st.slider("Posts per page", 1, 100, 10, key="fix_per_page")
    max_pages = st.slider("Max pages of posts", 1, 20, 5, key="fix_max_pages")
    apply_schema = st.checkbox("Apply JSON-LD schema", value=True, key="fix_schema")
    apply_internal_links = st.checkbox("Apply internal linking", value=False, key="fix_internal_links")

    if st.button("Run Full SEO Optimizer", use_container_width=True, key="run_full_optimizer_btn"):
        with st.spinner("Running SEO optimizer..."):
            report = run_seo_optimizer(
                status="publish",
                per_page=per_page,
                max_pages=max_pages,
                dry_run=dry_run,
                min_score_to_fix=min_score_to_fix,
                report_file=os.path.join(OUTPUT_DIR, "seo_report.json"),
                apply_schema=apply_schema,
                apply_internal_links=apply_internal_links,
            )
        if report:
            st.success(f"Processed {len(report)} posts.")
            st.dataframe(pd.DataFrame(report), use_container_width=True)
        else:
            st.info("No posts returned.")

    st.subheader("Run fixes from latest audit CSV")
    if st.button("Run Fix Issues from Latest Audit", type="primary", key="run_fix_from_audit_btn"):
        if fix_from_audit and latest_audit_csv:
            try:
                latest_csv = latest_audit_csv()
                if latest_csv:
                    out = fix_from_audit(latest_csv)
                    st.success(f"Fix report saved: {out}")
                else:
                    st.warning("No latest audit CSV found.")
            except Exception as e:
                st.error(f"Fix run failed: {e}")
        else:
            st.warning("fix_issues.py helpers are not available.")
except Exception as e:
    st.error(f"Fix Issues section error: {e}")
section_close()


# Fixed Issues
section_open("Fixed Issues")
try:
    fix_dates = get_fix_report_dates()
    if fix_dates:
        selected_fix_date = st.selectbox("Fix report date", fix_dates, key="fixed_issues_date")
        fix_df = load_fix_issues(selected_fix_date)
        if fix_df is not None and len(fix_df) > 0:
            st.dataframe(fix_df, use_container_width=True)
        else:
            st.info("No fixed issues report for selected date.")
    else:
        st.info("No fixed issue reports found.")
except Exception as e:
    st.error(f"Fixed Issues section error: {e}")
section_close()