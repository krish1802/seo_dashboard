#!/usr/bin/env python3
"""
Fix issues based on latest technical audit CSV.

- Reads latest seo_reports/<DOMAIN>_technical_audit_YYYY-MM-DD.csv
- For each URL with issues and status=200:
    * Resolves WP post via slug
    * Fixes:
        - SEO title (Yoast + Rank Math meta)
        - Meta description (Yoast + Rank Math meta)
        - Missing image alt attributes
        - Slug cleanup (shorter, keyword-based)
- Writes CSV + JSON report in seo_reports/
"""

import os
import re
import time
import glob
import json
import html
from datetime import datetime
from base64 import b64encode
from urllib.parse import urlparse

import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── CONFIG ──────────────────────────────────────────────────────────────

DOMAIN     = "aifrontierdispatch.com"
OUTPUT_DIR = "seo_reports"
os.makedirs(OUTPUT_DIR, exist_ok=True)

WP_URL       = os.getenv("WP_URL", "https://aifrontierdispatch.com").rstrip("/")
WP_USER      = os.getenv("WP_USER", "")
WP_APP_PASS  = os.getenv("WP_APP_PASSWORD", "")
API_BASE     = f"{WP_URL}/wp-json/wp/v2"

REQUEST_DELAY   = 1.0
SEO_TITLE_MIN   = 50
SEO_TITLE_MAX   = 60
META_DESC_MIN   = 150
META_DESC_MAX   = 160

SLUG_STOP_WORDS = {
    "a","an","the","and","or","but","in","on","at","to","for",
    "of","with","by","from","is","was","are","were","be","been",
    "has","have","had","do","does","did","will","would","could",
    "should","may","might","this","that","these","those","it","its"
}

# ── HTTP SESSION WITH RETRIES ───────────────────────────────────────────

def _make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Connection": "keep-alive",
    })
    retry_strategy = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=2, pool_maxsize=5)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = _make_session()

def safe_request(method, url, max_attempts=4, **kwargs):
    global SESSION
    kwargs.setdefault("timeout", 30)
    for attempt in range(1, max_attempts + 1):
        try:
            fn = getattr(SESSION, method)
            return fn(url, **kwargs)
        except (requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError) as e:
            wait = 2 ** attempt
            print(f"  [HTTP] Error (attempt {attempt}/{max_attempts}): {e}")
            if attempt < max_attempts:
                time.sleep(wait)
                SESSION = _make_session()
            else:
                raise

def _auth_header():
    token = b64encode(f"{WP_USER}:{WP_APP_PASS}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json",
    }

# ── TEXT UTILITIES ──────────────────────────────────────────────────────

def clean_html_entities(text: str) -> str:
    return html.unescape(text or "")

def strip_html_tags(text: str) -> str:
    return BeautifulSoup(text or "", "html.parser").get_text(separator=" ").strip()

def word_count(text: str) -> int:
    return len(strip_html_tags(text).split())

def extract_keywords_from_title(title: str):
    clean = clean_html_entities(title).lower()
    clean = re.sub(r"[^a-z0-9\s]", " ", clean)
    words = [w for w in clean.split() if w and w not in SLUG_STOP_WORDS and len(w) > 2]
    return words[:5]

def optimize_slug_from_title(title: str) -> str:
    clean = clean_html_entities(title).lower()
    clean = re.sub(r"[^a-z0-9\s]", " ", clean)
    words = [w for w in clean.split() if w not in SLUG_STOP_WORDS and len(w) > 1]
    slug = "-".join(words[:8])
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug

def generate_seo_title(raw_title: str, site_name="AI Frontier Dispatch") -> str:
    clean = clean_html_entities(raw_title).strip()
    full  = f"{clean} | {site_name}"
    if SEO_TITLE_MIN <= len(full) <= SEO_TITLE_MAX:
        return full
    if len(full) > SEO_TITLE_MAX:
        max_content = SEO_TITLE_MAX - len(f" | {site_name}")
        trimmed = clean[:max_content].rsplit(" ", 1)[0]
        return f"{trimmed} | {site_name}"
    return full

def generate_meta_description(content_html: str, title: str, keywords):
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

def add_alt_tags_to_images(content_html: str, keywords):
    soup = BeautifulSoup(content_html or "", "html.parser")
    images = soup.find_all("img")
    updated = False
    count = 0
    for i, img in enumerate(images):
        if not img.get("alt") or img.get("alt", "").strip() == "":
            base = " ".join(keywords[:3]) if keywords else "article image"
            alt = f"{base} - image {i+1}" if i > 0 else base
            img["alt"] = alt
            updated = True
            count += 1
    return str(soup), updated, count

# ── WP HELPERS ───────────────────────────────────────────────────────────

def slug_from_url(url: str) -> str:
    path = urlparse(url).path
    parts = [p for p in path.split("/") if p]
    return parts[-1] if parts else ""

def get_post_by_slug(slug: str):
    if not slug:
        return None
    r = safe_request(
        "get",
        f"{API_BASE}/posts",
        params={"slug": slug, "context": "edit"},
        headers=_auth_header(),
    )
    if r and r.ok:
        data = r.json()
        if isinstance(data, list) and data:
            return data[0]
    return None

def update_post(post_id: int, payload: dict):
    r = safe_request(
        "post",
        f"{API_BASE}/posts/{post_id}",
        headers=_auth_header(),
        json=payload,
    )
    return r

# ── AUDIT CSV LOADER ─────────────────────────────────────────────────────

def latest_audit_csv() -> str | None:
    pattern = os.path.join(
        OUTPUT_DIR,
        f"{DOMAIN}_technical_audit_*.csv"
    )
    files = sorted(glob.glob(pattern))
    return files[-1] if files else None

# ── CORE FIXER LOGIC (USED BY CLI & STREAMLIT) ──────────────────────────

def fix_from_audit(dry_run: bool = False):
    """
    Returns a list[dict] results; each dict contains:
      url, slug, post_id, fixed (bool), reason, issues, changes (list[str])
    """
    csv_path = latest_audit_csv()
    if not csv_path:
        print("❌ No technical audit CSV found in seo_reports/")
        return []

    print(f"📄 Using audit file: {csv_path}")
    df = pd.read_csv(csv_path)
    df["issues"] = df["issues"].astype(str).fillna("")

    df = df[(df["status"].astype(str) == "200") & (df["issues"].str.len() > 0)]

    results = []
    for _, row in df.iterrows():
        url = row["url"]
        issue_str = row["issues"]
        print(f"\n🔧 Fixing based on audit for URL: {url}")
        print(f"   Issues: {issue_str}")

        slug = slug_from_url(url)
        post = get_post_by_slug(slug)
        if not post:
            print("   ⚠️ No matching post found for slug:", slug)
            results.append({
                "url": url,
                "slug": slug,
                "post_id": None,
                "fixed": False,
                "reason": "No matching WP post",
                "issues": issue_str,
                "changes": [],
            })
            continue

        pid = post["id"]
        wp_title = post["title"]["rendered"]
        wp_content = post["content"]["rendered"]
        meta = post.get("meta", {}) or {}

        changes = {}
        changes_meta = {}
        changes_made = []

        clean_title = clean_html_entities(wp_title)
        keywords = extract_keywords_from_title(clean_title)

        # Title related issues
        if ("Missing title" in issue_str or
            "Title short" in issue_str or
            "Title long"  in issue_str):

            new_title = generate_seo_title(clean_title)
            old_yoast_title = meta.get("_yoast_wpseo_title", "")
            if new_title != old_yoast_title:
                changes_meta["_yoast_wpseo_title"] = new_title
                changes_meta["rank_math_title"]   = new_title
                changes_made.append("Updated SEO title (Yoast/RankMath)")

        # Meta description issues
        if ("Missing meta desc" in issue_str or
            "Meta short"        in issue_str or
            "Meta long"         in issue_str):

            new_desc = generate_meta_description(wp_content, clean_title, keywords)
            old_yoast_desc = meta.get("_yoast_wpseo_metadesc", "")
            if new_desc != old_yoast_desc:
                changes_meta["_yoast_wpseo_metadesc"] = new_desc
                changes_meta["rank_math_description"]  = new_desc
                changes_made.append("Updated meta description (Yoast/RankMath)")

        # Image alt issues
        if "img no alt" in issue_str:
            new_content, alt_updated, alt_count = add_alt_tags_to_images(wp_content, keywords)
            if alt_updated:
                changes["content"] = new_content
                changes_made.append(f"Added ALT tags to {alt_count} image(s)")

        # Slug cleanup (heuristic using title/URL issues)
        if "Title long" in issue_str or "Slug" in issue_str:
            new_slug = optimize_slug_from_title(clean_title)
            if new_slug and new_slug != post["slug"] and len(new_slug) < len(post["slug"]):
                changes["slug"] = new_slug
                changes_made.append(f"Slug cleaned to '{new_slug}'")

        if changes_meta:
            # merge into existing meta so we don't drop fields
            merged_meta = dict(meta)
            merged_meta.update(changes_meta)
            changes["meta"] = merged_meta

        if not changes:
            print("   ℹ️ Nothing to change for this post (based on mappable issues).")
            results.append({
                "url": url,
                "slug": slug,
                "post_id": pid,
                "fixed": False,
                "reason": "No applicable fix for listed issues",
                "issues": issue_str,
                "changes": [],
            })
            continue

        if dry_run:
            print("   🧪 DRY RUN — would apply:", ", ".join(changes_made))
            results.append({
                "url": url,
                "slug": slug,
                "post_id": pid,
                "fixed": False,
                "reason": "dry-run",
                "issues": issue_str,
                "changes": changes_made,
            })
        else:
            resp = update_post(pid, changes)
            if resp and resp.ok:
                print("   ✅ Applied:", ", ".join(changes_made))
                fixed = True
                reason = ""
            else:
                status = resp.status_code if resp else "no response"
                print("   ❌ Save failed:", status)
                fixed = False
                reason = f"Save failed ({status})"
            results.append({
                "url": url,
                "slug": slug,
                "post_id": pid,
                "fixed": fixed,
                "reason": reason,
                "issues": issue_str,
                "changes": changes_made,
            })

        time.sleep(REQUEST_DELAY)

    today = datetime.today().strftime("%Y-%m-%d")
    rep_df = pd.DataFrame(results)
    csv_out  = os.path.join(OUTPUT_DIR, f"{DOMAIN}_fix_issues_{today}.csv")
    json_out = os.path.join(OUTPUT_DIR, f"{DOMAIN}_fix_issues_{today}.json")
    rep_df.to_csv(csv_out, index=False)
    with open(json_out, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n📄 Fix report saved → {csv_out}")
    return results


if __name__ == "__main__":
    print("🚀 Running CSV-driven Fix Issues (based on latest technical audit)...")
    fix_from_audit(dry_run=False)
