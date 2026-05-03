#!/usr/bin/env python3
"""
Fix issues based on latest technical audit CSVs — multi-site.

For each registered site:
  - Reads latest seo_reports/<slug>/<DOMAIN>_technical_audit_YYYY-MM-DD.csv
  - For each URL with issues and status=200:
      * Resolves WP post via slug
      * Fixes:
          - SEO title (Yoast + Rank Math meta)
          - Meta description (Yoast + Rank Math meta)
          - Missing image alt attributes
          - Slug cleanup (shorter, keyword-based)
  - Writes CSV + JSON report to seo_reports/<slug>/

CLI:
    python fix_issues.py                 # all sites, live
    python fix_issues.py --dry-run       # all sites, no writes
    python fix_issues.py --site sanfranciscobriefing.com
"""

from __future__ import annotations

import argparse
import os
import re
import time
import glob
import json
import html
from datetime import datetime
from base64 import b64encode
from urllib.parse import urlparse
from typing import Optional

import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from sites_config import Site, SITES, get_site


# ── CONFIG ──────────────────────────────────────────────────────────────

REQUEST_DELAY   = 1.0
SEO_TITLE_MIN   = 50
SEO_TITLE_MAX   = 60
META_DESC_MIN   = 120
META_DESC_MAX   = 155

SLUG_STOP_WORDS = {
    "a","an","the","and","or","but","in","on","at","to","for",
    "of","with","by","from","is","was","are","were","be","been",
    "has","have","had","do","does","did","will","would","could",
    "should","may","might","this","that","these","those","it","its",
}

# ── HTTP SESSION WITH RETRIES ───────────────────────────────────────────

def _make_session() -> requests.Session:
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


def safe_request(method, url, max_attempts: int = 4, **kwargs):
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


def _auth_header(site: Site) -> dict:
    token = b64encode(f"{site.wp_user}:{site.wp_app_pass}".encode()).decode()
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


def _short_brand(brand_name: str) -> str:
    """Initials fallback, e.g. 'San Francisco Briefing' -> 'SFB'."""
    parts = re.findall(r"[A-Za-z]+", brand_name)
    if not parts:
        return brand_name
    if len(parts) == 1:
        return parts[0][:4].upper()
    return "".join(p[0] for p in parts).upper()[:5]


def generate_seo_title(raw_title: str, site_name: str) -> str:
    """SEO title within 50–60 chars; favors content over branding when tight."""
    clean = clean_html_entities(raw_title).strip()

    full_with_brand = f"{clean} - {site_name}"
    if SEO_TITLE_MIN <= len(full_with_brand) <= SEO_TITLE_MAX:
        return full_with_brand

    if len(full_with_brand) > SEO_TITLE_MAX:
        short = _short_brand(site_name)
        full_with_short = f"{clean} - {short}"
        if len(full_with_short) <= SEO_TITLE_MAX:
            return full_with_short
        max_length = SEO_TITLE_MAX
        trimmed = clean[:max_length].rsplit(" ", 1)[0].rstrip(".,!?;:")
        return trimmed

    return full_with_brand


def generate_meta_description(content_html: str, title: str, keywords) -> str:
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


# ── WP HELPERS (per-site) ───────────────────────────────────────────────

def slug_from_url(url: str) -> str:
    path = urlparse(url).path
    parts = [p for p in path.split("/") if p]
    return parts[-1] if parts else ""


def get_post_by_slug(site: Site, slug: str):
    if not slug:
        return None
    r = safe_request(
        "get",
        f"{site.api_base}/posts",
        params={"slug": slug, "context": "edit"},
        headers=_auth_header(site),
    )
    if r and r.ok:
        data = r.json()
        if isinstance(data, list) and data:
            return data[0]
    return None


def update_post(site: Site, post_id: int, payload: dict):
    return safe_request(
        "post",
        f"{site.api_base}/posts/{post_id}",
        headers=_auth_header(site),
        json=payload,
    )


# ── AUDIT CSV LOADER (per-site) ─────────────────────────────────────────

def latest_audit_csv(site: Site, base_output: str = "seo_reports") -> Optional[str]:
    """Find the latest technical-audit CSV for a site.

    Looks first inside the per-site folder (seo_reports/<slug>/) and falls
    back to the legacy flat layout (seo_reports/) for backwards compat.
    """
    candidates: list[str] = []
    per_site_dir = os.path.join(base_output, site.slug)
    candidates += glob.glob(os.path.join(per_site_dir, f"{site.domain}_technical_audit_*.csv"))
    candidates += glob.glob(os.path.join(base_output, f"{site.domain}_technical_audit_*.csv"))
    if not candidates:
        return None
    return sorted(candidates)[-1]


# ── CORE FIXER LOGIC (per-site) ─────────────────────────────────────────

def fix_from_audit(
    site: Site | str,
    dry_run: bool = False,
    base_output: str = "seo_reports",
) -> list[dict]:
    """Run the SEO fixer against a single site using its latest audit CSV.

    Returns list[dict] results; each contains:
      url, slug, post_id, fixed (bool), reason, issues, changes (list[str])
    """
    if isinstance(site, str):
        site = get_site(site)

    output_dir = site.output_dir(base_output)
    csv_path = latest_audit_csv(site, base_output)
    if not csv_path:
        print(f"❌ [{site.domain}] No technical audit CSV found.")
        return []

    print(f"\n📄 [{site.domain}] Using audit file: {csv_path}")
    df = pd.read_csv(csv_path)
    df["issues"] = df["issues"].astype(str).fillna("")
    df = df[(df["status"].astype(str) == "200") & (df["issues"].str.len() > 0)]

    results: list[dict] = []
    for _, row in df.iterrows():
        url = row["url"]
        issue_str = row["issues"]
        print(f"\n🔧 [{site.domain}] Fixing: {url}")
        print(f"   Issues: {issue_str}")

        slug = slug_from_url(url)
        post = get_post_by_slug(site, slug)
        if not post:
            print(f"   ⚠️ No matching post for slug: {slug}")
            results.append({
                "url": url, "slug": slug, "post_id": None, "fixed": False,
                "reason": "No matching WP post", "issues": issue_str, "changes": [],
            })
            continue

        pid = post["id"]
        wp_title = post["title"]["rendered"]
        wp_content = post["content"]["rendered"]
        meta = post.get("meta", {}) or {}

        changes: dict = {}
        changes_meta: dict = {}
        changes_made: list[str] = []

        clean_title = clean_html_entities(wp_title)
        keywords = extract_keywords_from_title(clean_title)

        if ("Missing title" in issue_str or
            "Title short"  in issue_str or
            "Title long"   in issue_str):
            new_title = generate_seo_title(clean_title, site.brand_name)
            old_yoast_title = meta.get("_yoast_wpseo_title", "")
            if new_title != old_yoast_title:
                changes_meta["_yoast_wpseo_title"] = new_title
                changes_meta["rank_math_title"]   = new_title
                changes_made.append("Updated SEO title (Yoast/RankMath)")

        if ("Missing meta desc" in issue_str or
            "Meta short"        in issue_str or
            "Meta long"         in issue_str):
            new_desc = generate_meta_description(wp_content, clean_title, keywords)
            old_yoast_desc = meta.get("_yoast_wpseo_metadesc", "")
            if new_desc != old_yoast_desc:
                changes_meta["_yoast_wpseo_metadesc"] = new_desc
                changes_meta["rank_math_description"]  = new_desc
                changes_made.append("Updated meta description (Yoast/RankMath)")

        if "img no alt" in issue_str:
            new_content, alt_updated, alt_count = add_alt_tags_to_images(wp_content, keywords)
            if alt_updated:
                changes["content"] = new_content
                changes_made.append(f"Added ALT tags to {alt_count} image(s)")

        if "Title long" in issue_str or "Slug" in issue_str:
            new_slug = optimize_slug_from_title(clean_title)
            if new_slug and new_slug != post["slug"] and len(new_slug) < len(post["slug"]):
                changes["slug"] = new_slug
                changes_made.append(f"Slug cleaned to '{new_slug}'")

        if changes_meta:
            merged_meta = dict(meta)
            merged_meta.update(changes_meta)
            changes["meta"] = merged_meta

        if not changes:
            print("   ℹ️ Nothing to change.")
            results.append({
                "url": url, "slug": slug, "post_id": pid, "fixed": False,
                "reason": "No applicable fix for listed issues",
                "issues": issue_str, "changes": [],
            })
            continue

        if dry_run:
            print("   🧪 DRY RUN — would apply:", ", ".join(changes_made))
            results.append({
                "url": url, "slug": slug, "post_id": pid, "fixed": False,
                "reason": "dry-run", "issues": issue_str, "changes": changes_made,
            })
        else:
            resp = update_post(site, pid, changes)
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
                "url": url, "slug": slug, "post_id": pid, "fixed": fixed,
                "reason": reason, "issues": issue_str, "changes": changes_made,
            })

        time.sleep(REQUEST_DELAY)

    today = datetime.today().strftime("%Y-%m-%d")
    rep_df = pd.DataFrame(results)
    csv_out  = os.path.join(output_dir, f"{site.domain}_fix_issues_{today}.csv")
    json_out = os.path.join(output_dir, f"{site.domain}_fix_issues_{today}.json")
    if not rep_df.empty:
        rep_df.to_csv(csv_out, index=False)
    with open(json_out, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"📄 [{site.domain}] Fix report → {csv_out}")
    return results


def fix_all_sites(dry_run: bool = False, base_output: str = "seo_reports") -> dict[str, list[dict]]:
    """Run fix_from_audit for every registered site. Returns {domain: results}."""
    summary: dict[str, list[dict]] = {}
    for site in SITES:
        try:
            summary[site.domain] = fix_from_audit(site, dry_run=dry_run, base_output=base_output)
        except Exception as e:
            print(f"❌ [{site.domain}] Fixer crashed: {e}")
            summary[site.domain] = []
    return summary


# ── CLI ─────────────────────────────────────────────────────────────────

def _main() -> None:
    ap = argparse.ArgumentParser(description="WordPress SEO auto-fixer (multi-site)")
    ap.add_argument("--site", help="Run for one domain only (e.g. sanfranciscobriefing.com)")
    ap.add_argument("--dry-run", action="store_true", help="Plan changes; don't write to WP")
    ap.add_argument("--output", default="seo_reports", help="Reports base directory")
    args = ap.parse_args()

    print("🚀 Running CSV-driven Fix Issues (multi-site)...")
    if args.site:
        fix_from_audit(get_site(args.site), dry_run=args.dry_run, base_output=args.output)
    else:
        results = fix_all_sites(dry_run=args.dry_run, base_output=args.output)
        print("\n──────── SUMMARY ────────")
        for domain, rows in results.items():
            ok = sum(1 for r in rows if r.get("fixed"))
            print(f"  {domain:35s}  attempts={len(rows):4d}  fixed={ok}")


if __name__ == "__main__":
    _main()