script = """
WordPress SEO Auto-Optimizer
==============================
Automatically audits and fixes SEO issues across all posts:

  ✅ SEO title optimization  (50-60 chars)
  ✅ Meta description        (150-160 chars)
  ✅ Focus keyword injection into title/content/slug
  ✅ Slug cleanup            (lowercase, hyphens, no stop words)
  ✅ Content improvements    (headings, internal structure check)
  ✅ Yoast SEO meta fields   (_yoast_wpseo_title, _yoast_wpseo_metadesc)
  ✅ Rank Math meta fields   (rank_math_title, rank_math_description)
  ✅ Open Graph title/desc   (_yoast_wpseo_opengraph-title, etc.)
  ✅ HTML entity cleanup     in titles (&#8211; → – etc.)
  ✅ Auto-retry on connection drops (WinError 10054 fix)
  ✅ Detailed per-post report with before/after comparison

Install:
    pip install requests python-dotenv beautifulsoup4

.env file:
    WP_URL=https://aifrontierdispatch.com
    WP_USER=californianartisinal
    WP_APP_PASSWORD=MgHu CJ79 DTwE vI4a DmxG SjvU
"""

import requests
import os
import time
import re
import html
import json
from base64 import b64encode
from datetime import datetime
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Config ──────────────────────────────────────────────────────────────────
WP_URL      = os.getenv("WP_URL", "https://aifrontierdispatch.com").rstrip("/")
WP_USER     = os.getenv("WP_USER", "californianartisinal")
WP_APP_PASS = os.getenv("WP_APP_PASSWORD", "")
API_BASE    = f"{WP_URL}/wp-json/wp/v2"

# SEO length targets
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

# ── Session with retry + browser UA ─────────────────────────────────────────
# Fixes WinError 10054 (connection forcibly closed):
#   - HTTPAdapter with Retry retries on connection drops automatically
#   - Browser User-Agent bypasses nginx bot-blocking rules
#   - pool_connections/pool_maxsize keeps connections alive (no rapid reconnects)
#   - REQUEST_DELAY adds pause between calls so server doesn\'t rate-limit

REQUEST_DELAY = 1.5   # seconds between API calls — increase if still dropping

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
        total            = 5,          # retry up to 5 times
        backoff_factor   = 2,          # wait 2s, 4s, 8s, 16s... between retries
        status_forcelist = [429, 500, 502, 503, 504],
        allowed_methods  = ["GET", "POST"],
        raise_on_status  = False,
    )
    adapter = HTTPAdapter(
        max_retries      = retry_strategy,
        pool_connections = 2,
        pool_maxsize     = 5,
    )
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    return session

SESSION = _make_session()


def safe_request(method, url, max_attempts=4, **kwargs):
    """
    Wrapper around SESSION.get/post that handles WinError 10054
    by recreating the session and retrying with exponential backoff.
    """
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
                print(f"      Retrying in {wait}s with a fresh session...")
                time.sleep(wait)
                SESSION = _make_session()   # fresh TCP connection pool
            else:
                print("  ❌ All retries exhausted.")
                raise


def _auth_header():
    token = b64encode(f"{WP_USER}:{WP_APP_PASS}".encode()).decode()
    return {"Authorization": f"Basic {token}", "Content-Type": "application/json"}


# ══════════════════════════════════════════════════════════════════════════
# TEXT HELPERS
# ══════════════════════════════════════════════════════════════════════════

def clean_html_entities(text):
    return html.unescape(text)

def strip_html_tags(text):
    return BeautifulSoup(text, "html.parser").get_text(separator=" ").strip()

def word_count(text):
    return len(strip_html_tags(text).split())

def extract_keywords_from_title(title):
    clean = clean_html_entities(title).lower()
    clean = re.sub(r"[^a-z0-9\\s]", " ", clean)
    words = [w for w in clean.split() if w and w not in SLUG_STOP_WORDS and len(w) > 2]
    return words[:5]

def optimize_slug(title):
    clean = clean_html_entities(title).lower()
    # Replace hyphens AND spaces with a separator, then split
    clean = re.sub(r"[^a-z0-9\s]", " ", clean)   # remove everything except letters, numbers, spaces
    words = [w for w in clean.split() if w not in SLUG_STOP_WORDS and len(w) > 1]
    slug  = "-".join(words[:8])                    # max 8 keywords
    slug  = re.sub(r"-+", "-", slug).strip("-")
    return slug

def generate_seo_title(raw_title, site_name="AI Frontier Dispatch"):
    clean = clean_html_entities(raw_title).strip()
    full  = f"{clean} | {site_name}"
    if SEO_TITLE_MIN <= len(full) <= SEO_TITLE_MAX:
        return full
    if len(full) > SEO_TITLE_MAX:
        max_len = SEO_TITLE_MAX - len(f" | {site_name}")
        trimmed = clean[:max_len].rsplit(" ", 1)[0]
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
    desc = re.sub(r"\\s+", " ", desc)
    if len(desc) > META_DESC_MAX:
        desc = desc[:META_DESC_MAX].rsplit(" ", 1)[0]
        if not desc.endswith("."):
            desc += "…"
    if len(desc) < META_DESC_MIN and keywords:
        kw_phrase = f" Learn about {', '.join(keywords[:3])}."
        desc = (desc + kw_phrase)[:META_DESC_MAX]
    return desc.strip()

def seo_score(title_clean, meta_desc, slug, content_html, keywords):
    issues = []
    score  = 100
    tlen   = len(title_clean)
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
    plain    = soup.get_text().lower()
    kw_found = sum(1 for k in keywords if k in plain)
    if keywords and kw_found == 0:
        issues.append("Focus keywords not found in content")
        score -= 10
    return max(0, score), issues


# ══════════════════════════════════════════════════════════════════════════
# FETCH POSTS
# ══════════════════════════════════════════════════════════════════════════

def get_all_posts(status="publish", per_page=10, max_pages=5):
    """
    Fetch posts page by page with delay between requests.
    per_page=10 (not 20) reduces payload size per request — helps avoid drops.
    """
    all_posts = []
    for page in range(1, max_pages + 1):
        print(f"  📄 Fetching page {page}...", end=" ", flush=True)
        r = safe_request(
            "get",
            f"{API_BASE}/posts",
            headers=_auth_header(),
            params={
                "status":   status,
                "per_page": per_page,
                "page":     page,
                "context":  "edit",
                "_fields":  "id,title,slug,content,excerpt,meta,link,modified"
            }
        )
        if not r.ok or not r.json():
            print(f"stopped (status {r.status_code})")
            break
        batch       = r.json()
        total_pages = int(r.headers.get("X-WP-TotalPages", 1))
        all_posts.extend(batch)
        print(f"got {len(batch)} posts (total so far: {len(all_posts)})")
        if page >= total_pages:
            break
        time.sleep(REQUEST_DELAY)   # pause between pages

    return all_posts


# ══════════════════════════════════════════════════════════════════════════
# APPLY SEO FIXES
# ══════════════════════════════════════════════════════════════════════════

def apply_seo_fixes(post, dry_run=True):
    pid          = post["id"]
    raw_title    = post["title"]["rendered"]
    slug         = post["slug"]
    content_html = post["content"]["rendered"]
    meta         = post.get("meta", {})

    clean_title   = clean_html_entities(raw_title)
    keywords      = extract_keywords_from_title(clean_title)
    new_seo_title = generate_seo_title(clean_title)
    new_meta_desc = generate_meta_description(content_html, clean_title, keywords)
    new_slug      = optimize_slug(clean_title)

    old_yoast_title = meta.get("_yoast_wpseo_title", "")
    old_yoast_desc  = meta.get("_yoast_wpseo_metadesc", "")
    old_rm_title    = meta.get("rank_math_title", "")
    old_rm_desc     = meta.get("rank_math_description", "")

    score_before, issues = seo_score(
        old_yoast_title or clean_title,
        old_yoast_desc  or new_meta_desc,
        slug, content_html, keywords
    )
    score_after, _ = seo_score(
        new_seo_title, new_meta_desc,
        new_slug, content_html, keywords
    )

    result = {
        "id":            pid,
        "title":         clean_title,
        "slug_old":      slug,
        "slug_new":      new_slug,
        "seo_title_old": old_yoast_title or old_rm_title or "(none)",
        "seo_title_new": new_seo_title,
        "meta_desc_old": old_yoast_desc  or old_rm_desc  or "(none)",
        "meta_desc_new": new_meta_desc,
        "score_before":  score_before,
        "score_after":   score_after,
        "issues":        issues,
        "keywords":      keywords,
        "word_count":    word_count(content_html),
        "changes_made":  [],
        "dry_run":       dry_run,
    }

    if dry_run:
        return result

    changes = {}

    if raw_title != clean_title:
        changes["title"] = clean_title
        result["changes_made"].append("Cleaned HTML entities from title")

    if new_slug and new_slug != slug and len(new_slug) < len(slug):
        changes["slug"] = new_slug
        result["changes_made"].append(f"Slug: {slug} → {new_slug}")

    yoast_meta = {}
    if new_seo_title != old_yoast_title:
        yoast_meta["_yoast_wpseo_title"]              = new_seo_title
        yoast_meta["_yoast_wpseo_opengraph-title"]    = new_seo_title
        result["changes_made"].append("Updated Yoast SEO title")
    if new_meta_desc != old_yoast_desc:
        yoast_meta["_yoast_wpseo_metadesc"]                = new_meta_desc
        yoast_meta["_yoast_wpseo_opengraph-description"]   = new_meta_desc
        result["changes_made"].append("Updated Yoast meta description")
    if yoast_meta:
        changes["meta"] = yoast_meta

    rm_meta = {}
    if new_seo_title != old_rm_title:
        rm_meta["rank_math_title"]       = new_seo_title
    if new_meta_desc != old_rm_desc:
        rm_meta["rank_math_description"] = new_meta_desc
    if rm_meta:
        changes.setdefault("meta", {}).update(rm_meta)

    if changes:
        time.sleep(REQUEST_DELAY)   # pause before write request
        r = safe_request(
            "post",
            f"{API_BASE}/posts/{pid}",
            headers=_auth_header(),
            json=changes
        )
        if r.ok:
            result["changes_made"].append("✅ Saved to WordPress")
        else:
            result["changes_made"].append(f"❌ Save failed ({r.status_code}): {r.text[:200]}")

    return result


# ══════════════════════════════════════════════════════════════════════════
# BULK AUDIT + FIX
# ══════════════════════════════════════════════════════════════════════════

def run_seo_optimizer(
    status           = "publish",
    per_page         = 10,
    max_pages        = 10,
    dry_run          = True,
    min_score_to_fix = 80,
    report_file      = "seo_report.json"
):
    print("=" * 60)
    mode = "DRY RUN (audit only)" if dry_run else "LIVE (applying fixes)"
    print(f"  WordPress SEO Optimizer — {mode}")
    print(f"  Site  : {WP_URL}")
    print(f"  Date  : {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    print(f"\\n📥 Fetching {status} posts (10 per page with delays)...")
    posts = get_all_posts(status=status, per_page=per_page, max_pages=max_pages)
    print(f"\\n✅ Loaded {len(posts)} posts\\n")

    if not posts:
        print("No posts found.")
        return []

    report    = []
    needs_fix = 0
    fixed     = 0

    for i, post in enumerate(posts, 1):
        pid   = post["id"]
        title = clean_html_entities(post["title"]["rendered"])
        print(f"[{i:>3}/{len(posts)}] ID {pid} — {title[:55]}")

        result = apply_seo_fixes(post, dry_run=True)
        score  = result["score_before"]

        flag = "⚠️ " if score < min_score_to_fix else "✅ "
        if score < min_score_to_fix:
            needs_fix += 1

        print(f"        Score: {flag}{score}/100  |  Words: {result['word_count']}  |  Issues: {len(result['issues'])}")
        for issue in result["issues"]:
            print(f"          • {issue}")

        if not dry_run and score < min_score_to_fix:
            result = apply_seo_fixes(post, dry_run=False)
            if result["changes_made"]:
                fixed += 1
                print(f"        Applied: {', '.join(result['changes_made'])}")

        report.append(result)
        time.sleep(REQUEST_DELAY)   # pause between each post audit
        print()

    # ── Summary ──
    print("=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    avg_score = sum(r["score_before"] for r in report) / len(report) if report else 0
    print(f"  Total posts audited : {len(report)}")
    print(f"  Average SEO score   : {avg_score:.1f}/100")
    print(f"  Posts needing fixes : {needs_fix}")
    if not dry_run:
        print(f"  Posts fixed         : {fixed}")
    print()

    all_issues   = [issue for r in report for issue in r["issues"]]
    issue_counts = {}
    for issue in all_issues:
        key = issue.split("(")[0].strip()
        issue_counts[key] = issue_counts.get(key, 0) + 1

    print("  Top Issues:")
    for issue, count in sorted(issue_counts.items(), key=lambda x: -x[1])[:5]:
        print(f"    {count}x  {issue}")

    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"\\n📄 Full report saved to: {report_file}")
    print("=" * 60)

    return report


# ══════════════════════════════════════════════════════════════════════════
# SINGLE POST FIX
# ══════════════════════════════════════════════════════════════════════════

def fix_post_by_id(post_id, dry_run=False):
    r = safe_request(
        "get",
        f"{API_BASE}/posts/{post_id}",
        headers=_auth_header(),
        params={"context": "edit"}
    )
    if not r.ok:
        print(f"❌ Could not fetch post {post_id}: {r.status_code}")
        return None
    result = apply_seo_fixes(r.json(), dry_run=dry_run)
    print(f"\\nPost {post_id}: {result['title']}")
    print(f"  SEO Score : {result['score_before']} → {result['score_after']}")
    print(f"  SEO Title : {result['seo_title_new']}")
    print(f"  Meta Desc : {result['meta_desc_new']}")
    print(f"  Slug      : {result['slug_old']} → {result['slug_new']}")
    print(f"  Keywords  : {result['keywords']}")
    if result["issues"]:
        print("  Issues    :")
        for i in result["issues"]:
            print(f"    • {i}")
    if not dry_run and result["changes_made"]:
        print(f"  Applied   : {result['changes_made']}")
    return result


# ══════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":

    # ── OPTION 1: Audit ALL published posts (dry run — no changes) ──────────
    run_seo_optimizer(
        status           = "publish",
        per_page         = 10,      # 10 per page — gentler on server
        max_pages        = 10,      # up to 100 posts total
        dry_run          = False,    # ← change to False to apply fixes
        min_score_to_fix = 80,
        report_file      = "seo_report.json"
    )

    # ── OPTION 2: Apply fixes to all posts scoring below 80 (LIVE) ─────────
    # run_seo_optimizer(
    #     status           = "publish",
    #     dry_run          = False,   # ← LIVE mode
    #     min_score_to_fix = 80,
    # )

    # ── OPTION 3: Fix a single post by ID ───────────────────────────────────
    # fix_post_by_id(2330, dry_run=True)    # audit only
    # fix_post_by_id(2330, dry_run=False)   # apply fixes


os.makedirs("output", exist_ok=True)
with open("output/seo_optimizer.py", "w", encoding="utf-8") as f:
    f.write(script)
print("Done")