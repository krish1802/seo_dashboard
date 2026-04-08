#!/usr/bin/env python3
"""
Standalone daily crawl script — runs via GitHub Actions.
Saves CSVs to seo_reports/ and auto-commits back to the repo.
"""
import os, re, time, requests, pandas as pd
from datetime import datetime
from urllib.parse import urljoin, urlparse
from collections import defaultdict

SITE_URL   = "https://aifrontierdispatch.com"
DOMAIN     = "aifrontierdispatch.com"
OUTPUT_DIR = "seo_reports"
HEADERS    = {"User-Agent": "Mozilla/5.0 (compatible; SEO-Audit-Bot/1.0)"}
os.makedirs(OUTPUT_DIR, exist_ok=True)

TRACKED_KEYWORDS = [
    "AI news 2025", "artificial intelligence breakthroughs",
    "AI business insights", "machine learning research",
    "AI frontier news", "generative AI tools",
    "AI startup news", "large language models news",
]

def crawl(start_url, max_pages=80):
    visited, queue, results = set(), [start_url], []
    parsed = urlparse(start_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    while queue and len(visited) < max_pages:
        url = queue.pop(0)
        if url in visited: continue
        visited.add(url)
        print(f"[{len(visited)}/{max_pages}] {url}")
        try:
            t0 = time.time()
            r  = requests.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
            lt = round(time.time() - t0, 2)
        except Exception as e:
            results.append({"url": url, "status": "ERROR", "load_time_s": None,
                "title": "", "title_length": 0, "meta_description": "", "meta_desc_length": 0,
                "h1_count": 0, "canonical": "", "noindex": False, "images_missing_alt": 0,
                "has_og_tags": False, "has_schema": False, "issues": str(e)})
            continue
        status  = r.status_code
        content = r.text if status == 200 else ""
        tm  = re.search(r"<title[^>]*>(.*?)</title>", content, re.I|re.S)
        title = re.sub(r"<[^>]+>","", tm.group(1)).strip() if tm else ""
        mm  = (re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']*)', content, re.I) or
               re.search(r'<meta[^>]+content=["\']([^"\']*)["\'][^>]+name=["\']description["\']', content, re.I))
        meta = mm.group(1).strip() if mm else ""
        h1   = len(re.findall(r"<h1[^>]*>", content, re.I))
        cm   = re.search(r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']([^"\']*)', content, re.I)
        can  = cm.group(1).strip() if cm else ""
        noi  = bool(re.search(r'content=["\'][^"\']*noindex', content, re.I))
        img  = len(re.findall(r'<img(?![^>]*\balt\s*=)[^>]*/?>', content, re.I))
        og   = bool(re.search(r'property=["\']og:', content, re.I))
        sch  = bool(re.search(r'application/ld\+json', content, re.I))
        if status == 200:
            for link in re.findall(r'href=["\']([^"\'#?][^"\']*)["\']', content, re.I):
                full = urljoin(base, link)
                if full.startswith(base) and full not in visited:
                    queue.append(full)
        issues = []
        if status >= 400: issues.append(f"HTTP {status}")
        if not title:            issues.append("Missing title")
        elif len(title) < 30:   issues.append(f"Title short ({len(title)})")
        elif len(title) > 65:   issues.append(f"Title long ({len(title)})")
        if not meta:             issues.append("Missing meta desc")
        elif len(meta) < 70:    issues.append(f"Meta short ({len(meta)})")
        elif len(meta) > 160:   issues.append(f"Meta long ({len(meta)})")
        if h1 == 0:  issues.append("No H1")
        elif h1 > 1: issues.append(f"Multiple H1s ({h1})")
        if lt > 3.0: issues.append(f"Slow ({lt}s)")
        if noi:      issues.append("Noindexed")
        if img > 0:  issues.append(f"{img} img no alt")
        if not og:   issues.append("No OG tags")
        if not sch:  issues.append("No Schema")
        results.append({"url": url, "status": status, "load_time_s": lt,
            "title": title, "title_length": len(title),
            "meta_description": meta, "meta_desc_length": len(meta),
            "h1_count": h1, "canonical": can, "noindex": noi,
            "images_missing_alt": img, "has_og_tags": og,
            "has_schema": sch, "issues": " | ".join(issues)})
        time.sleep(0.3)
    return results

def extract_keywords(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        content = r.text
    except:
        return None
    clean = re.sub(r"<(script|style)[^>]*>.*?</(script|style)>", "", content, flags=re.I|re.S)
    text  = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", clean)).strip().lower()
    tm    = re.search(r"<title[^>]*>(.*?)</title>", content, re.I|re.S)
    title = re.sub(r"<[^>]+>","", tm.group(1)).strip() if tm else ""
    h1s   = [re.sub(r"<[^>]+>","",h).strip() for h in re.findall(r"<h1[^>]*>(.*?)</h1>", content, re.I|re.S)]
    h2s   = [re.sub(r"<[^>]+>","",h).strip() for h in re.findall(r"<h2[^>]*>(.*?)</h2>", content, re.I|re.S)]
    STOP  = set("the a an and or but in on at to for of with is are was were be been have has had do does did will would could should may might shall this that these those i we you he she it they my our your his her its their from by up about into than then so if not no more".split())
    words = re.findall(r"\b[a-z][a-z\-]{2,}\b", text)
    freq  = defaultdict(int)
    for w in words:
        if w not in STOP: freq[w] += 1
    top    = sorted(freq.items(), key=lambda x:-x[1])[:20]
    bigrams = defaultdict(int)
    for i in range(len(words)-1):
        if words[i] not in STOP and words[i+1] not in STOP:
            bigrams[f"{words[i]} {words[i+1]}"] += 1
    top_bi = sorted(bigrams.items(), key=lambda x:-x[1])[:15]
    return {"url": url, "title": title, "h1": " | ".join(h1s),
            "h2s": " | ".join(h2s[:8]), "top_words": top, "top_bigrams": top_bi, "word_count": len(words)}

if __name__ == "__main__":
    today = datetime.today().strftime("%Y-%m-%d")
    print(f"\n🔍 Starting crawl for {DOMAIN} on {today}...\n")

    # ── Technical Audit ──
    results = crawl(SITE_URL)
    if results:
        df   = pd.DataFrame(results)
        path = f"{OUTPUT_DIR}/{DOMAIN}_technical_audit_{today}.csv"
        df.to_csv(path, index=False)
        issues = len(df[df["issues"].astype(str).str.len() > 0])
        print(f"✅ Audit: {len(results)} pages crawled, {issues} with issues → {path}")
    else:
        print("❌ Crawl returned no results"); exit(1)

    # ── Content Analysis ──
    print("\n📝 Analyzing page content...")
    urls    = df[df["status"].astype(str) == "200"]["url"].tolist()[:30]
    kw_rows = []
    for i, url in enumerate(urls):
        print(f"  [{i+1}/{len(urls)}] {url}")
        kd = extract_keywords(url)
        if kd:
            kw_rows.append({
                "url": kd["url"], "title": kd["title"], "h1": kd["h1"], "h2s": kd["h2s"],
                "top_words":   ", ".join(f"{w}({c})" for w,c in kd["top_words"]),
                "top_bigrams": ", ".join(f"{b}({c})" for b,c in kd["top_bigrams"]),
                "word_count":  kd["word_count"]
            })
        time.sleep(0.4)

    if kw_rows:
        kw_path = f"{OUTPUT_DIR}/{DOMAIN}_page_keywords_{today}.csv"
        pd.DataFrame(kw_rows).to_csv(kw_path, index=False)
        print(f"✅ Keywords: {len(kw_rows)} pages analysed → {kw_path}")

        all_kws  = list(TRACKED_KEYWORDS)
        for row in kw_rows:
            for h in str(row.get("h2s","")).split(" | "):
                h = h.strip()
                if 10 < len(h) < 80: all_kws.append(h)
        clusters = defaultdict(list)
        for kw in all_kws:
            w = kw.lower().split()
            k = " ".join(w[:2]) if len(w) >= 2 else (w[0] if w else "other")
            clusters[k].append(kw)
        crows = [{"cluster": k, "keyword_count": len(v), "keywords": " | ".join(v)}
                 for k,v in sorted(clusters.items(), key=lambda x:-len(x[1]))]
        cl_path = f"{OUTPUT_DIR}/{DOMAIN}_keyword_clusters_{today}.csv"
        pd.DataFrame(crows).to_csv(cl_path, index=False)
        print(f"✅ Clusters saved → {cl_path}")

    print("\n🎉 Done! All results saved to seo_reports/")