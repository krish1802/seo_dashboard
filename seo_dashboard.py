#!/usr/bin/env python3
"""
SEO Analytics Dashboard — Streamlit App
For aifrontierdispatch.com
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import csv
import re
import time
import requests
import json
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
from collections import defaultdict
from pathlib import Path

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

st.set_page_config(page_title="SEO Dashboard — AI Frontier Dispatch", page_icon="📊", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    .stApp { font-family: 'Inter', sans-serif; }
    div[data-testid="stSidebar"] { background: #f7f6f2; }
    .big-metric { text-align: center; padding: 16px; background: #f8f9fa; border-radius: 10px; border: 1px solid #dee2e6; }
    .big-metric h2 { margin: 0; color: #01696f; font-size: 2rem; }
    .big-metric p { margin: 4px 0 0 0; color: #6c757d; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; }
</style>
""", unsafe_allow_html=True)


# ═══ CRAWL ENGINE ═══
def crawl_site(start_url, max_pages=100, progress_bar=None, status_text=None):
    visited, queue, results = set(), [start_url], []
    parsed = urlparse(start_url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    while queue and len(visited) < max_pages:
        url = queue.pop(0)
        if url in visited: continue
        visited.add(url)
        if progress_bar: progress_bar.progress(min(len(visited) / max_pages, 1.0))
        if status_text: status_text.text(f"Crawling ({len(visited)}/{max_pages}): {url[:80]}")

        try:
            t0 = time.time()
            resp = requests.get(url, headers=CRAWL_HEADERS, timeout=15, allow_redirects=True)
            load_time = round(time.time() - t0, 2)
        except Exception as e:
            results.append({"url": url, "status": "ERROR", "load_time_s": None, "title": "", "title_length": 0, "meta_description": "", "meta_desc_length": 0, "h1_count": 0, "canonical": "", "noindex": False, "images_missing_alt": 0, "has_og_tags": False, "has_schema": False, "issues": f"Connection error: {e}"})
            continue

        status = resp.status_code
        content = resp.text if status == 200 else ""

        title_m = re.search(r"<title[^>]*>(.*?)</title>", content, re.I | re.S)
        title_text = re.sub(r"<[^>]+>", "", title_m.group(1)).strip() if title_m else ""
        meta_m = re.search(r'<meta[^>]+name=["\'"]description["\'"][^>]+content=["\'"]([^"\']*)', content, re.I) or re.search(r'<meta[^>]+content=["\'"]([^"\']*)["\'"][^>]+name=["\'"]description["\'"]', content, re.I)
        meta_text = meta_m.group(1).strip() if meta_m else ""
        h1_count = len(re.findall(r"<h1[^>]*>", content, re.I))
        canonical_m = re.search(r'<link[^>]+rel=["\'"]canonical["\'"][^>]+href=["\'"]([^"\']*)', content, re.I)
        canonical_u = canonical_m.group(1).strip() if canonical_m else ""
        noindex = bool(re.search(r'content=["\'"][^"\']*noindex', content, re.I))
        img_missing = len(re.findall(r'<img(?![^>]*\balt\s*=)[^>]*/?>', content, re.I))
        has_og = bool(re.search(r'property=["\'"]og:', content, re.I))
        has_schema = bool(re.search(r'application/ld\+json', content, re.I))

        if status == 200:
            for link in re.findall(r'href=["\'"]([^"\'#?][^"\']*)["\'"]', content, re.I):
                full = urljoin(base, link)
                if full.startswith(base) and full not in visited:
                    queue.append(full)

        issues = []
        if status >= 400: issues.append(f"HTTP {status}")
        if not title_text: issues.append("Missing title")
        elif len(title_text) < 30: issues.append(f"Title short ({len(title_text)})")
        elif len(title_text) > 65: issues.append(f"Title long ({len(title_text)})")
        if not meta_text: issues.append("Missing meta desc")
        elif len(meta_text) < 70: issues.append(f"Meta short ({len(meta_text)})")
        elif len(meta_text) > 160: issues.append(f"Meta long ({len(meta_text)})")
        if h1_count == 0: issues.append("No H1")
        elif h1_count > 1: issues.append(f"Multiple H1s ({h1_count})")
        if load_time and load_time > 3.0: issues.append(f"Slow ({load_time}s)")
        if noindex: issues.append("Noindexed")
        if img_missing > 0: issues.append(f"{img_missing} img no alt")
        if not has_og: issues.append("No OG tags")
        if not has_schema: issues.append("No Schema")

        results.append({"url": url, "status": status, "load_time_s": load_time, "title": title_text, "title_length": len(title_text), "meta_description": meta_text, "meta_desc_length": len(meta_text), "h1_count": h1_count, "canonical": canonical_u, "noindex": noindex, "images_missing_alt": img_missing, "has_og_tags": has_og, "has_schema": has_schema, "issues": " | ".join(issues)})
        time.sleep(0.3)
    return results


def extract_page_keywords(url):
    try:
        resp = requests.get(url, headers=CRAWL_HEADERS, timeout=15)
        resp.raise_for_status()
        content = resp.text
    except:
        return {"url": url, "error": "fetch failed"}

    clean = re.sub(r"<(script|style)[^>]*>.*?</(script|style)>", "", content, flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", " ", clean)
    text = re.sub(r"\s+", " ", text).strip().lower()
    title_m = re.search(r"<title[^>]*>(.*?)</title>", content, re.I | re.S)
    title = re.sub(r"<[^>]+>", "", title_m.group(1)).strip() if title_m else ""
    h1s = [re.sub(r"<[^>]+>", "", h).strip() for h in re.findall(r"<h1[^>]*>(.*?)</h1>", content, re.I | re.S)]
    h2s = [re.sub(r"<[^>]+>", "", h).strip() for h in re.findall(r"<h2[^>]*>(.*?)</h2>", content, re.I | re.S)]

    STOP = set("the a an and or but in on at to for of with is are was were be been have has had do does did will would could should may might shall this that these those i we you he she it they my our your his her its their from by up about into than then so if not no more".split())
    words = re.findall(r"\b[a-z][a-z\-]{2,}\b", text)
    freq = defaultdict(int)
    for w in words:
        if w not in STOP: freq[w] += 1
    top = sorted(freq.items(), key=lambda x: -x[1])[:20]
    bigrams = defaultdict(int)
    for i in range(len(words) - 1):
        if words[i] not in STOP and words[i+1] not in STOP:
            bigrams[f"{words[i]} {words[i+1]}"] += 1
    top_bi = sorted(bigrams.items(), key=lambda x: -x[1])[:15]
    return {"url": url, "title": title, "h1": " | ".join(h1s), "h2s": " | ".join(h2s[:8]), "top_words": top, "top_bigrams": top_bi, "word_count": len(words)}


# ═══ DATA LOADERS ═══
def get_report_dates():
    dates = set()
    if os.path.exists(OUTPUT_DIR):
        for f in os.listdir(OUTPUT_DIR):
            m = re.search(r"(\d{4}-\d{2}-\d{2})", f)
            if m: dates.add(m.group(1))
    return sorted(dates, reverse=True)

def load_csv(prefix, date):
    path = f"{OUTPUT_DIR}/{prefix}_{date}.csv"
    return pd.read_csv(path) if os.path.exists(path) else None

def load_audit(date): return load_csv(f"{DOMAIN}_technical_audit", date)
def load_serp(date): return load_csv("serp_tracking", date)
def load_keywords(date): return load_csv(f"{DOMAIN}_page_keywords", date)
def load_clusters(date): return load_csv(f"{DOMAIN}_keyword_clusters", date)


# ═══ SIDEBAR ═══
with st.sidebar:
    st.markdown("## 📊 SEO Dashboard")
    st.markdown(f"**Site:** `{DOMAIN}`")
    st.divider()
    page = st.radio("Navigate", ["🏠 Overview", "🔍 Technical Audit", "📈 SERP Rankings", "📝 Content Analysis", "🔑 Keywords", "⚡ Run New Scan"], label_visibility="collapsed")
    st.divider()
    dates = get_report_dates()
    selected_date = st.selectbox("Report Date", dates, index=0) if dates else datetime.today().strftime("%Y-%m-%d")
    if not dates: st.info("No reports yet. Run a scan!")
    st.divider()
    st.caption("SEO Automation Toolkit — Free Edition")


# ═══ OVERVIEW ═══
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
                fig = px.pie(names=list(cats.keys()), values=list(cats.values()), color_discrete_sequence=px.colors.qualitative.Set2, hole=0.4)
                fig.update_layout(margin=dict(t=20, b=20, l=20, r=20), height=350)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.success("No issues! 🎉")

        with col2:
            st.markdown("### ⏱️ Load Times")
            lt = audit_df["load_time_s"].dropna()
            if len(lt) > 0:
                fig = px.histogram(lt, nbins=20, labels={"value": "Load Time (s)"}, color_discrete_sequence=["#01696f"])
                fig.add_vline(x=3.0, line_dash="dash", line_color="#da7101", annotation_text="3s threshold")
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


# ═══ TECHNICAL AUDIT ═══
elif page == "🔍 Technical Audit":
    st.markdown("# 🔍 Technical SEO Audit")
    st.divider()
    audit_df = load_audit(selected_date)
    if audit_df is not None:
        c1, c2, c3 = st.columns(3)
        with c1: filt = st.selectbox("Filter", ["All", "With Issues", "Clean"])
        with c2: search = st.text_input("Search URL")
        with c3: statuses = st.multiselect("Status", sorted(audit_df["status"].astype(str).unique()))

        df = audit_df.copy()
        if filt == "With Issues": df = df[df["issues"].astype(str).str.len() > 0]
        elif filt == "Clean": df = df[(df["issues"].isna()) | (df["issues"].astype(str).str.len() == 0)]
        if statuses: df = df[df["status"].astype(str).isin(statuses)]
        if search: df = df[df["url"].str.contains(search, case=False, na=False)]

        st.markdown(f"**{len(df)} of {len(audit_df)} pages**")
        cols = [c for c in ["url", "status", "load_time_s", "title_length", "meta_desc_length", "h1_count", "images_missing_alt", "has_og_tags", "has_schema", "issues"] if c in df.columns]
        st.dataframe(df[cols], use_container_width=True, height=500)
        st.download_button("📥 Download CSV", df.to_csv(index=False).encode(), f"audit_{selected_date}.csv", "text/csv")

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


# ═══ SERP ═══
elif page == "📈 SERP Rankings":
    st.markdown("# 📈 SERP Rankings")
    st.divider()
    serp_df = load_serp(selected_date)
    if serp_df is not None and len(serp_df) > 0:
        ss = serp_df[serp_df["site"] == DOMAIN] if "site" in serp_df.columns else serp_df
        cd = ss[["keyword", "our_position"]].copy()
        cd["our_position"] = pd.to_numeric(cd["our_position"], errors="coerce")
        cd = cd.dropna()
        if len(cd) > 0:
            fig = px.bar(cd, x="keyword", y="our_position", color="our_position", color_continuous_scale=["#437a22", "#da7101", "#a12c7b"], range_color=[1, 20])
            fig.update_layout(yaxis=dict(autorange="reversed", title="Position"), xaxis_tickangle=-45, height=400)
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
                        if pd.notna(p): rows.append({"date": d, "keyword": r["keyword"], "position": p})
            if rows:
                fig = px.line(pd.DataFrame(rows), x="date", y="position", color="keyword", markers=True)
                fig.update_layout(yaxis=dict(autorange="reversed"), height=400)
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No SERP data.")


# ═══ CONTENT ═══
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


# ═══ KEYWORDS ═══
elif page == "🔑 Keywords":
    st.markdown("# 🔑 Keyword Clusters")
    st.divider()
    cl = load_clusters(selected_date)
    if cl is not None and len(cl) > 0:
        fig = px.treemap(cl.head(15), path=["cluster"], values="keyword_count", color="keyword_count", color_continuous_scale="Teal")
        fig.update_layout(height=400, margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(cl, use_container_width=True)
    else:
        st.warning("No keyword data.")


# ═══ RUN SCAN ═══
elif page == "⚡ Run New Scan":
    st.markdown("# ⚡ Run New SEO Scan")
    st.markdown(f"Scan **{DOMAIN}** now")
    st.divider()

    c1, c2 = st.columns(2)
    with c1: max_pages = st.slider("Max pages", 10, 300, 50, 10)
    with c2: modules = st.multiselect("Modules", ["Technical Audit", "Content Analysis"], default=["Technical Audit"])
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
                prog2.progress((i+1)/len(urls))
                kd = extract_page_keywords(url)
                if "error" not in kd:
                    kw_rows.append({"url": kd["url"], "title": kd["title"], "h1": kd["h1"], "h2s": kd["h2s"], "top_words": ", ".join(f"{w}({c})" for w, c in kd.get("top_words", [])), "top_bigrams": ", ".join(f"{b}({c})" for b, c in kd.get("top_bigrams", [])), "word_count": kd.get("word_count", 0)})
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
                crows = [{"cluster": k, "keyword_count": len(v), "keywords": " | ".join(v)} for k, v in sorted(clusters.items(), key=lambda x: -len(x[1]))]
                pd.DataFrame(crows).to_csv(f"{OUTPUT_DIR}/{DOMAIN}_keyword_clusters_{today}.csv", index=False)
                st.success(f"✅ Analyzed {len(kw_rows)} pages.")
        st.balloons()
        st.info("Navigate to other tabs to view results!")
