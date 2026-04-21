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
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest

GA4_PROPERTY_ID = "532475459"  # e.g. "123456789"
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

st.set_page_config(page_title="SEO Dashboard — AI Frontier Dispatch", page_icon="📊", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');
    html, body, .stApp { font-family: 'DM Sans', sans-serif; }
    div[data-testid="stSidebar"] { background: #0f1117; }
    div[data-testid="stSidebar"] * { color: #e2e8f0 !important; }
    div[data-testid="stSidebar"] .stRadio label { color: #e2e8f0 !important; }
    div[data-testid="stSidebar"] hr { border-color: #2d3748; }

    .growth-card {
        background: linear-gradient(135deg, #0f1117 0%, #1a202c 100%);
        border: 1px solid #2d3748;
        border-radius: 12px;
        padding: 20px;
        margin-bottom: 12px;
        position: relative;
        overflow: hidden;
    }
    .growth-card::before {
        content: '';
        position: absolute;
        top: 0; left: 0;
        width: 3px; height: 100%;
        background: var(--accent, #38b2ac);
        border-radius: 3px 0 0 3px;
    }
    .growth-card.positive { --accent: #48bb78; }
    .growth-card.negative { --accent: #fc8181; }
    .growth-card.neutral  { --accent: #718096; }

    .trend-badge {
        display: inline-block;
        padding: 2px 10px;
        border-radius: 20px;
        font-size: 0.72rem;
        font-weight: 600;
        letter-spacing: 0.04em;
        text-transform: uppercase;
    }
    .trend-up   { background: #1a3a2a; color: #68d391; }
    .trend-down { background: #3a1a1a; color: #fc8181; }
    .trend-flat { background: #1e2333; color: #a0aec0; }

    .kpi-delta-pos { color: #68d391; font-size: 0.85rem; font-weight: 600; }
    .kpi-delta-neg { color: #fc8181; font-size: 0.85rem; font-weight: 600; }
    .kpi-delta-neu { color: #a0aec0; font-size: 0.85rem; font-weight: 600; }

    .timeline-dot {
        width: 10px; height: 10px;
        border-radius: 50%;
        background: #38b2ac;
        display: inline-block;
        margin-right: 8px;
    }
    .scan-history-row {
        background: #1a202c;
        border: 1px solid #2d3748;
        border-radius: 8px;
        padding: 14px 18px;
        margin-bottom: 8px;
        display: flex;
        align-items: center;
        justify-content: space-between;
    }
</style>
""", unsafe_allow_html=True)


# ═══ CRAWL ENGINE ═══
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
            results.append({"url": url, "status": "ERROR", "load_time_s": None, "title": "", "title_length": 0,
                            "meta_description": "", "meta_desc_length": 0, "h1_count": 0, "canonical": "",
                            "noindex": False, "images_missing_alt": 0, "has_og_tags": False, "has_schema": False,
                            "issues": f"Connection error: {e}"})
            continue

        status = resp.status_code
        content = resp.text if status == 200 else ""

        title_m = re.search(r"<title[^>]*>(.*?)</title>", content, re.I | re.S)
        title_text = re.sub(r"<[^>]+>", "", title_m.group(1)).strip() if title_m else ""
        meta_m = (re.search(r'<meta[^>]+name=["\'"]description["\'"][^>]+content=["\'"]([^"\']*)', content, re.I) or
                  re.search(r'<meta[^>]+content=["\'"]([^"\']*)["\'"][^>]+name=["\'"]description["\'"]', content, re.I))
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

        results.append({
            "url": url, "status": status, "load_time_s": load_time,
            "title": title_text, "title_length": len(title_text),
            "meta_description": meta_text, "meta_desc_length": len(meta_text),
            "h1_count": h1_count, "canonical": canonical_u, "noindex": noindex,
            "images_missing_alt": img_missing, "has_og_tags": has_og,
            "has_schema": has_schema, "issues": " | ".join(issues)
        })
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
        if words[i] not in STOP and words[i + 1] not in STOP:
            bigrams[f"{words[i]} {words[i + 1]}"] += 1
    top_bi = sorted(bigrams.items(), key=lambda x: -x[1])[:15]
    return {"url": url, "title": title, "h1": " | ".join(h1s), "h2s": " | ".join(h2s[:8]),
            "top_words": top, "top_bigrams": top_bi, "word_count": len(words)}


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


# ═══ GROWTH HELPERS ═══
def compute_audit_snapshot(df):
    """Return a dict of key health metrics from an audit dataframe."""
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
    """Return SERP summary metrics."""
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
    """Load audit + serp snapshots across all available dates."""
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
    serp_df  = pd.DataFrame(serp_rows).sort_values("date")  if serp_rows  else pd.DataFrame()
    return audit_df, serp_df

def delta_str(new_val, old_val, higher_is_better=True, fmt="{:.0f}", suffix=""):
    """Return a coloured delta string and CSS class."""
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

def pct_change(new_val, old_val):
    if old_val is None or old_val == 0 or new_val is None:
        return None
    return round((new_val - old_val) / old_val * 100, 1)

def trend_icon(new_val, old_val, higher_is_better=True):
    if old_val is None or new_val is None or new_val == old_val:
        return "→", "trend-flat", "FLAT"
    improved = (new_val > old_val) == higher_is_better
    if improved:
        return "↑", "trend-up", "IMPROVED"
    return "↓", "trend-down", "DECLINED"


# ═══ SIDEBAR ═══
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
        "⚡ Run New Scan"
    ], label_visibility="collapsed")
    st.divider()
    dates = get_report_dates()
    selected_date = st.selectbox("Report Date", dates, index=0) if dates else datetime.today().strftime("%Y-%m-%d")
    if not dates: st.info("No reports yet. Run a scan!")
    st.divider()
    st.caption("SEO Automation Toolkit — Free Edition")


# ═══════════════════════════════════════
# PAGE: OVERVIEW
# ═══════════════════════════════════════
if page == "🏠 Overview":
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

        c1.metric("Users (7d)", ga_df["users"].sum())
        c2.metric("Sessions (7d)", ga_df["sessions"].sum())
        c3.metric("Pageviews (7d)", ga_df["pageviews"].sum())

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
